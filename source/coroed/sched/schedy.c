#include "schedy.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <threads.h>

#include "coroed/api/task.h"
#include "kthread.h"
#include "uthread.h"

enum {
  /**
   * Количество рабочих потоков для исполнения файберов.
   */
  SCHED_WORKERS_COUNT = (size_t)(8),
  PRIORITY_LEVELS = 3,
  BOOST_INTERVAL = 64,
};

/**
 * Задача, выполняющаяся на планировщике.
 *
 * Hint: также тут можно хранить список задач, ожидающих
 *       завершения этой, чтобы уведомить их о данном
 *       событии.
 */
struct task {
  /**
   * Контекст исполнения.
   */
  struct uthread* thread;

  /**
   * Установлен при `UTHREAD_RUNNING`. Указывает на
   * рабочий поток, на котором исполняется задача.
   * Необходим для получения контекста локального
   * планировщика при операциях `yield`, `await`, `submit`.
   */
  struct worker* worker;

  enum {
    /** Готова к исполнению. */
    UTHREAD_RUNNABLE,

    /** Прямо сейчас выполняется. */
    UTHREAD_RUNNING,

    /** Завершена, контекст будет освобожден планировщиком. */
    UTHREAD_FINISHED,
  } state;  // Текущее состояние задачи

  struct task* next;

  size_t priority;

  size_t yields_left;
};

struct ready_queue {
  struct task* head;
  struct task* tail;
};

/**
 * Рабочий поток, исполняющий файберы.
 */
struct worker {
  /**
   * Идентификатор рабочего потока.
   */
  size_t index;

  /**
   * Поток операционной системы, на котором
   * исполняется рабочий.
   */
  struct kthread kthread;

  /**
   * Контекст планировщика. Нужно переключиться на него,
   * чтобы вернуться в планировщик.
   */
  struct uthread sched_thread;

  /**
   * В данный момент исполняемая на рабочем
   * задача. Может быть `NULL`.
   */
  struct task* running_task;

  mtx_t ready_lock;
  struct ready_queue ready[PRIORITY_LEVELS];
  size_t boost_epoch;

  struct {
    size_t steps;     // Сколько шагов было выполнено
    size_t finished;  // Сколько задач было завершено
    size_t steals;    // Сколько задач было взято у других воркеров
  } statistics;       // Локальная статистика работяги
};


static kthread_id_t kthread_ids[SCHED_WORKERS_COUNT];
static struct worker workers[SCHED_WORKERS_COUNT];

static const size_t yield_budget[PRIORITY_LEVELS] = {1, 2, 4};
static atomic_size_t dispatch_count;
static size_t next_submit_worker;

// Защищает учёт задач и синхронизирует добавление работы с ожиданием.
static mtx_t work_lock;
static cnd_t ready_changed;
static size_t unfinished_tasks;
static size_t work_generation;

/**
 * Установить рабочего в пустое состояние.
 */
void sched_worker_init(struct worker* worker, size_t index) {
  worker->index = index;
  worker->sched_thread.context = NULL;
  worker->running_task = NULL;
  worker->statistics.steps = 0;
  worker->statistics.finished = 0;
  worker->statistics.steals = 0;
  memset(worker->ready, 0, sizeof(worker->ready));
  worker->boost_epoch = 0;
  if (mtx_init(&worker->ready_lock, mtx_plain) != thrd_success) {
    abort();
  }
}

void sched_init() {
  unfinished_tasks = 0;
  work_generation = 0;
  next_submit_worker = 0;
  atomic_store_explicit(&dispatch_count, 0, memory_order_relaxed);

  if (mtx_init(&work_lock, mtx_plain) != thrd_success) {
    abort();
  }
  if (cnd_init(&ready_changed) != thrd_success) {
    mtx_destroy(&work_lock);
    abort();
  }

  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    kthread_ids[i] = 0;
    sched_worker_init(&workers[i], i);
  }
}

/**
 * Находясь в контексте задачи `task`, переключиться
 * в контекст планировщика.
 */
void sched_switch_to_scheduler(struct task* task) {
  struct uthread* sched = &task->worker->sched_thread;
  task->worker = NULL;
  uthread_switch(task->thread, sched);
}

/**
 * Находясь в контексте планировщика, переключиться
 * в контекст задачи `task`. Выполнять ее до следующего
 * невынужденного возвращения в планировщик.
 */
void sched_switch_to(struct worker* worker, struct task* task) {
  assert(task->thread != &worker->sched_thread);

  task->state = UTHREAD_RUNNING;

  task->worker = worker;
  worker->running_task = task;

  struct uthread* sched = &worker->sched_thread;
  uthread_switch(sched, task->thread);
}

/**
 * Получить следующую задачу на исполнение в
 * соответствии с текущим алгоритмом планирования.
 *
 * Вызывающему передается владение задачей после снятия
 * с очереди. Мьютекс очереди отпускается до возврата.
 */
static struct task* sched_acquire_next(struct worker* worker);

/**
 * Вернуть готовую задачу в очередь планирования
 * либо освободить завершенную задачу.
 */
static void sched_release(struct worker* worker, struct task* task);

/**
 * Цикл планировщика. Выполняется, пока есть задачи.
 */
int sched_loop(void* argument) {
  struct worker* worker = argument;
  kthread_ids[worker->index] = kthread_id();

  for (;;) {
    struct task* task = sched_acquire_next(worker);
    if (task == NULL) {
      break;
    }

    sched_switch_to(worker, task);
    worker->running_task = NULL;

    worker->statistics.steps += 1;
    if (task->state == UTHREAD_FINISHED) {
      worker->statistics.finished += 1;
    }

    sched_release(worker, task);

    // Hint: где-то здесь можно было бы опросить
    //       механизмы для неблокирующего ввода-вывода
    //       и перевести удовлетворенные BLOCKED
    //       потоки в RUNNABLE состояние.
  }

  return 0;
}

// Операции со списком выполняются под ready_lock его воркера.
static void ready_push(struct ready_queue* queue, struct task* task) {
  task->next = NULL;

  if (queue->tail != NULL) {
    queue->tail->next = task;
  } else {
    queue->head = task;
  }

  queue->tail = task;
}

static struct task* ready_pop(struct ready_queue* queue) {
  struct task* task = queue->head;
  if (task == NULL) {
    return NULL;
  }

  queue->head = task->next;
  if (queue->head == NULL) {
    queue->tail = NULL;
  }

  task->next = NULL;
  return task;
}

// Каждые BOOST_INTERVAL выборов начинается новый раунд повышения.
// Воркер применяет его к конкретному набору очередей при обращении к нему.
static void boost_ready_tasks(struct worker* owner) {
  size_t epoch = atomic_load_explicit(&dispatch_count, memory_order_relaxed) / BOOST_INTERVAL;
  if (owner->boost_epoch == epoch) {
    return;
  }

  for (size_t priority = 1; priority < PRIORITY_LEVELS; ++priority) {
    struct task* task;
    while ((task = ready_pop(&owner->ready[priority])) != NULL) {
      task->priority = 0;
      task->yields_left = yield_budget[0];
      ready_push(&owner->ready[0], task);
    }
  }

  owner->boost_epoch = epoch;
}

static struct task* take_ready_task(struct worker* owner) {
  mtx_lock(&owner->ready_lock);
  boost_ready_tasks(owner);

  struct task* task = NULL;
  for (size_t priority = 0; priority < PRIORITY_LEVELS; ++priority) {
    task = ready_pop(&owner->ready[priority]);
    if (task != NULL) {
      atomic_fetch_add_explicit(&dispatch_count, 1, memory_order_relaxed);
      break;
    }
  }

  mtx_unlock(&owner->ready_lock);
  return task;
}

// Если нужен общий мьютекс и мьютекс очередей, захватываем их в этом порядке.
static void enqueue_task(struct worker* owner, struct task* task, bool is_new) {
  mtx_lock(&work_lock);
  if (is_new) {
    ++unfinished_tasks;
  }

  mtx_lock(&owner->ready_lock);
  boost_ready_tasks(owner);
  ready_push(&owner->ready[task->priority], task);
  mtx_unlock(&owner->ready_lock);

  ++work_generation;
  cnd_signal(&ready_changed);
  mtx_unlock(&work_lock);
}

static struct task* sched_acquire_next(struct worker* worker) {
  for (;;) {
    mtx_lock(&work_lock);
    if (unfinished_tasks == 0) {
      mtx_unlock(&work_lock);
      return NULL;
    }
    size_t observed_generation = work_generation;
    mtx_unlock(&work_lock);

    struct task* task = take_ready_task(worker);
    if (task != NULL) {
      return task;
    }

    for (size_t offset = 1; offset < SCHED_WORKERS_COUNT; ++offset) {
      size_t victim_index = (worker->index + offset) % SCHED_WORKERS_COUNT;
      task = take_ready_task(&workers[victim_index]);
      if (task != NULL) {
        ++worker->statistics.steals;
        return task;
      }
    }

    mtx_lock(&work_lock);
    // Если работа появилась во время обхода, начинаем следующий поиск без сна.
    while (unfinished_tasks != 0 && work_generation == observed_generation) {
      cnd_wait(&ready_changed, &work_lock);
    }
    mtx_unlock(&work_lock);
  }
}

static void sched_release(struct worker* worker, struct task* task) {
  task->worker = NULL;

  if (task->state == UTHREAD_FINISHED) {
    uthread_free(task->thread);
    free(task);

    mtx_lock(&work_lock);
    if (--unfinished_tasks == 0) {
      cnd_broadcast(&ready_changed);
    }
    mtx_unlock(&work_lock);
    return;
  }

  assert(task->state == UTHREAD_RUNNING);

  if (--task->yields_left == 0) {
    if (task->priority + 1 < PRIORITY_LEVELS) {
      ++task->priority;
    }
    task->yields_left = yield_budget[task->priority];
  }

  task->state = UTHREAD_RUNNABLE;
  enqueue_task(worker, task, false);
}

/**
 * Отметить задачу завершенной.
 */
void sched_finish(struct task* task) {
  task->state = UTHREAD_FINISHED;
}

void task_yield(struct task* caller) {
  sched_switch_to_scheduler(caller);
}

void task_exit(struct task* caller) {
  sched_finish(caller);
  task_yield(caller);
}

static task_t submit_to_worker(struct worker* worker, uthread_routine entry, void* argument) {
  struct task* task = calloc(1, sizeof(*task));
  if (task == NULL) {
    abort();
  }

  task->thread = uthread_allocate();
  if (task->thread == NULL) {
    free(task);
    abort();
  }

  uthread_set_entry(task->thread, entry);
  uthread_set_arg_0(task->thread, task);
  uthread_set_arg_1(task->thread, argument);
  task->state = UTHREAD_RUNNABLE;
  task->priority = 0;
  task->yields_left = yield_budget[0];

  task_t handle = {.task = task};
  enqueue_task(worker, task, true);
  return handle;
}

task_t task_submit(struct task* caller, uthread_routine entry, void* argument) {
  return submit_to_worker(caller->worker, entry, argument);
}

task_t sched_submit(uthread_routine entry, void* argument) {
  // Стартовые задачи подаются через tasks_submit() до запуска воркеров.
  struct worker* worker = &workers[next_submit_worker];
  next_submit_worker = (next_submit_worker + 1) % SCHED_WORKERS_COUNT;
  return submit_to_worker(worker, entry, argument);
}

void sched_start() {
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* worker = &workers[i];
    enum kthread_status status = kthread_create(&worker->kthread, sched_loop, worker);
    assert(status == KTHREAD_SUCCESS);
  }
}

void sched_wait() {
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* worker = &workers[i];
    enum kthread_status status = kthread_join(&worker->kthread);
    assert(status == KTHREAD_SUCCESS);
  }
}

void sched_print_statistics() {
  printf("\nsched statistics\n");

  size_t tasks_count = 0;
  size_t steps_count = 0;
  size_t steals_count = 0;
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* worker = &workers[i];
    tasks_count += worker->statistics.finished;
    steps_count += worker->statistics.steps;
    steals_count += worker->statistics.steals;
  }

  printf("|- tasks executed %zu\n", tasks_count);
  printf("|- steps done     %zu\n", steps_count);
  printf("|- tasks stolen   %zu\n", steals_count);

  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* worker = &workers[i];
    printf("|- worker %zu %zu\n", i, kthread_ids[i]);
    printf("   |- steps     %zu\n", worker->statistics.steps);
    printf("   |- finished  %zu\n", worker->statistics.finished);
    printf("   |- steals    %zu\n", worker->statistics.steals);
  }
}

void sched_destroy() {
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    mtx_destroy(&workers[i].ready_lock);
  }
  cnd_destroy(&ready_changed);
  mtx_destroy(&work_lock);
}
