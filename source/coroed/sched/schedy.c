#include "schedy.h"

#include <assert.h>
#include <sched.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <threads.h>

#include "coroed/api/task.h"
#include "kthread.h"
#include "uthread.h"

enum {
  /**
   * Количество рабочих потоков для исполнения файберов.
   */
  SCHED_WORKERS_COUNT = (size_t)(8),
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

  struct {
    size_t steps;     // Сколько шагов было выполнено
    size_t finished;  // Сколько задач было завершено
  } statistics;       // Локальная статистика работяги
};


static kthread_id_t kthread_ids[SCHED_WORKERS_COUNT];
static struct worker workers[SCHED_WORKERS_COUNT];

enum {
    PRIORITY_LEVELS = 3,
    BOOST_INTERVAL = 64,
};

struct ready_queue {
    struct task* head;
    struct task* tail;
};

static const size_t yield_budget[PRIORITY_LEVELS] = {1, 2, 4};
static struct ready_queue ready[PRIORITY_LEVELS];
static mtx_t ready_lock;
static cnd_t ready_changed;
static size_t unfinished_tasks;
static size_t dispatches_since_boost;

/**
 * Установить рабочего в пустое состояние.
 */
void sched_worker_init(struct worker* worker, size_t index) {
  worker->index = index;
  worker->sched_thread.context = NULL;
  worker->running_task = NULL;
  worker->statistics.steps = 0;
  worker->statistics.finished = 0;
}

void sched_init() {
  memset(ready, 0, sizeof(ready));
  unfinished_tasks = 0;
  dispatches_since_boost = 0;

  if (mtx_init(&ready_lock, mtx_plain) != thrd_success) {
    abort();
  }
  if (cnd_init(&ready_changed) != thrd_success) {
    mtx_destroy(&ready_lock);
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
struct task* sched_acquire_next();

/**
 * Вернуть готовую задачу в очередь планирования
 * либо освободить завершенную задачу.
 */
void sched_release(struct task* task);

/**
 * Цикл планировщика. Выполняется, пока есть задачи.
 */
int sched_loop(void* argument) {
  struct worker* worker = argument;
  kthread_ids[worker->index] = kthread_id();

  for (;;) {
    struct task* task = sched_acquire_next();
    if (task == NULL) {
      break;
    }

    sched_switch_to(worker, task);
    worker->running_task = NULL;

    worker->statistics.steps += 1;
    if (task->state == UTHREAD_FINISHED) {
      worker->statistics.finished += 1;
    }

    sched_release(task);

    // Hint: где-то здесь можно было бы опросить
    //       механизмы для неблокирующего ввода-вывода
    //       и перевести удовлетворенные BLOCKED
    //       потоки в RUNNABLE состояние.
  }

  return 0;
}

static void ready_push(struct task* task) {
    struct ready_queue* queue = &ready[task->priority];
    task->next = NULL;

    if (queue->tail != NULL) {
        queue->tail->next = task;
    } else {
        queue->head = task;
    }

    queue->tail = task;
}

static struct task* ready_pop(size_t priority) {
    struct ready_queue* queue = &ready[priority];
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

static void boost_ready_tasks(void) {
    for (size_t priority = 1; priority < PRIORITY_LEVELS; ++priority) {
        struct task* task;
        while ((task = ready_pop(priority)) != NULL) {
            task->priority = 0;
            task->yields_left = yield_budget[0];
            ready_push(task);
        }
    }
}

struct task* sched_acquire_next() {
    mtx_lock(&ready_lock);

    for (;;) {
        for (size_t priority = 0; priority < PRIORITY_LEVELS; ++priority) {
            struct task* task = ready_pop(priority);
            if (task == NULL) {
                continue;
            }

            if (++dispatches_since_boost == BOOST_INTERVAL) {
                boost_ready_tasks();
                dispatches_since_boost = 0;
            }

            mtx_unlock(&ready_lock);
            return task;
        }

        if (unfinished_tasks == 0) {
            mtx_unlock(&ready_lock);
            return NULL;
        }

        cnd_wait(&ready_changed, &ready_lock);
    }
}

void sched_release(struct task* task) {
    task->worker = NULL;

    if (task->state == UTHREAD_FINISHED) {
        uthread_free(task->thread);
        free(task);

        mtx_lock(&ready_lock);
        if (--unfinished_tasks == 0) {
            cnd_broadcast(&ready_changed);
        }
        mtx_unlock(&ready_lock);
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

    mtx_lock(&ready_lock);
    ready_push(task);
    cnd_signal(&ready_changed);
    mtx_unlock(&ready_lock);
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

task_t task_submit(struct task* caller, uthread_routine entry, void* argument) {
  (void)caller;  // Может быть полезно.
  task_t child = sched_submit(*entry, argument);
  return child;
}

task_t sched_submit(uthread_routine entry, void* argument) {
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

    mtx_lock(&ready_lock);
    ++unfinished_tasks;
    ready_push(task);
    cnd_signal(&ready_changed);
    mtx_unlock(&ready_lock);

    return (task_t){.task = task};
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
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* worker = &workers[i];
    tasks_count += worker->statistics.finished;
    steps_count += worker->statistics.steps;
  }

  printf("|- tasks executed %zu\n", tasks_count);
  printf("|- steps done     %zu\n", steps_count);

  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* worker = &workers[i];
    printf("|- worker %zu %zu\n", i, kthread_ids[i]);
    printf("   |- steps     %zu\n", worker->statistics.steps);
    printf("   |- finished  %zu\n", worker->statistics.finished);
  }
}

void sched_destroy() {
  cnd_destroy(&ready_changed);
  mtx_destroy(&ready_lock);
}