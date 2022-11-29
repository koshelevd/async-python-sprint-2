import logging
import time
from datetime import datetime
from threading import Timer

from job import Job

logger = logging.getLogger(__name__)


class Scheduler:
    """
        Represents a dispatcher that can schedule jobs to run periodically.

        :param pool_size: The maximum number of jobs that can be scheduled
                          at the same time.
    """

    def __init__(self, pool_size=10):
        self._queue: list[Job] = []
        self._pool_size = pool_size

    def schedule(self, task: Job) -> bool:
        """Adds a job to the FIFO queue."""
        if len(self._queue) >= self._pool_size:
            logger.error("Queue is full")
            return False
        if task.start_at and task.start_at < datetime.now():
            logger.info("Task %s is expired", task)
            return False
        if task.start_at and task.start_at > datetime.now():
            logger.info("Scheduling %s to start at %s", task, task.start_at)
            t = Timer((task.start_at - datetime.now()).total_seconds(),
                      self._queue.append, [task])
            t.start()
            return False
        self._queue.append(task)
        return True

    def get_task(self) -> Job | None:
        """Returns the next task to run."""
        if self._queue:
            return self._queue.pop(0)

    def _run_task(self, task: Job | None) -> None:
        """Runs a task.
           Прошу описать в docstring как корутины из task.dependencies
           будут выполняться. Пока это не очень понятно.

           Я исходил из того, что в зависимостях находятся функции,
           которые мы отдельно должны запланировать. Таким образом
           тут просто проверяется, что если зависимость находится
           в очереди, то текущую таску мы не запускаем.

           Подразумавается ли, что тут может быть свой набор корутин,
           которые мы тоже должны добавить в планировщик отдельно?
        """
        if task is None:
            return

        if task.end_at and task.end_at < datetime.now():
            logger.info("Task %s is expired", task)
            return

        if task.dependencies:
            for dependency in task.dependencies:
                if dependency in self._queue:
                    logger.info("Task %s is waiting for dependency %s", task,
                                dependency)
                    self._queue.append(task)
                    return

        try:
            logger.info("Running %s", task)
            result = task.run()
        except StopIteration:
            logger.info("Task %s finished", task)
            return
        self._queue.append(task)
        return result

    def run(self) -> None:
        """Runs the dispatcher."""
        logger.info("Starting tasks")
        while True:
            task = self.get_task()
            self._run_task(task)
            time.sleep(0.1)
