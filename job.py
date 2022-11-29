import logging
from datetime import datetime, timedelta
from typing import Callable

from uuid import uuid4 as uid

logger = logging.getLogger(__name__)

TIME_FORMAT = '%Y-%m-%d %H:%M'


class Job:
    """
    Represents a job that can be scheduled to run periodically.

    :param task: The task to run.
    :param start_at: The time when the job should start running.
    :param duration: The duration of the job.
    :param max_working_time: The maximum time the job can run.
    :param tries: The number of times the job should be tried.
    :param dependencies: The list of dependencies that should be run
                         before the job.
    """

    def __init__(self,
                 task: Callable,
                 start_at: str = "",
                 duration: float = 0.0,
                 tries: int = 0,
                 dependencies: list[Callable] | None = None,
                 *args, **kwargs):
        self.start_at = datetime.strptime(start_at,
                                          TIME_FORMAT) if start_at else None
        self.duration = timedelta(seconds=duration) if duration else None
        if start_at and duration:
            self.end_at = self.start_at + self.duration
        elif duration:
            self.end_at = datetime.now() + self.duration
        else:
            self.end_at = None
        self.tries = tries
        self.dependencies = dependencies or []
        self.task = task(*args, **kwargs)
        self.id = uid()

    def __str__(self):
        return (f'Job {self.id} start_at={self.start_at} end_at={self.end_at}'
                f'tries={self.tries}')

    def run(self):
        """Runs the job."""
        result = next(self.task)
        return result
