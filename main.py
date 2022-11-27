import logging
import os
import shutil
import sys

from forecasting.api_client import YandexWeatherAPI
from forecasting.utils import CITIES
from job import Job
from scheduler import Scheduler

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s-%(name)s-%(levelname)s-%(message)s',
                    stream=sys.stdout)

logger = logging.getLogger(__name__)


def make_and_delete_dirs():
    logger.info("make_and_delete_dirs start")
    for number in range(2):
        dir_name = f"dir_{number}"
        if not os.path.isdir(dir_name):
            os.makedirs(dir_name)
        shutil.rmtree(dir_name)
        yield
    logger.info("make_and_delete_dirs finished")


def write_lines():
    logger.info("write_lines start")
    with open("lines.txt", "w") as file:
        file.writelines([f"{number}\n" for number in range(100)])
        yield
    logger.info("write_lines finished")


def read_lines():
    logger.info("read_lines start")
    with open("lines.txt", "r") as file:
        for line in file.readlines():
            logger.info(line)
            yield
    logger.info("read_lines finished")


def get_forecast():
    logger.info("get_forecast start")
    api = YandexWeatherAPI()
    for city in CITIES:
        logger.info(f"forecast: {city}")
        result = api.get_forecasting(city)
        logger.info(f"forecast: {result}")
        yield
    logger.info("get_forecast finished")


if __name__ == '__main__':
    scheduler = Scheduler()

    job = Job(make_and_delete_dirs)
    scheduler.schedule(job)
    job = Job(write_lines)
    scheduler.schedule(job)
    job = Job(read_lines)
    scheduler.schedule(job)
    job = Job(get_forecast)
    scheduler.schedule(job)

    scheduler.run()
