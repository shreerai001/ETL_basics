import schedule
import time


def job():
    print("Reading time")


def coding():
    print("programming time")


schedule.every(10).seconds.do(job())
