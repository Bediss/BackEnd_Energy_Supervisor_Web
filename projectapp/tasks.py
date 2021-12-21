from celery import shared_task
from celery import Celery
#from projectapp.models import Widget
#the first argument is the name of the current module
app = Celery('projectapp', broker='pyamqp://guest@localhost//')

@shared_task(name="sum_two_numbers",time_limit=300)
def add(x, y):
    return  x + y


@app.task
def mul(x, y):
    return x * y


@shared_task
def xsum(numbers):
    return sum(numbers)