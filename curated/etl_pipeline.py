import schedule
import time
from datetime import datetime


def run_cust():
    print("Running ERP customer job")
    import transformation4


def run_loc():
    print("Running ERP location job")
    import transformation5


def run_px():
    print("Running ERP product job")
    import transformation6

def run_cust2():
    print("Running CRM customer job")
    import transformation1

def run_px2():
    print("Running CRM product job")
    import transformation2

def run_sales():
    print("Running CRM sales job")
    import transformation3

# run once immediately
run_cust()

# then schedule recurring runs
schedule.every().hour.do(run_cust)
schedule.every().day.do(run_loc)
# schedule.every().day.at("02:00").do(run_loc)
schedule.every().day.do(run_px)
# schedule.every().day.at("03:00").do(run_px)
schedule.every().day.do(run_cust2)
schedule.every().day.do(run_px2)
schedule.every().day.do(run_sales)



print("Scheduler started...")

while True:
    schedule.run_pending()
    for job in schedule.jobs:
        print(job)
    time.sleep(5)
    print('running')
















































