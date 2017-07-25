import os
import requests
from string import Template
import datetime
from datetime import timedelta, date

API_URL = 'http://analytics.hike.in/job_completed'
JOB_COMPLETED_TEMPLATE = Template("payments_transfer_$sink-$date")
start_date = datetime.datetime(2017, 7, 20, 0)
end_date = datetime.datetime(2017, 7, 24, 23)


def date_range(start_date, end_date):
    d = start_date
    delta = datetime.timedelta(hours=1)
    while d < end_date:
        d += delta
        yield d.strftime('%Y-%m-%d-%H')


def main():
    for current__iteration_date in date_range(start_date, end_date):
        print "Working for " + str(current__iteration_date)

        job_name_gs = JOB_COMPLETED_TEMPLATE.substitute(sink='gs', date=current__iteration_date)
        json_gs = {'name': job_name_gs}
        response_gs = requests.post(API_URL, json=json_gs)
        response_gs.raise_for_status()
        print "Completed for " + str(job_name_gs)

        job_name_s3 = JOB_COMPLETED_TEMPLATE.substitute(sink='s3', date=current__iteration_date)
        json_s3 = {'name': job_name_s3}
        response_s3 = requests.post(API_URL, json=json_s3)
        response_s3.raise_for_status()
        print "Completed for " + str(job_name_s3)


if __name__ == "__main__":
    main()
