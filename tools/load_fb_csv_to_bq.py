#!/usr/bin/python

from google.cloud import bigquery
import time
import sys

client = bigquery.Client()
file=sys.argv[1]

def load_csv_to_bq(fd):
    dataset_id = "elastic"
    tbl = "fb_" + time.strftime("%Y%m%d")

    dataset_ref = client.dataset(dataset_id)

    job_config = bigquery.LoadJobConfig()

    job_config.schema = [
        bigquery.SchemaField('ts', 'STRING'),
        bigquery.SchemaField('k1', 'STRING'),
        bigquery.SchemaField('host', 'STRING'),
        bigquery.SchemaField('file', 'STRING'),
        bigquery.SchemaField('reason', 'STRING'),
        bigquery.SchemaField('log', 'STRING')
    ]

    job_config.skip_leading_rows = 0

    job_config.source_format = bigquery.SourceFormat.CSV

    load_job = client.load_table_from_file(
        fd,
        dataset_ref.table(tbl),
        job_config=job_config)

    assert load_job.job_type == 'load'

    load_job.result()

    assert load_job.state == 'DONE'

with open( file , "rb" ) as fd:
    load_csv_to_bq(fd)
