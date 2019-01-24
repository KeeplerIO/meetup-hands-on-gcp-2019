from google.cloud import bigquery


def load_to_bigquery(event, context):
    client = bigquery.Client()
    dataset_id = 'googleplaystore'
    bucket = event['bucket']
    file = event['name']
    url = 'gs://{0}/{1}'.format(bucket, file)
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.skip_leading_rows = 1
    job_config.max_bad_records = 1
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    load_job = client.load_table_from_uri(
        url,
        dataset_ref.table('playstore_app'),
        job_config=job_config)  # API request
    print('Starting job {}'.format(load_job.job_id))

    result = load_job.result()  # Waits for table load to complete.
    print(result)
    print('Job finished.')
    
