from google.cloud import bigquery


def load_to_bigquery(event, context):
    client = bigquery.Client()
    dataset_id = 'googleplaystore'
    bucket = event['bucket']
    file = event['name']
    url = 'gs://{0}/{1}'.format(bucket, file)
    
    <YOUR CODE>

    print('Starting job {}'.format(load_job.job_id))

    result = load_job.result()  # Waits for table load to complete.
    print(result)
    print('Job finished.')
