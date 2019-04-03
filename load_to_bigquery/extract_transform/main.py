import pandas as pd
import os


def transform_to_mb(val):
    if val.endswith('K'):
        multiplier = 1000
    elif val.endswith('M'):
        multiplier = 1000 * 1000
    else:
        return None
    return (float(val[:-1]) * multiplier) / 1000000


def extract_and_transform(event, context):
    bucket = event['bucket']
    file = event['name']
    url = 'gs://{0}/{1}'.format(bucket, file)
    dataset = pd.read_csv(url, sep=',', header=0)
    dataset.dropna(subset=['Android Ver'], inplace=True)
    dataset['Installs'] = dataset['Installs'].str.replace("+", "").str.replace(",", "").astype(int)
    dataset['Last Updated'] = pd.to_datetime(dataset['Last Updated'], format='%B %d, %Y')
    dataset['Size'] = dataset['Size'].apply(transform_to_mb)
    bucket_name = os.environ.get('BUCKET', 'meetup-step-processing-output')
    dataset.to_csv('gs://{0}/{1}'.format(bucket_name, file), sep=',', header=True, index=False)
    print('Job finished.')
