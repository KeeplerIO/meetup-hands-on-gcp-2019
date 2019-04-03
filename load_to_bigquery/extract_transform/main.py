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
    <YOUR CODE>
    
    print('Job finished.')
