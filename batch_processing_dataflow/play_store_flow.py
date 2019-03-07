import argparse
import logging

import apache_beam as beam
from apache_beam.io import WriteToBigQuery
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


class ProcessCSV(beam.DoFn):
    def process(self, element, *args, **kwargs):
        import csv

        formated_element = [element]
        processed_csv = csv.DictReader(formated_element, fieldnames=['App', 'Category', 'Rating', 'Reviews', 'Size',
                                                                     'Installs', 'Type', 'Price', 'Content Rating',
                                                                     'Genres', 'Last Updated', 'Current Ver',
                                                                         'Android Ver'], delimiter=',')
        processed_fields = processed_csv.next()
        logging.info('Processed line: {}'.format(processed_csv))
        return processed_fields


def run(argv=None):
    """Main entry point. It defines and runs the pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='gs://meetup-batch-processing/input/googleplaystore.csv',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        default='gs://meetup-batch-processing/output/googleplaystore.csv',
                        help='Output file to process.')
    parser.add_argument('--table-output',
                        dest='table_output',
                        default='meetup-hands-on-gcp-2019:googleplaystore_batch_dataflow.play_store',
                        help='Bigquery table name for output.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options=pipeline_options) as pipeline:
        lines = pipeline | 'ReadFromCsv' >> ReadFromText(known_args.input, skip_header_lines=1)

        output = lines | 'processCsv' >> beam.ParDo(ProcessCSV())

        output | WriteToBigQuery(known_args.table_output,
                                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                 create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)

        result = pipeline.run()
        result.wait_until_finish()
        logging.info('Finished.')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
