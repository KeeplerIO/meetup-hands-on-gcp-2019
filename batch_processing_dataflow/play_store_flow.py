import argparse
import logging

import apache_beam as beam
from apache_beam.io import WriteToBigQuery
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


class ProcessCSV(beam.DoFn):
    def process(self, element, *args, **kwargs):
        import csv

        formated_element = [element.encode('utf8')]
        processed_csv = csv.DictReader(formated_element, fieldnames=['App', 'Category', 'Rating', 'Reviews', 'Size',
                                                                     'Installs', 'Type', 'Price', 'Content_Rating',
                                                                     'Genres', 'Last_Updated', 'Current_Ver',
                                                                     'Android_Ver'], delimiter=',')
        processed_fields = processed_csv.next()
        if processed_fields.get('Category').replace('.','').isdigit():
            return None
        return [processed_fields]


class ParseRecord(beam.DoFn):
    def process(self, element, *args, **kwargs):
        from datetime import datetime
        def string_to_megabyte(raw_string):
            if raw_string.endswith('K'):
                multiplier = 1000
            elif raw_string.endswith('M'):
                multiplier = 1000 * 1000
            else:
                return None
            return (float(raw_string[:-1]) * multiplier) / 1000000
        
        element['Rating'] = float(element['Rating']) if element['Rating'] is not None and element['Rating'] != element['Rating'] else None
        element['Size'] = string_to_megabyte(element['Size'])
        element['Price'] = float(element['Price'].replace("$",""))
        element['Installs'] = int(element['Installs'].replace("+", "").replace(",",""))
        element['Last_Updated'] = datetime.strptime(element['Last_Updated'], '%B %d, %Y').isoformat()
        logging.info(element)
        return [element]


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
        raw_lines = pipeline | 'ReadFromCsv' >> ReadFromText(known_args.input, skip_header_lines=1)

        lines = raw_lines | 'processCsv' >> beam.ParDo(ProcessCSV())
        
        output = lines | 'parseRecord' >> beam.ParDo(ParseRecord())

        output | WriteToBigQuery(known_args.table_output,
                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)

        result = pipeline.run()
        result.wait_until_finish()
        logging.info('Finished.')
        logging.info('result: %s', result)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

