#!/usr/bin/env python
import argparse
import logging
import sys

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions

import datetime as dt
from datetime import timedelta, date
import time

STAT_API_SCHEMA = [
    
    {
        'name' : 'id',
        'type' : 'INTEGER',
        'mode' : 'REQUIRED'
    },
    {
        'name' : 'keyword',
        'type' : 'STRING',
        'mode' : 'REQUIRED'
    },
    {
        'name' : 'KeywordLocation',
        'type' : 'STRING',
        'mode' : 'NULLABLE'
    },
    {
        'name' : 'KeywordCategories',
        'type' : 'STRING',
        'mode' : 'NULLABLE'
    },
    {
        'name' : 'KeywordDevice',
        'type' : 'STRING',
        'mode' : 'NULLABLE'
    },
    {
        'name' : 'KeywordStats',
        'type' : 'RECORD',
        'mode' : 'REQUIRED',
        'fields': [
            {
                'name': 'AdvertiserCompetition',
                'type': 'FLOAT',
                'mode': 'NULLABLE'
            },
            {
                'name': 'GlobalSearchVolume',
                'type': 'INTEGER',
                'mode': 'NULLABLE'
            },
            {
                'name': 'TargetedSearchVolume',
                'type': 'INTEGER',
                'mode': 'NULLABLE'
            },
            {
                'name': 'CPC',
                'type': 'FLOAT',
                'mode': 'NULLABLE'
            }
        ]
    },
    {
        'name' : 'CreatedAt',
        'type' : 'DATETIME',
        'mode' : 'REQUIRED'
    },
    {
        'name' : 'Ranking',
        'type' : 'RECORD',
        'mode' : 'REQUIRED',
        'fields': [
            {
                'name': 'date',
                'type': 'DATETIME',
                'mode': 'REQUIRED'
            },
            {
                'name': 'type',
                'type': 'STRING',
                'mode': 'NULLABLE'
            },
            {
                'name' : 'Google',
                'type' : 'RECORD',
                'mode' : 'REQUIRED',
                'fields': [
                    {
                        'name': 'Rank',
                        'type': 'INTEGER',
                        'mode': 'REQUIRED'
                    },
                    {
                        'name': 'BaseRank',
                        'type': 'INTEGER',
                        'mode': 'REQUIRED'
                    },
                    {
                        'name': 'Url',
                        'type': 'STRING',
                        'mode': 'REQUIRED'
                    }


                ]
            }
            
        ]
    }
]

                
                
class StatAPI(beam.DoFn):
    def __init__(self, data={}):
        super(StatAPI, self).__init__()
        self.num_api_errors = Metrics.counter(self.__class__, 'num_api_errors')
        self.data = data
        
    def process(self, elem):
        import requests
        endpoint = "https://{0}.getstat.com/api/v2/{1}".format(self.data['subdomain'],self.data['api_key'])
        params = dict(
                        date = self.data['date'],
                        rank_type = 'highest',
                        crawled_keywords_only = 'true',
                        format = 'json'
                      )        
        try:
            for i in range(0,10):
                res = requests.get(endpoint,params=params)
                if res.status_code == 200:
                    break
                time.sleep(3)
            json_data = res.json()
            if 'Response' in json_data:
                response = res_data['Response']
                if response['responsecode'] == 200:
                    project = response['Project']
                    if isinstance(project, list):
                        yield project
                    else:
                        yield [project]
            

        except Exception as e:
            # Log and count parse errors
            self.num_api_errors.inc()
            logging.error('Exception: {}'.format(e))
            logging.error('Extract error on "%s"', elem)
            

            
class IterSites(beam.DoFn):
    def __init__(self):
        super(IterSites, self).__init__()
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, project):

        try:
            output = []
            if 'Site' in project:
                site = project['Site']
                if isinstance(site, list):
                    site = [site]
                for s in site:
                    s['ProjectName'] = project['Name']
                    output.append(s)
                    
            yield output

        except Exception as e:
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Exception: {}'.format(e))
            logging.error('Parse error on "%s"', elem)
            
            

class IterKeywords(beam.DoFn):
    def __init__(self):
        super(IterKeywords, self).__init__()
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, site):
        
        try:
            output = []
            if 'Keyword' in site:
                keyword = site['Keyword']
                if not isinstance(keyword, list):
                    keyword = [keyword]
                for k in keyword:
                    k['ProjectName'] = site['ProjectName']
                    k['Url'] = site['Url']
                    output.append(k)
                    
            yield output

        except Exception as e:
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Exception: {}'.format(e))
            logging.error('Parse error on "%s"', elem)
            

class ConvertBigQueryRowFn(beam.DoFn):
    def __init__(self, schema):
        super(ConvertBigQueryRowFn, self).__init__()
        self.schema = schema

    def process(self, keywords):
        for keyword in keywords:
            yield self.convert(self.schema, keyword)

    def convert(self, schema, keyword):
        from datetime import datetime
        output = {}
        for field in schema:
            column = field['name']
            if field['type'] == 'DATETIME':
                # Convert YYYY-MM-DDTHH:MM:SS
                output[column] = datetime.strptime(tweet[column], '%a %b %d %X +0000 %Y').strftime('%Y-%m-%dT%X')
            elif field['type'] == 'RECORD':
                output[column] = self.convert(field['fields'], keyword[column])
            else:
                output[column] = keyword[column]
        return output

class WriteToBigQuery(beam.PTransform):
    
    """Generate, format, and write BigQuery table row information."""
    def __init__(self, table_name, dataset, schema):
        """Initializes the transform.
        Args:
          table_name: Name of the BigQuery table to use.
          dataset: Name of the dataset to use.
          schema: Dictionary in the format {'column_name': 'bigquery_type'}
        """
        super(WriteToBigQuery, self).__init__()
        self.table_name = table_name
        self.dataset = dataset
        self.schema = schema

    def get_bq_table_schema(self):
        table_schema = bigquery.TableSchema()
        for field in self.schema:
            field_schema = self.get_bq_field_schema(field)
            table_schema.fields.append(field_schema)
        return table_schema

    def get_bq_field_schema(self, field):
        field_schema = bigquery.TableFieldSchema()
        field_schema.name = field['name']
        field_schema.type = field['type']
        field_schema.mode = field['mode']
        if field['type'] == 'RECORD':
            for nested_field in field['fields']:
                nested_field_schema = self.get_bq_field_schema(nested_field)
                field_schema.fields.append(nested_field_schema)
        return field_schema

    def expand(self, pcoll):
        project = pcoll.pipeline.options.view_as(GoogleCloudOptions).project
        return (
            pcoll
            | 'ConvertBigQueryRowFn' >> beam.ParDo(ConvertBigQueryRowFn(self.schema))
            | beam.io.WriteToBigQuery(
                self.table_name, self.dataset, project, self.get_bq_table_schema()))

    
def run(argv=None):
    
    """Main entry point; defines and runs the hourly_team_score pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--subdomain',
        type=str,
        default='adaptpartners'
        required=True,
        help='Sub-domain for Stat API.')
    parser.add_argument('--api_key',
        type=str,
        default='pHRhgOkDJ8cmKCmKKoYwPDH3'
        required=True,
        help='API key for Stat API.')
    parser.add_argument('--date',
        type=str,
        default=(dt.date.today()-dt.timedelta(days = 2)).strftime("%Y-%m-%d")
        help='Run date in YYYY-MM-DD format. Defaults to today-2 if not given.')
    parser.add_argument('--dataset',
        type=str,
        required=True,
        help='BigQuery Dataset to write tables to. Must already exist.')
    parser.add_argument('--table_name',
        type=str,
        default='adapt_rankings',
        help='The BigQuery table name. Should not already exist.')
    parser.add_argument('--project',
        type=str,
        default='ap-ranking-data',
        help='The BigQuery table name. Should not already exist.')
    parser.add_argument('--runner',
        type=str,
        default='DataflowRunner',
        help='Type of DataFlow runner.')
    
    args, pipeline_args = parser.parse_known_args(argv)
    
        
    # Create and set your PipelineOptions.
    options = PipelineOptions(pipeline_args, flags=['--save_main_session',
                                     '--requirements_file=requirements.txt'
                                    ])

    # For Cloud execution, set the Cloud Platform project, job_name,
    # staging location, temp_location and specify DataflowRunner.
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = args.project
    google_cloud_options.job_name = ("{0}-{1}".format(args.project, str(dt.datetime.today().strftime("%m%dt%H%M")) ) 
    google_cloud_options.staging_location = "gs://{0}/binaries".format(args.project)
    google_cloud_options.temp_location = "gs://{0}/temp".format(args.project)
    options.view_as(StandardOptions).runner = args.runner

    with beam.Pipeline(options=options) as pipeline:
        # Read projects from Stat API
        projects = (
            pipeline
            | 'create' >> beam.Create()
            | 'StatAPI'  >> beam.ParDo(StatAPI(data=args))
        )
        # Iteates Sites in Projects and writes to bigquery based on specified schema
        sites = (
            projects
            | 'IterSites' >> beam.ParDo(IterSites())
            | 'IterKeywords' >> beam.ParDo(IterKeywords())
            | 'WriteToBigQuery' >> WriteToBigQuery(args.table_name,
                args.dataset, STAT_API_SCHEMA))




if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
