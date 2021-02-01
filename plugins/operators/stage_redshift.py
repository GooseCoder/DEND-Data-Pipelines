from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook

import datetime
import logging

class StageToRedshiftOperator(BaseOperator):
    """
    Data Operator that loads the JSON formatted files from S3 
    into Amazon Redshift.
    """

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 aws_connection_id,
                 redshift_connection_id,
                 s3_origin,
                 s3_prefix,
                 schema_target,
                 table_target,
                 options,
                 *args, **kwargs):
        """
        Class constructor.
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_origin = s3_origin
        self.s3_prefix = s3_prefix
        self.target_schema = schema_target
        self.target_table = table_target
        self.aws_connection_id = aws_connection_id
        self.redshift_connection_id = redshift_connection_id
        self.autocommit = True
        self.options = options
        self.region = 'us-west-2'

    def execute(self, context):
        """
        Executes the copy operation.
        """

        self.log.info('Starting COPY')        
        credentials = aws_hook.get_credentials()
        aws_hook = AwsHook(self.aws_connection_id)
        copy_options = '\n\t\t\t'.join(self.options)
        self.hook = PostgresHook(postgres_connection_id=self.redshift_connection_id)

        copy_query = """
            COPY {schema}.{table}
            FROM 's3://{s3_bucket}/{s3_key}'
            with credentials
            'aws_access_key_id={aws_access_key};aws_secret_access_key={aws_secret_key}'
            region 'us-west-2'
            {copy_options};
        """.format(table=self.target_table,
                   schema=self.target_schema,
                   aws_access_key=credentials.access_key,
                   aws_secret_key=credentials.secret_key,
                   s3_bucket=self.s3_origin,
                   s3_key=self.s3_prefix,
                   copy_options=copy_options)

        self.log.info(f'COPY data from bucket s3://{self.s3_origin}/{self.s3_prefix} to {self.target_schema}.{self.target_table} into Redshift')
        self.hook.run(copy_query, self.autocommit)
        self.log.info("COPY complete!")





