#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 12 00:24:23 2020

@author: Emanuel Blei
"""

# =============================================================================
# Import libraries
# =============================================================================
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os


# =============================================================================
# Define Class
# =============================================================================
class StageToRedshiftOperator(BaseOperator):
    """
    Loads JSON files from a S3 bucket to an already exisiting table on a
    Redshift cluster.
    ___________________________________________________________________________
    :param s3_key: source data file path on S3 bucket (templated)
    :type s3_key: str
    :param redshift_conn_id: reference to a specific redshift cluster
    :type redshift_conn_id: str
    :param aws_credentials_id: name of AWS credentials stored by Airflow
    :type aws_credentials_id: str
    :param table: name of destination table where data is to be written to
    :type table: str
    :param s3_bucket: name of S3 bucket where source data is held
    :type s3_bucket: str
    :param json_path: (optional) the complete file path to a json_path file
    :type json_path: str
    :param region: (optional) S3 bucket region, required if Redshift cluster
        and S3 bucket are in different AWS regions
    :type region: str
    :param overwrite: (optional) if True destination database is emptied
        before new data is uploaded (defaults to False)
    :type overwrite: bool
    :param database: (optional) name of database, which overrides database
        name defined in connection
    :type database: str
    """

    template_fields = ('s3_key',)
    ui_color = '#358140'

    copy_sql = ("""
                COPY {destination_table}
                FROM '{source}'
                ACCESS_KEY_ID '{key_id}'
                SECRET_ACCESS_KEY '{secret_key}'
                FORMAT AS JSON '{json_path}'
                """)

    # Inititialise operator instance___________________________________________
    @apply_defaults
    def __init__(self,
                 s3_key,
                 redshift_conn_id,
                 aws_credentials_id,
                 table,
                 s3_bucket,
                 json_path="auto",
                 region=None,
                 overwrite=False,
                 database=None,
                 *args, **kwargs):


        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path
        self.region = region
        self.overwrite = overwrite
        self.database = database

    # Define execute function for operator_____________________________________
    def execute(self, context):

        # Hooks________________________________________________________________
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id,
                                schema=self.database)

        # Empty existing records from table____________________________________
        if self.overwrite:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run(f"DELETE FROM {self.table}")

        # Copy new records over from S3 bucket to Redshift_____________________
        rendered_key = self.s3_key.format(**context)
        s3_path = os.path.join("s3://", self.s3_bucket, rendered_key)
        self.log.info(f"Copying data from {s3_path} to {self.table} on "
                      "Redshift")

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
                destination_table=self.table,
                source=s3_path,
                key_id=credentials.access_key,
                secret_key=credentials.secret_key,
                json_path=self.json_path)

        if self.region is not None:
            formatted_sql += f"REGION '{self.region}'"

        redshift.run(formatted_sql)
