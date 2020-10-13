#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 12 00:28:00 2020

@author: Emanuel Blei
"""

# =============================================================================
# Import libraries
# =============================================================================
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# =============================================================================
# Define Class
# =============================================================================
class LoadFactOperator(BaseOperator):
    """
    Loads JSON files from a S3 bucket to an already exisiting table on a
    Redshift cluster.
    ___________________________________________________________________________
    :param sql: the SQL code to be execute (templated)
    :type sql: can be either a string containing an SQL statement or reference
        to template file; reference to template file is recognised by str
        ending in '.sql'
    :param redshift_conn_id: reference to a specific redshift cluster
    :type redshift_conn_id: str
    :param target_table: name of destination table
    :type target_table: str
    :param database: (optional) name of database, which overrides database
        name defined in connection
    :type database: str
    """

    ui_color = '#F98866'
    template_fields = ('sql',)

    # Inititialise operator instance___________________________________________
    @apply_defaults
    def __init__(self,
                 sql,
                 redshift_conn_id,
                 target_table,
                 database=None,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.database = database

    # Define execute function for operator_____________________________________
    def execute(self, context):
        """

        """
        self.log.info(f"Load data into {self.target_table} fact table")

        # Hooks________________________________________________________________
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id,
                                schema=self.database)

        # Copy new records to fact table_______________________________________
        formatted_sql = f"INSERT INTO {self.target_table}({self.sql})"

        redshift.run(formatted_sql)
