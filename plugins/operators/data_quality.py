#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 11 23:59:46 2020

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
class DataQualityOperator(BaseOperator):
    """
    Loads JSON files from a S3 bucket to an already exisiting table on a
    Redshift cluster.
    ___________________________________________________________________________
    :param redshift_conn_id: reference to a specific redshift cluster
    :type redshift_conn_id: str
    :param table_columns_dict: dictionary with keys being the names of tables,
        and values being either a string or a list of strings
    :type table_columns_dict: dict, {str: [str]} or {str: str}
    :param database: (optional) name of database, which overrides database
        name defined in connection
    :type database: str
    """

    ui_color = '#89DA59'

    # Inititialise operator instance___________________________________________
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table_columns_dict,
                 database=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_columns_dict = table_columns_dict
        self.database = database

    # Define static helper methods_____________________________________________
    @staticmethod
    def check_number_of_records(db_hook, table):
        """
        Checks that database table exists and is not empty and returns number
        of rows. Throws error if table does not exist or is empty.
        _______________________________________________________________________
        :param db_hook: hook to database connection
        :type redshift_conn_id: database connection instance
        :param table: name of table to quality check
        :type tablet: str
        _______________________________________________________________________
        :return param: number of rows in table
        :return type: int
        """
        records = db_hook.get_records(f"SELECT COUNT(*) FROM {table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError("Data quality check failed. "
                             f"{table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError("Data quality check failed. "
                             f"{table} contained 0 rows")
        else:
            return num_records

    @staticmethod
    def check_for_missing_data(db_hook, table, column):
        """
        Checks database column for missing values. Throws error if missing
        values are found.
        _______________________________________________________________________
        :param db_hook: hook to database connection
        :type redshift_conn_id: database connection instance
        :param table: name of database table
        :type tablet: str
        :param column: name of column to check for missing values
        :type tablet: str
        """
        records = db_hook.get_records(
                f"SELECT (COUNT(*) - COUNT({column})) FROM {table}")
        nulls = records[0][0]
        if nulls > 0:
            raise ValueError("There are missing values in column"
                             f"{column} in table {table}!")

    # Define execute function for operator_____________________________________
    def execute(self, context):
        self.log.info("DataQualityOperator is checking tables for correct "
                      "data.")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id,
                                schema=self.database)

        #  Check that table is not empty_______________________________________
        for table in self.table_columns_dict.keys():
            num_records = DataQualityOperator.check_number_of_records(
                redshift, table)
            self.log.info(f"Data quality check on table {table} passed with "
                          f"{num_records} records.")

        # Check for missing values_____________________________________________
        for table, columns in self.table_columns_dict.items():
            if type(columns) == list:
                for column in columns:
                    DataQualityOperator.check_for_missing_data(
                            redshift, table, column)
                    self.log.info(f"{column} in {table} has no missing "
                                  "values.")
            else:
                DataQualityOperator.check_for_missing_data(
                        redshift, table, columns)
                self.log.info(f"{columns} in {table} has no missing values.")
