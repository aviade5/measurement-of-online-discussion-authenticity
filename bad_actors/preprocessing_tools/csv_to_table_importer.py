from __future__ import print_function
import datetime

from DB.schema_definition import Post, date
from commons.commons import convert_str_to_unicode_datetime
from preprocessing_tools.abstract_controller import AbstractController
from csv import DictReader
import os
import numpy as np
import pandas as pd


class CsvToTableImporter(AbstractController):
    def __init__(self, db):
        super(CsvToTableImporter, self).__init__(db)
        self._input_path = self._config_parser.eval(self.__class__.__name__, "input_path")
        self._posts_table_name = self._config_parser.eval(self.__class__.__name__, "posts_table_name")
        self._authors_table_name = self._config_parser.eval(self.__class__.__name__, "authors_table_name")
        self._claims_table_name = self._config_parser.eval(self.__class__.__name__, "claims_table_name")
        self._claim_tweet_connection_table_name = self._config_parser.eval(self.__class__.__name__,
                                                                           "claim_tweet_connection_table_name")

    def execute(self, window_start=None):
        self.import_csv_to_table(self._posts_table_name)
        self.import_csv_to_table(self._authors_table_name)
        self.import_csv_to_table(self._claims_table_name)
        self.import_csv_to_table(self._claim_tweet_connection_table_name)

    def import_csv_to_table(self, input_table_name):
        origin_table = pd.read_sql_table(input_table_name, self._db.engine)
        imported_table = pd.DataFrame.from_csv(self._input_path + input_table_name + '.csv')
        merged_tables = pd.merge(origin_table, imported_table, how='outer')
        assert isinstance(merged_tables, pd.DataFrame)
        object_table = merged_tables.select_dtypes(include=['object']).astype('unicode')
        primitive_table = merged_tables.select_dtypes(exclude=['object'])
        merged_tables = object_table.join(primitive_table)
        row_count = merged_tables.shape[0]
        max_object_without_save = 100000
        start = max_object_without_save
        merged_tables[:min(start, row_count)].to_sql(input_table_name, con=self._db.engine, if_exists='replace', index=False)
        while start < row_count:
            print("Add rows to DB {0}/{1}".format(start, row_count), end="")
            merged_tables[start:min(start + max_object_without_save, row_count)].to_sql(input_table_name, con=self._db.engine, if_exists='append', index=False)
            start += max_object_without_save
            self._db.session.commit()
        print()
