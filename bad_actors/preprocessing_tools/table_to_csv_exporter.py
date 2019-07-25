from DB.schema_definition import Post
from preprocessing_tools.abstract_controller import AbstractController
import os
import pandas as pd


class TableToCsvExporter(AbstractController):
    def __init__(self, db):
        super(TableToCsvExporter, self).__init__(db)
        self._output_path = self._config_parser.eval(self.__class__.__name__, "output_path")
        self._posts_table_name = self._config_parser.eval(self.__class__.__name__, "posts_table_name")
        self._authors_table_name = self._config_parser.eval(self.__class__.__name__, "authors_table_name")
        self._claims_table_name = self._config_parser.eval(self.__class__.__name__, "claims_table_name")
        self._claim_tweet_connection_table_name = self._config_parser.eval(self.__class__.__name__,
                                                                           "claim_tweet_connection_table_name")

    def setUp(self):
        if not os.path.exists(self._output_path):
            os.makedirs(self._output_path)

    def execute(self, window_start=None):
        self._export_table_data('posts', self._output_path + self._posts_table_name)
        self._export_table_data('authors', self._output_path + self._authors_table_name)
        self._export_table_data('claim_tweet_connection', self._output_path + self._claim_tweet_connection_table_name)
        self._export_table_data('claims', self._output_path + self._claims_table_name)

    def _export_table_data(self, db_table_name, export_csv_name):
        table = pd.read_sql_table(db_table_name, self._db.engine)
        table.to_csv(export_csv_name + '.csv')
