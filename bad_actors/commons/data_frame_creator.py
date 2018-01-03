# Created by Aviad Elyashar (aviade@post.bgu.ac.il) at 07/09/2016
# Ben Gurion University of the Negev - Department of Information Systems Engineering

import pandas as pd
import logging
from preprocessing_tools.abstract_executor import AbstractExecutor


class DataFrameCreator(AbstractExecutor):
    def __init__(self, db):
        AbstractExecutor.__init__(self, db)
        self.normalize = self._config_parser.get(self.__class__.__name__, "normalize")
        self._all_authors_features = self._config_parser.eval(self.__class__.__name__, 'all_authors')
        self._default_cols = ['author_guid', 'attribute_name', 'attribute_value']
        self._author_features_data_frame = {}

    def setUp(self):
        pass

    def execute(self, window_start):
        self.create_author_features_data_frame()

    def create_author_features_data_frame(self):
        logging.info("Start getting authors features")
        if self._all_authors_features:
            author_features_objects = self._db.get_author_features()
        else:
            author_features_objects = self._db.get_author_features_labeled_authors_only()
        logging.info("Finished getting authors features")

        if not author_features_objects:
            logging.info("The table AUTHOR_FEATURES has no records. Execution is stopped, bye...")
            exit(0)

        logging.info("Start converting authors features into a dataframe")

        if self._all_authors_features:
            author_features_dataframe = self._convert_author_features_to_data_frame(author_features_objects)
        else:
            author_features_dataframe = self._convert_labeled_only_authors_features_to_dataframe(author_features_objects)
        logging.info("Finished converting authors features into a dataframe")

        logging.info("Start pivoting authors features rows into columns")
        self._author_features_data_frame = self._pivot_dataframe(author_features_dataframe)
        logging.info("Finished pivoting authors features rows into columns")

    '''
    This function receives a list of AuthorFeature objects and converts it into a pandas.DataFrame object
    to be able to apply the pivoting function
    '''

    def _convert_author_features_to_data_frame(self, author_features):
        data_frame = pd.DataFrame([[getattr(i, j) for j in self._default_cols] for i in author_features],
                                  columns=self._default_cols)
        #data_frame = data_frame.fillna(value='?')
        return data_frame

    def _convert_labeled_only_authors_features_to_dataframe(self, author_features):
        indices = [0, 3, 4]
        data_frame = pd.DataFrame([[i[j] for j in indices] for i in author_features], columns=self._default_cols)
        #data_frame = data_frame.fillna(value='?')
        return data_frame

    '''
    This function receives a DataFrame consisting of the following columns:
    author_guid, attribute_name, attribute_value
    1,attrib_1,value_1
    1,attrib_2,value_2
    2,attrib_1,value_1
    2,attrib_2,value_2

    and applies the pandas.DataFrame.pivot function which converts the dataframe into the following sructure
    Author_guid, attribute_1, attribute_2
    1, value_1, value_2
    2, value_1, value_2

    Observation: I cannot get the pivoting function to work when dealing with author_guid, window_start, window_end
     The filtering of record according to a time window (window_start, window_end) will have to be done before calling
     this function
    '''

    def _pivot_dataframe(self, df):
        pivoted = df.pivot(columns='attribute_name', values='attribute_value', index='author_guid')
        #pivoted = pivoted.fillna(value='?')
        return pivoted

    def get_author_features_data_frame(self):
        data_frame = self._author_features_data_frame
        return data_frame

    def fill_empty_fields_for_dataframe(self, data_frame):
        data_frame = data_frame.fillna(value='?')
        return data_frame