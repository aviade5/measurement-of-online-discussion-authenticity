# Created by jorgeaug at 30/06/2016
from __future__ import print_function
import time
import logging
from DB.schema_definition import AuthorFeatures
import pandas as pd
from configuration.config_class import getConfig
import datetime
import time

class BaseFeatureGenerator():
    def __init__(self, db, **kwargs):
        #AbstractController.__init__(self, db)
        self._db = db
        self._config_parser = getConfig()
        self._targeted_social_network = self._config_parser.get("DEFAULT", "social_network_name")

        start_date = self._config_parser.get("DEFAULT", "start_date").strip("date('')")
        self._window_start = datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        self._window_size = datetime.timedelta(
            seconds=int(self._config_parser.get("DEFAULT", "window_analyze_size_in_sec")))
        self._window_end = self._window_start + self._window_size
        self._domain = unicode(self._config_parser.get(self.__class__.__name__, "domain"))

        if 'authors' in kwargs and 'posts' in kwargs:
            self.authors = kwargs['authors']
            self.author_guid_posts_dict = kwargs['posts']
        else:
            raise Exception('Author object was not passed as parameter')

        if kwargs.has_key('measure'):
            self._measure = kwargs['measure']
        if kwargs.has_key('calculator_type'):
            self._calculator_type = kwargs['calculator_type']
        if kwargs.has_key('aggregation_function'):
            self._aggregation_function = kwargs['aggregation_function']
        if kwargs.has_key('graph_type'):
            self._graph_type = kwargs['graph_type']
        if kwargs.has_key('targeted_class_field_name'):
            self._targeted_class_field_name = kwargs['targeted_class_field_name']

    def execute(self):
        start_time = time.time()
        info_msg = "execute started for " + self.__class__.__name__
        logging.info(info_msg)

        total_authors = len(self.authors)
        processed_authors = 0
        features = getConfig().eval(self.__class__.__name__, "feature_list")

        authors_features = []
        for author in self.authors:
            author_guid = author.author_guid
            if author_guid in self.author_guid_posts_dict.keys():
                posts = self.author_guid_posts_dict[str(author.author_guid)]
                getattr(self, 'cleanUp')()
                for feature in features:
                    author_feature = self.run_and_create_author_feature(author, feature, posts, author_guid, feature)
                    authors_features.append(author_feature)

            processed_authors += 1
            print ("\r processed authors "+str(processed_authors) + " from " + str(total_authors), end="")

        if authors_features:
            self.submit_author_features_to_db(authors_features)

        end_time = time.time()
        diff_time = end_time - start_time
        print ('execute finished in ' + str(diff_time) + ' seconds')

    def is_well_defined(self):
        return True

    def submit_author_features_to_db(self, authors_features):
        print('\n Beginning merging author_features objects')
        counter = 0
        for author_features_row in authors_features:
            counter += 1
            self._db.update_author_features(author_features_row)
            if counter == 100:
                print("\r " + str(self.__class__.__name__) + " merging author-features objects", end="")
                self._db.commit()
                counter = 0
        if counter != 0:
            self._db.commit()
        print('Finished merging author_features objects')

    def run_and_create_author_feature(self, author, feature, posts, id_val, feature_name):
        try:
            result = getattr(self, feature)(posts=posts, author=author)
            author_feature = AuthorFeatures()
            author_feature.author_guid = id_val
            author_feature.window_start = self._window_start
            author_feature.window_end = self._window_end
            author_feature.attribute_name = unicode(feature_name)
            author_feature.attribute_value = result
            return author_feature
        except Exception as e:
            info_msg = e.message
            logging.error(info_msg + feature)

    def run_and_create_author_feature_with_given_value(self, author, value, feature_name):
        try:
            result = value
            author_feature = AuthorFeatures()
            author_feature.author_guid = author
            author_feature.window_start = self._window_start
            author_feature.window_end = self._window_end
            author_feature.attribute_name = unicode(feature_name)
            author_feature.attribute_value = result
            return author_feature
        except Exception as e:
            info_msg = e.message
            logging.error(info_msg + str(value))

    def convert_posts_to_dataframe(self, posts):
        '''
          Input: list of posts
          Output: DataFrame with two columns: date and content where each row represents a post
        '''
        cols = ['date', 'content']
        data_frame = pd.DataFrame(columns=cols)
        for post in posts:
            data_frame.loc[len(data_frame)] = [post.date, post.content]
        return data_frame

    def insert_author_features_to_db(self, authors_features):
        logging.info("Inserting authors features to db")
        start_time = time.time()
        if authors_features:
            self.submit_author_features_to_db(authors_features)

        end_time = time.time()
        diff_time = end_time - start_time
        print ('execute finished in ' + str(diff_time) + ' seconds')

    def load_target_field_for_id(self, targeted_fields):
        author_id_texts_dict = {}
        for targeted_fields_dict in targeted_fields:
            table_name = targeted_fields_dict["table_name"]
            id_field = targeted_fields_dict["id_field"]
            targeted_field_name = targeted_fields_dict["targeted_field_name"]
            key_tuple = table_name + "-" + id_field + "-" + targeted_field_name
            where_clauses = targeted_fields_dict["where_clauses"]
            author_id_texts_dict[key_tuple] = {}

            tuples = self._db.get_targeted_records_by_id_targeted_field_and_table_name(id_field, targeted_field_name,
                                                                                       table_name, where_clauses)
            i = 1
            for tuple in tuples:
                i += 1
                author_id = tuple[0]
                # text could be content, description, etc
                text = tuple[1]
                if author_id not in author_id_texts_dict[key_tuple]:
                    author_id_texts_dict[key_tuple][author_id] = []
                    author_id_texts_dict[key_tuple][author_id].append(text)
                else:
                    author_id_texts_dict[key_tuple][author_id].append(text)
            if i==1:
                author_id_texts_dict[key_tuple] = {}
        return author_id_texts_dict

    def get_key_tuple(self, targeted_fields_dict):
        table_name = targeted_fields_dict["table_name"]
        id_field = targeted_fields_dict["id_field"]
        targeted_field_name = targeted_fields_dict["targeted_field_name"]
        key_tuple = table_name + "-" + id_field + "-" + targeted_field_name
        return key_tuple

    @staticmethod
    def create_author_feature(feature_name, author_guid, attribute_value, window_start, window_end):
        feature = AuthorFeatures()
        feature.author_guid = author_guid
        feature.attribute_name = feature_name
        feature.attribute_value = attribute_value
        feature.window_start = window_start
        feature.window_end = window_end
        return feature