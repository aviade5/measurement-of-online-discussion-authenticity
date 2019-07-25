#  Created by YY at 1/2/2019
#  Modified by YY at 1/12/19
from __future__ import print_function

from dataset_builder.feature_extractor.base_feature_generator import BaseFeatureGenerator
from dataset_builder.feature_extractor.cooperation_topic_feature_generator import contains_posts_decorator
from preprocessing_tools.abstract_controller import AbstractController
from commons.commons import *
import numpy as np
import datetime
from dateutil import parser
import logging
import time
from scipy.stats import skew, kurtosis
from copy import deepcopy

'''
This class is responsible for generating features based on authors and posts temporal properties
Each author-feature and post-feature pair will be written in the AuthorFeature table
'''


class TemporalFeatureGenerator(BaseFeatureGenerator):

    def __init__(self, db, **kwargs):
        BaseFeatureGenerator.__init__(self, db, **kwargs)
        self._feature_list = self._config_parser.eval(self.__class__.__name__, "feature_list")
        self._delta_times = self._config_parser.eval(self.__class__.__name__, "delta_time")
        self._aggregated_functions = self._config_parser.eval(self.__class__.__name__, "aggregated_functions")
        self._targeted_fields = self._config_parser.eval(self.__class__.__name__, "targeted_fields")
        self._prefix = self.__class__.__name__
        self._delta_time = 1
        self._author_dict = {}

    def execute(self, window_start=None):
        self._author_dict = self._db.get_author_dictionary()
        function_name = 'extract_temporal_features'
        start_time = time.time()
        info_msg = "execute started for " + function_name + " started at " + str(start_time)
        logging.info(info_msg)

        aggregated_functions = map(eval, self._aggregated_functions)
        total_authors_features = []
        for targeted_fields_dict in self._targeted_fields:
            source_id_source_element_dict, source_id_target_elements_dict = self._load_data_using_arg(
                targeted_fields_dict)
            for self._delta_time in self._delta_times:
                suffix = u'delta_time_{}'.format(self._delta_time)
                authors_features = self._get_features(source_id_source_element_dict, source_id_target_elements_dict,
                                                      suffix, targeted_fields_dict, aggregated_functions)
                total_authors_features.extend(self._add_suffix_to_author_features(authors_features, suffix))
                if len(total_authors_features) > 100000:
                    self._db.add_author_features_fast(total_authors_features)
                    total_authors_features = []

        self._db.add_author_features_fast(total_authors_features)

        stop_time = time.time()
        info_msg = "execute ended at " + str(stop_time)
        logging.info(info_msg)

    def forward_date_range(self, date_list, span_days):
        date_list = sorted(date_list)
        span = timedelta(days=span_days)
        start, end, = date_list[0], date_list[-1]
        date_bucket_range = [0] * ((end - start).days / span.days + 1)
        i = 0
        bucket = 0
        while start <= end:
            current = start + span
            count = 0
            while i < len(date_list) and date_list[i] < current:
                i += 1
                count += 1
            date_bucket_range[bucket] = count
            bucket += 1
            start = current
        return date_bucket_range

    def cleanUp(self):
        pass

    """ Temporal features from time delta list """
    @contains_posts_decorator
    def posts_temporal(self, **kwargs):
        posts_dates = map(lambda p: getattr(p, 'date'), kwargs['posts'])
        return self.forward_date_range(posts_dates, self._delta_time)

    @contains_posts_decorator
    def authors_temporal(self, **kwargs):
        author_guids = set(map(lambda p: p.author_guid, kwargs['posts']))
        authors = [self._author_dict[a_guid] for a_guid in author_guids if a_guid in self._author_dict]
        authors_dates = map(lambda a:  parser.parse(getattr(a, 'created_at')), authors)
        return self.forward_date_range(authors_dates, self._delta_time)

