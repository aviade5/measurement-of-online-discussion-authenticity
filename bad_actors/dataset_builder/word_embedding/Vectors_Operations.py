import numpy as np

import commons
from dataset_builder.feature_extractor.base_feature_generator import BaseFeatureGenerator
from operator import sub
import logging.config

class Vector_Operations():
    @staticmethod
    def create_authors_feature_from_two_vectors(func, first_author_vector_dict, second_author_vector_dict,
                                                first_table_name,
                                                first_targeted_field_name, first_word_embedding_type, second_table_name,
                                                second_targeted_field_name, second_word_embedding_type, window_start,
                                                window_end):
        authors_features = []
        for author_id in first_author_vector_dict.keys():
            feature_name = u'word_embeddings_subtraction_'+first_table_name + "_" + first_targeted_field_name + "_" + first_word_embedding_type + "_TO_" \
                           + second_table_name + "_" + second_targeted_field_name + "_" + second_word_embedding_type + "_DISTANCE-FUNCTION_" + func
            first_vector = first_author_vector_dict[author_id]
            second_vector = second_author_vector_dict[author_id]
            # attribute_value = getattr(commons.commons, func)(first_vector, second_vector
            attribute_value = Vector_Operations.oparate_on_two_vectors(commons.commons, func,
                                                                       first_vector,
                                                                       second_vector)
            feature = BaseFeatureGenerator.create_author_feature(feature_name, author_id, attribute_value,
                                                                 window_start,
                                                                 window_end)
            authors_features.append(feature)
        return authors_features

    @staticmethod
    def oparate_on_two_vectors(func_location, func, vector_1, vector_2):
        value = getattr(func_location, func)(vector_1, vector_2)
        return value

    @staticmethod
    def create_features_from_word_embedding_dict(author_guid_word_embedding_dict, targeted_table, targeted_field_name,
                                                 targeted_word_embedding_type,window_start, window_end, db, commit_treshold, prefix=''):
        authors_features = []
        counter = 0
        for author_guid in author_guid_word_embedding_dict.keys():
            counter +=1
            if counter%commit_treshold==0:
                db.add_author_features(authors_features)
                db.commit()
                authors_features=[]
            author_vector = author_guid_word_embedding_dict[author_guid]
            feature_name = targeted_word_embedding_type+'_'+targeted_table + "_" + targeted_field_name
            dimentions_feature_for_author = Vector_Operations.create_author_feature_for_each_dimention(author_vector,
                                                                                                       feature_name,
                                                                                                       author_guid,
                                                                                                       window_start,
                                                                                                       window_end,
                                                                                                       prefix)
            authors_features = authors_features + dimentions_feature_for_author
        db.add_author_features(authors_features)
        db.commit()
        authors_features = []

    @staticmethod
    def create_author_feature_for_each_dimention(vector, feature_name, author_guid, window_start, window_end,
                                                 prefix_name=u''):
        authors_features = []
        dimension_counter = 0
        for dimension in vector:
            dimension = round(dimension, 4)

            final_feature_name = prefix_name + feature_name + "_d" + str(
                dimension_counter)
            dimension_counter = dimension_counter + 1
            feature = BaseFeatureGenerator.create_author_feature(final_feature_name, author_guid, dimension, window_start,
                                                                 window_end)
            authors_features.append(feature)
        return authors_features

    @staticmethod
    def create_subtruction_dimension_features_from_authors_dict(first_author_guid_word_embedding_vector_dict,
                                                                second_author_guid_word_embedding_vector_dict,
                                                                first_table_name, first_targeted_field_name,
                                                                first_word_embedding_type, second_table_name,
                                                                second_targeted_field_name, second_word_embedding_type,
                                                                window_start, window_end):
        author_features = []
        for author_guid in first_author_guid_word_embedding_vector_dict.keys():
            first_vector = first_author_guid_word_embedding_vector_dict[author_guid]
            second_vector = second_author_guid_word_embedding_vector_dict[author_guid]
            current_authors_feature = Vector_Operations.create_subtruction_dimention_features(first_vector,
                                                                                              second_vector,
                                                                                              first_table_name,
                                                                                              first_targeted_field_name,
                                                                                              first_word_embedding_type,
                                                                                              second_table_name,
                                                                                              second_targeted_field_name,
                                                                                              second_word_embedding_type,
                                                                                              window_start, window_end,
                                                                                              author_guid)
            author_features = author_features + current_authors_feature
        return author_features

    @staticmethod
    def create_subtruction_dimention_features(vector_1, vector_2, first_table_name, first_targeted_field_name,
                                              first_word_embedding_type, second_table_name,
                                              second_targeted_field_name, second_word_embedding_type,
                                              window_start, window_end, author_guid):
        result_vector = tuple(map(sub, vector_1, vector_2))
        feature_name = "subtraction_"+first_table_name + "_" + first_targeted_field_name + "_" + first_word_embedding_type +"_TO_" + \
                       second_table_name + "_" + second_targeted_field_name + "_" + second_word_embedding_type
        feature_id = author_guid
        author_features = Vector_Operations.create_author_feature_for_each_dimention(result_vector, feature_name,
                                                                                     feature_id,
                                                                                     window_start, window_end)
        return author_features