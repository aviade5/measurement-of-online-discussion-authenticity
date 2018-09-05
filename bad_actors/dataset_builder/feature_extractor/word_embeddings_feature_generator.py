#Lior Bass 26.10.17
import operator

from dataset_builder.feature_extractor.base_feature_generator import BaseFeatureGenerator
from DB.schema_definition import AuthorFeatures
import time
import logging.config
from dataset_builder.word_embedding.Vectors_Operations import Vector_Operations


class Word_Embeddings_Feature_Generator(BaseFeatureGenerator):
    def __init__(self, db, **kwargs):
        BaseFeatureGenerator.__init__(self, db, **kwargs)
        self._targeted_author_word_embeddings = self._config_parser.eval(self.__class__.__name__, "targeted_author_word_embeddings")
        self._max_objects_without_saving = self._config_parser.eval(self.__class__.__name__, "max_objects_without_saving")

    def execute(self):
        logging.info("started extracting word_embbeddings feature generator:")
        counter =0
        authors_features = []
        for target_author_word_embeddings_dict in self._targeted_author_word_embeddings:
            counter += 1
            targeted_table = target_author_word_embeddings_dict["table_name"]
            targeted_field_name = target_author_word_embeddings_dict["targeted_field_name"]
            targeted_word_embedding_type = target_author_word_embeddings_dict["word_embedding_type"]

            targeted_word_embeddings_combination = targeted_table + "_" + targeted_field_name + "_" + targeted_word_embedding_type
            logging.info("currently extracting features of " + targeted_word_embeddings_combination + ": " + str(counter) + " out of " + str(
                len(self._targeted_author_word_embeddings)))

            author_guid_word_embeding_dict = self._db.get_author_guid_word_embedding_vector_dict(targeted_table, targeted_field_name, targeted_word_embedding_type)
            Vector_Operations.create_features_from_word_embedding_dict(author_guid_word_embeding_dict, targeted_table, targeted_field_name, targeted_word_embedding_type, self._window_start, self._window_end, self._db, self._max_objects_without_saving)
            # if len(authors_features) % self._max_objects_without_saving == 0:
            #     print('\n Beginning merging author_features objects')
            #     for author_features_row in authors_features:
            #         self._db.update_author_features(author_features_row)
            #     self._db.commit()
            #     authors_features = []

        # for author_features_row in authors_features:
        #     self._db.update_author_features(author_features_row)
        # self._db.commit()

