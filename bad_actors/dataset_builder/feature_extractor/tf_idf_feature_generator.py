from __future__ import print_function

from collections import defaultdict
from math import log

import nltk
import pandas as pd
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer

from DB.schema_definition import AuthorFeatures
from commons.commons import *
from dataset_builder.feature_extractor.base_feature_generator import BaseFeatureGenerator


class TF_IDF_Feature_Generator(BaseFeatureGenerator):
    def __init__(self, db, **kwargs):
        BaseFeatureGenerator.__init__(self, db, **kwargs)
        self._aggregation_functions_names = self._config_parser.eval(self.__class__.__name__,
                                                                     "aggregation_functions_names")
        self._targeted_fields = self._config_parser.eval(self.__class__.__name__, "targeted_fields")
        self._remove_zeros = self._config_parser.eval(self.__class__.__name__, "remove_zeros")
        # self._posts_by_author_guid = self._db.get_author_guid_to_posts_dict()
        self._word_idf_dict = defaultdict()
        self._word_n_containing_dict = defaultdict()
        self._word_tf_dict = defaultdict()
        self._stemmer = PorterStemmer()

    def execute(self, window_start=None):
        source_id_combine_target_field = {}
        for target_field in self._targeted_fields:
            source_id_combine_field = self.get_source_id_destination_target_field(target_field)
            print("Create source id to posts content dict")
            for i, source_id in enumerate(source_id_combine_field):
                msg = "\r generate data for source {0}/{1}".format(str(i + 1), len(source_id_combine_field))
                print(msg, end='')
                all_authors_content = self._concat_all_post_contents_of_author(source_id_combine_field[source_id])
                source_id_combine_target_field[source_id] = all_authors_content
            author_results = defaultdict()
            print("\nCalculate sources tf-idf results")
            for i, source_id in enumerate(source_id_combine_target_field):
                msg = "\r calculate tf-idf for source {0}/{1}".format(str(i + 1), len(source_id_combine_target_field))
                print(msg, end='')
                author_results[source_id] = []
                splited_words = source_id_combine_target_field[source_id].split(' ')
                self.author_words_tf_idf_dict = defaultdict()
                for word in splited_words:
                    res = self.tfidf(word, source_id_combine_target_field[source_id], source_id_combine_target_field.values(), self.author_words_tf_idf_dict)
                    if self._remove_zeros and res == 0.0:
                        continue
                    author_results[source_id].append(res)

            author_features = []
            print("\ngenerate author features for sources")
            for i, source_id in enumerate(author_results):
                msg = "\r generate author feature for source {0}/{1}".format(str(i + 1), len(author_results))
                print(msg, end='')
                for aggregation_functions_name in self._aggregation_functions_names:
                    tf_idf_series = pd.Series(author_results[source_id])
                    aggregated_tf_idf_score = getattr(tf_idf_series, aggregation_functions_name)()

                    author_feature = self.create_author_feature(
                        "ttf-idf_feature_generator_" + aggregation_functions_name, source_id, aggregated_tf_idf_score,
                        self._window_start, self._window_end)
                    author_features.append(author_feature)
            print("\nAdd to author features DB")
            self._db.add_author_features(author_features)

    def _stem_tokens(self, tokens, stemmer):
        stemmed = stem_tokens(tokens, stemmer)
        return stemmed

    def _tokenize(self, text):
        tokens = nltk.word_tokenize(text)
        tokens = [token for token in tokens if token not in stopwords.words('english')]
        stems = self._stem_tokens(tokens, self._stemmer)
        return stems

    def _create_tf_idf_author_feature(self, tweet_id, aggregation_functions_name, aggregated_tf_idf_score):
        author_feature = AuthorFeatures()
        author_feature.author_guid = tweet_id
        author_feature.attribute_name = self.__class__.__name__ + "_" + aggregation_functions_name
        author_feature.attribute_value = aggregated_tf_idf_score
        return author_feature

    def _concat_all_post_contents_of_author(self, posts):
        return " ".join([clean_tweet(str(post)) for post in posts])

    def tf(self, word, document):
        if word in self._word_tf_dict:
            return self._word_tf_dict[word]
        else:
            res = (str(document).split().count(word) + 0.0) / len(str(document).split())
            self._word_tf_dict[word] = res
            return res

    def n_containing(self, word, documents):
        if word in self._word_n_containing_dict:
            return self._word_n_containing_dict[word]
        else:
            res = sum(1 for document in documents if word in document)
            return res

    def idf(self, word, documents):
        if word in self._word_idf_dict:
            return self._word_idf_dict[word]
        else:
            a = len(documents)
            b = (self.n_containing(word, documents))
            dev = (a + 0.0) / b
            res = log(dev, 10)
            self._word_idf_dict[word] = res
            return res

    def tfidf(self, word, document, documents, author_words_tfidf_dict):
        if word in author_words_tfidf_dict:
            return author_words_tfidf_dict[word]
        else:
            tf = self.tf(word, document)
            idf = self.idf(word, documents)
            res = tf * idf
            author_words_tfidf_dict[word] = res
            return res

    def clear_memo_dicts(self):
        self._word_tf_dict = defaultdict()
        self._word_idf_dict = defaultdict()
        self._word_n_containing_dict = defaultdict()
        self.author_words_tf_idf_dict = defaultdict()
