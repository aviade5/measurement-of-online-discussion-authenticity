from __future__ import print_function
import pandas as pd
from math import log
from DB.schema_definition import AuthorFeatures
from commons.commons import *
from nltk.corpus import stopwords
import nltk
from nltk.stem.porter import PorterStemmer
from commons.commons import *
from dataset_builder.feature_extractor.base_feature_generator import BaseFeatureGenerator


class TF_IDF_Feature_Generator(BaseFeatureGenerator):
    def __init__(self, db, **kwargs):
        BaseFeatureGenerator.__init__(self, db, **kwargs)
        self._aggregation_functions_names = self._config_parser.eval(self.__class__.__name__,
                                                                     "aggregation_functions_names")

        self._remove_zeros = self._config_parser.eval(self.__class__.__name__, "remove_zeros")
        self._posts_by_author_guid = {}
        self._word_idf_dict = {}
        self._word_n_containing_dict = {}

    def execute(self, window_start=None):
        self._posts_by_author_guid = self._db.get_author_guid_post_dict()
        token_dict = {}
        self._stemmer = PorterStemmer()

        for author in self._posts_by_author_guid.keys():
            all_authors_content = self._concat_all_post_contents_of_author(self._posts_by_author_guid[author])
            token_dict[author] = all_authors_content
        author_results = {}
        for author in token_dict.keys():
            author_results[author] = []
            splited_words = token_dict[author].split(' ')
            self.author_words_tf_idf_dict = {}
            for word in splited_words:
                res = self.tfidf(word, token_dict[author], token_dict.values(), self.author_words_tf_idf_dict)
                if self._remove_zeros and res == 0.0:
                    continue
                author_results[author].append(res)

        author_features = []
        for author in author_results.keys():
            for aggregation_functions_name in self._aggregation_functions_names:
                tf_idf_series = pd.Series(author_results[author])
                aggregated_tf_idf_score = getattr(tf_idf_series, aggregation_functions_name)()

                author_feature = self.create_author_feature("ttf-idf_feature_generator_" + aggregation_functions_name,
                                                            author, aggregated_tf_idf_score, self._window_start,
                                                            self._window_end)
                author_features.append(author_feature)
        self._db.add_author_features(author_features)

    def _create_tf_idf_author_feature(self, tweet_id, aggregation_functions_name, aggregated_tf_idf_score):
        author_feature = AuthorFeatures()
        author_feature.author_guid = tweet_id
        author_feature.attribute_name = "tf-idf_" + aggregation_functions_name
        author_feature.attribute_value = aggregated_tf_idf_score
        return author_feature

    def _concat_all_post_contents_of_author(self, posts):
        res = ""
        for post in posts:
            content = clean_tweet(post.content)
            res = res + " " + content
        return res[1:]

    def tf(self, word, document):
        res = (str(document).split().count(word) + 0.0) / len(str(document).split())
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
        self._word_idf_dict = {}
        self._word_n_containing_dict = {}
        self.author_words_tf_idf_dict = {}
