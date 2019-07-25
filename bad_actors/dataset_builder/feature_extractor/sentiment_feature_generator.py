from nltk.sentiment.vader import SentimentIntensityAnalyzer

from base_feature_generator import BaseFeatureGenerator
import numpy as np
from scipy.stats import skew, kurtosis
import nltk


class Sentiment_Feature_Generator(BaseFeatureGenerator):
    def __init__(self, db, **kwargs):
        BaseFeatureGenerator.__init__(self, db, **kwargs)
        self._features = self._config_parser.eval(self.__class__.__name__, "feature_list")
        self._targeted_fields = self._config_parser.eval(self.__class__.__name__, "targeted_fields")
        self._aggregated_functions = self._config_parser.eval(self.__class__.__name__, "aggregated_functions")
        nltk.download('vader_lexicon')
        self._sentence_analyser = SentimentIntensityAnalyzer()
        # self._target_fields = []

    def execute(self, window_start=None):
        aggregated_functions = map(eval, self._aggregated_functions)
        total_authors_features = []
        for target_fields in self._targeted_fields:
            source_id_source_element_dict, source_id_target_elements_dict = self._load_data_using_arg(target_fields)
            suffix = u''
            authors_features = self._get_features(source_id_source_element_dict, source_id_target_elements_dict,
                                                  suffix, target_fields, aggregated_functions)
            total_authors_features.extend(self._add_suffix_to_author_features(authors_features, suffix))
            if len(total_authors_features) > 100000:
                self.insert_author_features_to_db(total_authors_features)
                total_authors_features = []

        self.insert_author_features_to_db(total_authors_features)

    def authors_posts_semantic_compound(self, **kwargs):
        if 'posts' in kwargs.keys():
            return self._get_posts_semantic_scores_base(kwargs['posts'], 'compound')

    def authors_posts_semantic_positive(self, **kwargs):
        if 'posts' in kwargs.keys():
            return self._get_posts_semantic_scores_base(kwargs['posts'], 'pos')

    def authors_posts_semantic_negative(self, **kwargs):
        if 'posts' in kwargs.keys():
            return self._get_posts_semantic_scores_base(kwargs['posts'], 'neg')

    def authors_posts_semantic_neutral(self, **kwargs):
        if 'posts' in kwargs.keys():
            return self._get_posts_semantic_scores_base(kwargs['posts'], 'neu')

    def _get_posts_semantic_scores_base(self, posts, scores_param):
        return [self._get_sentence_scores(post.content, scores_param) for post in posts]

    def cleanUp(self, **kwargs):
        pass

    def _get_sentence_scores(self, sentence, param):  # param = 'neg','neu','pos','compound'
        if sentence is None:
            return 0.0
        score = self._sentence_analyser.polarity_scores(sentence)
        return score[param]
