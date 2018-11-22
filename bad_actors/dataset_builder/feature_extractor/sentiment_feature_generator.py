from nltk.sentiment.vader import SentimentIntensityAnalyzer
from base_feature_generator import BaseFeatureGenerator
import nltk
class Sentiment_Feature_Generator(BaseFeatureGenerator):
    def __init__(self, db, **kwargs):
        BaseFeatureGenerator.__init__(self, db, **kwargs)
        nltk.download('vader_lexicon')
        self._sentence_analyser = SentimentIntensityAnalyzer()

    def authors_posts_semantic_average_compound(self, **kwargs):
        if 'posts' in kwargs.keys():
            posts = kwargs['posts']
            posts_counter = 0
            author_score = 0
            if posts is None:
                return -1
            for post in posts:
                author_score = author_score + self._get_sentence_scores(post.content, 'compound')
                posts_counter = posts_counter + 1
            author_score = float(author_score)/posts_counter
            return author_score

    def authors_posts_semantic_average_positive(self, **kwargs):
        if 'posts' in kwargs.keys():
            posts = kwargs['posts']
            posts_counter = 0
            author_score = 0
            if posts is None:
                return -1
            for post in posts:
                author_score = author_score + self._get_sentence_scores(post.content, 'pos')
                posts_counter = posts_counter + 1
            author_score = float(author_score) / posts_counter
            return author_score

    def authors_posts_semantic_average_negative(self, **kwargs):
        if 'posts' in kwargs.keys():
            posts = kwargs['posts']
            posts_counter = 0
            author_score = 0
            if posts is None:
                return -1
            for post in posts:
                author_score = author_score + self._get_sentence_scores(post.content, 'neg')
                posts_counter = posts_counter + 1
            author_score = float(author_score) / posts_counter
            return author_score

    def authors_posts_semantic_average_neutral(self, **kwargs):
        if 'posts' in kwargs.keys():
            posts = kwargs['posts']
            posts_counter = 0
            author_score = 0
            if posts is None:
                return -1
            for post in posts:
                author_score = author_score + self._get_sentence_scores(post.content, 'neu')
                posts_counter = posts_counter + 1
            author_score = float(author_score) / posts_counter
            return author_score

    def cleanUp(self):
        pass

    def _get_sentence_scores(self, sentence, param): #param = 'neg','neu','pos','compound'
        if sentence is None:
            return 0.0
        score = self._sentence_analyser.polarity_scores(sentence)
        return score[param]