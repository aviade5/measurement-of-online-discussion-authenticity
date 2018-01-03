# Created by jorgeaug at 30/06/2016
from base_feature_generator import BaseFeatureGenerator
from configuration.config_class import getConfig
import datetime


class SyntaxFeatureGenerator(BaseFeatureGenerator):

    def cleanUp(self):
        self._average_post_length = -1
        self._average_hash_tags = -1
        self._average_links = -1
        self._average_user_mentions = -1

    def _fill_posts(self, posts):
        number_posts = len(posts)
        number_hashtags = 0
        number_links = 0
        number_user_mentions = 0
        sum_of_posts_length = 0

        for post in posts:
            if post.content is not None:
                content = post.content.lower()
                tokens = [_ for _ in content.split(" ") if len(_) > 0]
                sum_of_posts_length += len(tokens)
                for token in tokens:
                    if '#' in token:
                        number_hashtags += 1
                    elif any(s in token for s in ('http://', 'https://')):
                        number_links += 1
                    elif '@' in token:
                        number_user_mentions += 1

        if number_posts > 0:
            self._average_post_length = float(sum_of_posts_length) / float(number_posts)
            self._average_hash_tags = float(number_hashtags) / float(number_posts)
            self._average_links = float(number_links) / float(number_posts)
            self._average_user_mentions =  float(number_user_mentions) / float(number_posts)

        else:
            self._average_post_length = 0.0
            self._average_hash_tags = 0.0
            self._average_user_mentions = 0.0
            self._average_links = 0.0

    def average_hashtags(self, **kwargs):
        if self._average_hash_tags == -1:
            self._fill_posts(kwargs['posts'])
        return self._average_hash_tags

    def average_links(self, **kwargs):
        if self._average_links == -1:
            self._fill_posts(kwargs['posts'])
        return self._average_links

    def average_user_mentions(self, **kwargs):
        if self._average_user_mentions == -1:
            self._fill_posts(kwargs['posts'])
        return self._average_user_mentions

    def average_post_lenth(self, **kwargs):
        if self._average_post_length == -1:
            self._fill_posts(kwargs['posts'])
        return self._average_post_length
