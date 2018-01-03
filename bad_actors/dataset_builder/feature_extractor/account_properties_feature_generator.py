# Created by jorgeaug at 30/06/2016
from dateutil import parser
import datetime
from base_feature_generator import BaseFeatureGenerator
from commons.consts import Social_Networks
from commons.commons import *

'''
This class is responsible for generating features based on account properties such as account age (in days)
number of followers, number of characters in screen name, etc.
Each author-feature pair will be written in the AuthorFeature table
'''


class AccountPropertiesFeatureGenerator(BaseFeatureGenerator):
    def cleanUp(self):
        pass

    def account_age(self, **kwargs):
        if 'author' in kwargs.keys():
            author = kwargs['author']
            created_at = author.created_at
            if created_at is not None:
                account_creation_date = parser.parse(created_at).date()
                today_date = datetime.date.today()
                delta = today_date - account_creation_date
                return delta.days
        else:
            raise Exception('Author object was not passed as parameter')

    def number_followers(self, **kwargs):
        if 'author' in kwargs.keys():
            author = kwargs['author']
            return author.followers_count
        else:
            raise Exception('Author object was not passed as parameter')


    def number_friends(self, **kwargs):
        if 'author' in kwargs.keys():
            author = kwargs['author']
            return author.friends_count
        else:
            raise Exception('Author object was not passed as parameter')

    def friends_followers_ratio(self, **kwargs):
        if 'author' in kwargs.keys():
            author = kwargs['author']
            friends_count = author.friends_count
            followers_count = author.followers_count

            if friends_count > 0 and followers_count > 0:
                return float(friends_count) / float(followers_count)
            else:
                return 0.0

        else:
            raise Exception('Author object was not passed as parameter')

    def number_of_crawled_posts(self, **kwargs):
        if 'posts' in kwargs.keys():
            posts = kwargs['posts']
            return len(posts)
        else:
            raise Exception('Posts list object was not passed as parameter')

    def number_of_posts(self, **kwargs):
        if 'author' in kwargs.keys():
            author = kwargs['author']
            return author.statuses_count
        else:
            raise Exception('Author object was not passed as parameter')

    def default_profile(self, **kwargs):
        if 'author' in kwargs.keys():
            author = kwargs['author']
            return author.default_profile
        else:
            raise Exception('Author object was not passed as parameter')

    def default_profile_image(self, **kwargs):
        if 'author' in kwargs.keys():
            author = kwargs['author']
            return author.default_profile_image
        else:
            raise Exception('Author object was not passed as parameter')

    def listed_count(self, **kwargs):
        if 'author' in kwargs.keys():
            author = kwargs['author']
            return author.listed_count
        else:
            raise Exception('Author object was not passed as parameter')
    def verified(self, **kwargs):
        if 'author' in kwargs.keys():
            author = kwargs['author']
            return author.verified
        else:
            raise Exception('Author object was not passed as parameter')
    def screen_name_length(self, **kwargs):
        if 'author' in kwargs.keys():
            author = kwargs['author']
            return len(author.author_screen_name)
        else:
            raise Exception('Author object was not passed as parameter')

    def author_screen_name(self, **kwargs):
        if 'author' in kwargs.keys():
            author = kwargs['author']
            return author.author_screen_name
        else:
            raise Exception('Author object was not passed as parameter')

    def author_type(self, **kwargs):
        if 'author' in kwargs.keys():
            author = kwargs['author']
            return author.author_type
        else:
            raise Exception('Author object was not passed as parameter')

    def author_sub_type(self, **kwargs):
        if 'author' in kwargs.keys():
            author = kwargs['author']
            return author.author_sub_type
        else:
            raise Exception('Author object was not passed as parameter')