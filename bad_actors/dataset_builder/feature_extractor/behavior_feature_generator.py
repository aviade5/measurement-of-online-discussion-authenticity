# Created by jorgeaug at 30/06/2016
import math
import pandas as pd
from dateutil import parser
import datetime
from base_feature_generator import BaseFeatureGenerator
from commons.consts import Social_Networks
from commons.commons import *


class BehaviorFeatureGenerator(BaseFeatureGenerator):
    def __init__(self, db, **kwargs):
        BaseFeatureGenerator.__init__(self, db, **kwargs)
        self._retweet_count = db.get_retweet_count()
        self._retweets = pd.DataFrame(db.get_retweets().items(), columns=['post_id','content'])
        
    def cleanUp(self):
        pass

    def average_minutes_between_posts(self, **kwargs):

        if 'posts' in kwargs.keys():
            posts = kwargs['posts']
            data_frame = self.convert_posts_to_dataframe(posts)
            time_series = data_frame['date']
            time_series = time_series.sort_values(ascending=False)
            if len(time_series) > 1:
                diff = time_series - time_series.shift()
                mean = diff.mean().total_seconds() / 60
                if not math.isnan(mean):
                    avg_minutes_between_posts = math.fabs(mean)
                else:
                    avg_minutes_between_posts = None

            else:
                avg_minutes_between_posts = None

            return avg_minutes_between_posts

    def average_posts_per_day_active_days(self, **kwargs):
        if 'posts' in kwargs.keys():
            posts = kwargs['posts']
            data_frame = self.convert_posts_to_dataframe(posts)
            data_frame['date'] = data_frame['date'].map(lambda x: x.strftime('%d-%m-%Y'))
            grouped_data = data_frame.groupby('date')
            daily_count = grouped_data.date.apply(pd.value_counts)
            avg_posts_per_active_day = daily_count.mean()
            return avg_posts_per_active_day

    def average_posts_per_day_total(self, **kwargs):
        if 'author' in kwargs.keys():
            author = kwargs['author']
            if self._targeted_social_network == Social_Networks.TWITTER:
                return self._calculate_average_posts_per_day_total_within_twitter(author)
            elif self._targeted_social_network == Social_Networks.TUMBLR:
                return self._calculate_average_posts_per_day_total_within_tumblr(author)


    def retweet_count(self, **kwargs):
        if 'author' in kwargs.keys():
            author = kwargs['author']
            if author.author_guid in self._retweet_count.keys():
                return self._retweet_count[author.author_guid]
            else:
               0

    def average_retweets(self, **kwargs):
        if 'author' in kwargs.keys():
            author = kwargs['author']
            posts = kwargs['posts']
            if author.author_guid in self._retweet_count.keys() and len(posts) > 0:
                return float(self._retweet_count[author.author_guid])/float(len(posts))
            else:
                return 0.0

    def received_retweets_count(self, **kwargs):
        if 'author' in kwargs.keys():
            author = kwargs['author']
            screen_name = str('RT @'+author.author_screen_name)
            retweets_count = len(self._retweets[self._retweets['content'].str.contains(screen_name)].index)
            return retweets_count

    def _calculate_average_posts_per_day_total_within_twitter(self, author):
        if author.created_at is not None and author.statuses_count is not None:
            account_creation_date = parser.parse(author.created_at).date()
            today_date = datetime.date.today()
            delta = today_date - account_creation_date
            total_days = float(delta.days)

            total_posts = float(author.statuses_count)
            if total_posts > 0 and total_days > 0:
                return total_posts / total_days

    def _calculate_average_posts_per_day_total_within_tumblr(self, author):
        if author.created_at is not None:
            author_creation_date = author.created_at
            account_creation_date = str_to_date(author_creation_date)
            account_creation_date = account_creation_date.date()

            today_date = datetime.date.today()
            delta = today_date - account_creation_date
            total_days = float(delta.days)

            author_guid = author.author_guid

            posts = self._db.get_posts_by_author_guid(author_guid)
            total_posts = len(posts)
            #total_posts = float(author.statuses_count)
            if total_posts > 0 and total_days > 0:
                return total_posts / total_days
