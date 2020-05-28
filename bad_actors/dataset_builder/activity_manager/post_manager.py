from __future__ import print_function
import schedule
from datetime import timedelta, time
from datetime import datetime
import time
from commons.commons import *
from Twitter_API.twitter_api_requester import TwitterApiRequester
from commons.method_executor import Method_Executor
import pandas as pd
from twitter.error import TwitterError
import random
import requests
from twitter_rest_api.twitter_rest_api import Twitter_Rest_Api
from commons.consts import DB_Insertion_Type



class PostManager(Method_Executor):
    def __init__(self, db):
        Method_Executor.__init__(self, db)
        self._twitter_api = TwitterApiRequester()
        self._social_network_crawler = Twitter_Rest_Api(db)
        self._influence_strategy = self._config_parser.eval(self.__class__.__name__, "post_strategy")
        self._source_group = self._config_parser.eval(self.__class__.__name__, "source_group")
        self._target_group = self._config_parser.eval(self.__class__.__name__, "target_group")
        self._user_id = self._config_parser.eval(self.__class__.__name__, "user_id")
        self._number_of_posts = self._config_parser.eval(self.__class__.__name__, "number_of_posts")
        self._retweet_precent = self._config_parser.eval(self.__class__.__name__, "retweet_precent")
        self._related_hashtags = self._config_parser.eval(self.__class__.__name__, "related_hashtags")
        self._posts_num = self._config_parser.eval(self.__class__.__name__, "posts_num")

    def _publish_post(self, post, message, media):
        self._twitter_api = TwitterApiRequester()
        statuses = self._twitter_api.api.PostUpdate(message, media)
        activity = self._db.create_activity(self._user_id, post.post_osn_id, statuses.id, 'twitter_post', 'twitter',
                                            message, datetime.datetime.utcnow(), "twitter")
        return activity

    def _retweet_post(self, post):
        self._twitter_api = TwitterApiRequester()
        statuses = self._twitter_api.api.PostRetweet(post.post_osn_id, trim_user=False)
        activity = self._db.create_activity(self._user_id, post.post_osn_id, statuses.id, 'twitter_retweet', 'twitter',
                                            post.content, datetime.datetime.utcnow(), "twitter")
        return activity

    def _get_posts(self):

        team_guid = self._db.get_author_guid_by_screen_name(self._source_group)
        team_posts = []
        if (len(team_guid) == 1):
            team_posts = self._db.get_posts_by_author_guid(team_guid[0])
        else:
            for i in team_guid:
                team_posts.append(self._db.get_posts_by_author_guid(i))
            team_posts = [sublist for item in team_posts for sublist in item]

        return self._split_into_retweet_and_tweet(team_posts)

    def _split_into_retweet_and_tweet(self,team_posts):
        team_posts_without_retweet = []
        team_posts_with_retweet = []
        for post in team_posts:
            prefix = str(post.content[0:2])
            if prefix != "RT":
                team_posts_without_retweet.append(post)
            else:
                team_posts_with_retweet.append(post)
        return team_posts_without_retweet, team_posts_with_retweet

    def influence_strategy_sort(self, team_posts_without_retweet, team_posts_with_retweet):
        if self._influence_strategy == "last":
            team_posts_without_retweet.sort(key=lambda x: x.date, reverse=True)
            team_posts_with_retweet.sort(key=lambda x: x.date, reverse=True)

        if self._influence_strategy == "popular":
            team_posts_without_retweet.sort(key=lambda x: x.favorite_count, reverse=True)
            team_posts_with_retweet.sort(key=lambda x: x.favorite_count, reverse=True)

        return team_posts_without_retweet, team_posts_with_retweet

    def _create_tweet_content(self, post):

        tweet_length = 270
        message = post.content
        media = post.media_path
        message = message + '\n' + "@" + self._target_group + " #" + self._target_group + " " + str(
            datetime.datetime.utcnow())
        for i in self._related_hashtags:
            if (len(message + " " + i) > tweet_length):
                break
            else:
                message = message + " " + i
        if (len(message) > tweet_length):
            message = message[0:tweet_length]

        return message, media

    def _post_func(self, post, team_posts_without_retweet):
        try:
            message, media = self._create_tweet_content(post)
            activity = self._publish_post(post, message, media)
            flag = 'True'
            print("date: " + str(datetime.datetime.utcnow()) + " post number: " + str(
                self._posts_num) + " succeed to send a tweet")
            self._db.addPosts([activity])
            return str(flag), team_posts_without_retweet
        except Exception as e:
            print("Failed  {}".format(e))
            flag = False
            if (len(team_posts_without_retweet) >= 1):
                del team_posts_without_retweet[0]
            return str(flag), team_posts_without_retweet

    def _retweet_func(self, post, team_posts_with_retweet):
        try:
            activity = self._retweet_post(post)
            flag = 'True'
            print("date: " + str(datetime.datetime.utcnow()) + " post number: " + str(
                self._posts_num) + " succeed to send a retweet")
            self._db.addPosts([activity])
            return str(flag), team_posts_with_retweet
        except Exception as e:
            print("Failed  {}".format(e))
            flag = False
            if (len(team_posts_with_retweet) >= 1):
                del team_posts_with_retweet[0]
            return str(flag), team_posts_with_retweet

    def _execute_post_process(self, team_posts_without_retweet, team_posts_with_retweet):

        if (self._number_of_posts <= self._posts_num):
            return
        else:
            team_posts_without_retweet, team_posts_with_retweet = self.influence_strategy_sort(
                team_posts_without_retweet, team_posts_with_retweet)

            flag = 'False'
            self._posts_num = self._posts_num + 1
            coin = random.uniform(0, 1)

            while flag == 'False':
                if (coin >= self._retweet_precent):
                    post, team_posts_without_retweet, team_posts_with_retweet = self._selecting_post(
                        team_posts_without_retweet, team_posts_with_retweet, "post")
                else:
                    post, team_posts_without_retweet, team_posts_with_retweet = self._selecting_post(
                        team_posts_without_retweet, team_posts_with_retweet, "retweet")

                if (coin >= self._retweet_precent):
                    flag, team_posts_without_retweet = self._post_func(post, team_posts_without_retweet)

                else:
                    flag,team_posts_with_retweet = self._retweet_func(post, team_posts_with_retweet)


    def _selecting_post(self, team_posts_without_retweet, team_posts_with_retweet, type):

        post_exist = True
        while post_exist == True:
            if type == "post":
                ans = team_posts_without_retweet[0]
                message = ans.content
                while "@" + self._target_group in message:
                    if (len(team_posts_without_retweet) >= 1):
                        del team_posts_without_retweet[0]
                        ans = team_posts_without_retweet[0]
                        message = ans.content
                    else:
                        print("End of tweets")
            else:
                ans = team_posts_with_retweet[0]

            post_exist = self._db.check_if_post_sent(ans, self._user_id)
            if (post_exist == True):
                if type == "post":
                    if (len(team_posts_without_retweet) >= 1):
                        del team_posts_without_retweet[0]
                    else:
                        print("End of tweets")
                else:
                    if (len(team_posts_with_retweet) >= 1):
                        del team_posts_with_retweet[0]
                    else:
                        print("End of tweets")

        return ans, team_posts_without_retweet, team_posts_with_retweet

    def time_schedule(self):

        hours_in_a_day = 24 * 60
        minute_window = float(hours_in_a_day) / self._number_of_posts

        while True:
            self._posts_num = 0
            self._convert_timeline_tweets_to_posts_for_author_screen_names(self._source_group)
            without_retweet, with_retweet = self._get_posts()
            self._execute_post_process(without_retweet, with_retweet)

            schedule.every(minute_window).minutes.do(self._execute_post_process, without_retweet, with_retweet)

            while True:
                if (self._number_of_posts <= self._posts_num):
                    break
                schedule.run_pending()
                time.sleep(1)

    def calculate_posts_stat(self):

        author_guid = "0927dc1a-8bcb-3488-99ed-7a962aee56e2"
        date = "2020-03-04 03:28:20"

        ids = self._db.source_destination()
        author_posts = self._db.posts_statics_from_date(author_guid, date)
        author_posts_guid = self._db.posts_statics_guids(author_guid, date)
        author_posts_guid = [ids[i] for i in author_posts_guid]
        influencers_posts = self._db.posts_statics_from_date_for_specific_posts(author_posts_guid)

        df1 = pd.DataFrame(author_posts,
                           columns=['author_guid', 'post_count', 'retweet_sum', 'favorite_sum', 'retweet_avg',
                                    'favorite_avg'])
        df2 = pd.DataFrame(influencers_posts,
                           columns=['author_guid', 'post_count', 'retweet_sum', 'favorite_sum', 'retweet_avg',
                                    'favorite_avg'])
        frames = [df1, df2]
        result = pd.concat(frames)
        result.to_csv(author_guid + ".csv")

    def _convert_timeline_tweets_to_posts_for_author_screen_names(self, author_screen_names):
        posts = []
        for i, account_screen_name in enumerate(author_screen_names):
            try:

                timeline_tweets = self._social_network_crawler.get_timeline(account_screen_name, 3200)
                if timeline_tweets is not None:
                    print("\rSearching timeline tweets for author_guid: {0} {1}/{2} retrieved:{3}".format(
                        account_screen_name, i,
                        len(author_screen_names), len(timeline_tweets)),
                          end='')
                    for timeline_tweet in timeline_tweets:
                        post = self._db.create_post_from_tweet_data_api(timeline_tweet, self._domain)
                        posts.append(post)
            except requests.exceptions.ConnectionError as errc:
                x = 3


            except TwitterError as e:
                if e.message == "Not authorized.":
                    logging.info("Not authorized for user id: {0}".format(account_screen_name))
                    continue

        self._db.addPosts(posts)
        self.fill_data_for_sources()

    def fill_author_guid_to_posts(self):
        posts = self._db.get_all_posts()
        num_of_posts = len(posts)
        for i, post in enumerate(posts):
            msg = "\rPosts to fill: [{0}/{1}]".format(i, num_of_posts)
            print(msg, end="")
            post.author_guid = compute_author_guid_by_author_name(post.author)
        self._db.addPosts(posts)
        self._db.insert_or_update_authors_from_posts(self._domain, {}, {})

    def fill_data_for_sources(self):
        print("---complete_missing_information_for_authors_by_screen_names ---")

        twitter_author_screen_names = self._db.get_missing_data_twitter_screen_names_by_posts()
        author_type = None
        are_user_ids = False
        inseration_type = DB_Insertion_Type.MISSING_DATA_COMPLEMENTOR
        # retrieve_full_data_for_missing_users
        i = 1
        for author_screen_names in self._split_into_equal_chunks(twitter_author_screen_names, 10000):
            twitter_users = self._social_network_crawler.handle_get_users_request(
                author_screen_names, are_user_ids, author_type, inseration_type)

            print('retrieve authors {}/{}'.format(i * 10000,
                                                  len(twitter_author_screen_names)))
            i += 1
            self._social_network_crawler.save_authors_and_connections(twitter_users, author_type, inseration_type)

        self.fill_author_guid_to_posts()

        print("---complete_missing_information_for_authors_by_screen_names was completed!!!!---")
        #logging.info("---complete_missing_information_for_authors_by_screen_names was completed!!!!---")

    def _split_into_equal_chunks(self,elements, num_of_chunks):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(elements), num_of_chunks):
            yield elements[i:i + num_of_chunks]

