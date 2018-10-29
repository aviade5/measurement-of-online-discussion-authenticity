from __future__ import print_function

# import unicodedata
import sys
from datetime import timedelta
import os
import pandas as pd
from collections import defaultdict, Counter
from DB.schema_definition import Post, Claim_Tweet_Connection
from commons.commons import compute_post_guid, date_to_str
from commons.method_executor import Method_Executor
from vendors.GetOldTweets.got import manager

__author__ = "Aviad Elyashar"


class OldTweetsCrawler(Method_Executor):
    def __init__(self, db):
        Method_Executor.__init__(self, db)
        self._max_num_tweets = self._config_parser.eval(self.__class__.__name__, "max_num_tweets")
        self._max_num_of_objects_without_saving = self._config_parser.eval(self.__class__.__name__,
                                                                           "max_num_of_objects_without_saving")
        self._month_interval = self._config_parser.eval(self.__class__.__name__, "month_interval")
        self._output_folder_full_path = self._config_parser.eval(self.__class__.__name__, "output_folder_full_path")
        self._limit_start_date = self._config_parser.eval(self.__class__.__name__, "limit_start_date")
        self._limit_end_date = self._config_parser.eval(self.__class__.__name__, "limit_end_date")
        self._post_id_tweets_id_before_dict = defaultdict(set)
        self._post_id_tweets_id_after_dict = defaultdict(set)
        self._posts = []
        self._claim_post_connections = []

    def get_old_tweets_by_full_content(self):
        claims = self._db.get_posts()
        num_of_claims = len(claims)

        self._claims_dict = {c.post_id: c for c in claims}

        i = 0
        for claim in claims:
            assert isinstance(claim, Post)
            i += 1
            claim_content = claim.content
            self._retrieve_old_tweets(claim, claim_content)

        self._db.addPosts(self._posts)
        self._db.add_claim_connections(self._claim_post_connections)

        before_dict = self._post_id_tweets_id_before_dict
        after_dict = self._post_id_tweets_id_after_dict
        output_folder_path = self._output_folder_full_path

        self._export_csv_files_for_statistics(output_folder_path, before_dict, after_dict)

    def get_old_tweets_by_keywords(self):
        claims = self._db.get_posts()
        num_of_claims = len(claims)

        self._claims_dict = {c.post_id: c for c in claims}

        i = 0
        for claim in claims:
            assert isinstance(claim, Post)
            i += 1
            keywords_str = claim.tags
            keywords = keywords_str.split(",")
            retrieved_tweets_count = 0
            for keyword in keywords:
                retrieved_tweets_count += len(self._retrieve_old_tweets(claim, keyword.lower().strip()))

            msg = "Processing claims {0}/{1}, Retreived {2} tweets".format(i, num_of_claims, retrieved_tweets_count)
            print(msg)

        self._db.addPosts(self._posts)
        self._db.add_claim_connections(self._claim_post_connections)
        before_dict = self._post_id_tweets_id_before_dict
        after_dict = self._post_id_tweets_id_after_dict
        output_folder_path = self._output_folder_full_path
        self._export_csv_files_for_statistics(output_folder_path, before_dict, after_dict)

    def _get_and_add_tweets_by_content_and_date(self, claim, content, since, until):
        tweetCriteria = manager.TweetCriteria().setQuerySearch(content).setMaxTweets(self._max_num_tweets)
        if self._limit_start_date:
            tweetCriteria = tweetCriteria.setSince(since)
        if self._limit_end_date:
            tweetCriteria = tweetCriteria.setUntil(until)
        tweets = manager.TweetManager.getTweets(tweetCriteria)
        self._add_tweets_and_connections_to_db(claim, tweets)
        return tweets

    def _add_tweets_and_connections_to_db(self, claim, tweets):
        original_claim_id = unicode(claim.post_id)
        post_type = claim.post_type

        posts_per_claim, claim_connections = self._convert_tweets_to_posts(tweets, original_claim_id, post_type)
        self._posts += posts_per_claim
        self._claim_post_connections += claim_connections
        if len(self._posts) > self._max_num_of_objects_without_saving:
            self._db.addPosts(self._posts)
            self._db.add_claim_connections(self._claim_post_connections)

            self._posts = []
            self._claim_post_connections = []

    def _convert_tweets_to_posts(self, tweets, original_claim_id, post_type):
        posts = []
        claim_post_connections = []
        for tweet in tweets:
            post, claim_post_connection = self._convert_tweet_to_post_and_claim_post_connection(tweet,
                                                                                                original_claim_id,
                                                                                                post_type)
            claim_post_connections.append(claim_post_connection)
            posts.append(post)
        return posts, claim_post_connections

    def _convert_tweet_to_post_and_claim_post_connection(self, tweet, original_claim_id, post_type):
        post = Post()

        post.post_osn_id = unicode(tweet.id)
        post_creation_date = tweet.date
        created_at = unicode(date_to_str(post_creation_date))
        post.created_at = created_at

        post.date = post_creation_date
        post.favorite_count = tweet.favorites
        post.retweet_count = tweet.retweets
        post.content = unicode(tweet.text)

        author_name = unicode(tweet.username)
        post.author = author_name

        post_url = tweet.permalink
        post.url = unicode(post_url)

        post_guid = compute_post_guid(post_url, author_name, created_at)
        post.guid = post_guid
        post.post_id = post_guid
        post.domain = self._domain

        # post.post_format = original_claim_id
        post.post_type = post_type

        claim_post_connection = Claim_Tweet_Connection()

        claim_post_connection.claim_id = original_claim_id
        claim_post_connection.post_id = post.post_id

        return post, claim_post_connection

    def _retrieve_old_tweets(self, claim, content):
        datetime_object = claim.date
        month_interval = timedelta(self._month_interval * 365 / 12)
        start_date = date_to_str(datetime_object - month_interval, "%Y-%m-%d")
        end_date = date_to_str(datetime_object + month_interval, "%Y-%m-%d")
        current_date = date_to_str(claim.date, "%Y-%m-%d")
        retrieved_tweets_count = 0
        tweets = []
        try:
            date_interval_dict = self._post_id_tweets_id_before_dict
            tweets = self._retrieve_tweets_between_dates(claim, content, start_date, end_date)

            retrieved_tweets_count = len(tweets)
        except:
            e = sys.exc_info()[0]
            print("tweet content: {0}, error:{1}".format(content, e))

        return tweets

    def _retrieve_tweets_between_dates(self, claim, content, start_date, current_date):
        original_claim_id = unicode(claim.post_id)
        tweets = self._get_and_add_tweets_by_content_and_date(claim, content, start_date, current_date)
        for tweet in tweets:
            if tweet.date < claim.date:
                self._post_id_tweets_id_before_dict[original_claim_id].add(tweet.id)
            else:
                self._post_id_tweets_id_after_dict[original_claim_id].add(tweet.id)
        return tweets

    # content can be keyword or full content

    def _export_csv_files_for_statistics(self, output_folder_path, before_dict, after_dict):
        claims = self._db.get_posts()
        claims_dict = {c.post_id: c for c in claims}
        if not os.path.exists(output_folder_path):
            os.makedirs(output_folder_path)
        claim_to_tweet_count_rows = []
        claim_to_tweet_id_rows = []
        post_ids = set(before_dict.keys() + after_dict.keys())
        for post_id in post_ids:
            claim = claims_dict[post_id]

            claim_content = claim.content
            original_claim_keywords = claim.tags
            keywords = original_claim_keywords.split(",")
            claim_keywords = "||".join(keywords)
            snopes_publication_date = date_to_str(claim.date)
            tweet_ids_before = before_dict[post_id]
            tweet_ids_after = after_dict[post_id]
            tweet_before_count = len(tweet_ids_before)
            tweet_after_count = len(tweet_ids_after)
            tweet_combined_count = len(tweet_ids_before & tweet_ids_after)
            total_tweet_count = tweet_before_count + tweet_after_count - tweet_combined_count
            claim_to_tweet_count_rows.append(
                (post_id, claim_content, claim_keywords, snopes_publication_date, tweet_before_count, tweet_after_count, tweet_combined_count, total_tweet_count))
            claim_to_tweet_id_rows.append((post_id, claim_content, claim_keywords, snopes_publication_date, ','.join(tweet_ids_before), ','.join(tweet_ids_after)))

        post_id_num_of_tweets_df = pd.DataFrame(claim_to_tweet_count_rows,
                                                columns=['claim_id', 'claim_content', 'claim_keywords', 'snopes_publication_date', 'num_before', 'num_after', 'num_combine', 'total'])
        post_id_num_of_tweets_df.to_csv(output_folder_path + "post_id_num_of_tweets.csv", index=False)

        post_id_tweet_ids_str_df = pd.DataFrame(claim_to_tweet_id_rows,
                                                columns=['claim_id', 'claim_content', 'claim_keywords', 'snopes_publication_date', 'tweet_ids_before', 'tweet_ids_after'])
        post_id_tweet_ids_str_df.to_csv(output_folder_path + "post_id_tweet_ids.csv", index=False)
