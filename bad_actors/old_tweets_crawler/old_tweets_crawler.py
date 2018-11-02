from __future__ import print_function

# import unicodedata
import sys
from collections import defaultdict
from datetime import timedelta

from DB.schema_definition import Post, Claim_Tweet_Connection
from commons.commons import compute_post_guid, date_to_str, compute_author_guid_by_author_name
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
        self._claim_id_tweets_id_before_dict = defaultdict(set)
        self._claim_id_tweets_id_after_dict = defaultdict(set)
        self._posts = []
        self._claim_post_connections = []
        self._retrieved = 0

    def get_old_tweets_by_claims_content(self):
        self._base_retrieve_tweets_from_claims(self._retrieve_tweets_from_claims_by_content)

    def get_old_tweets_by_claims_keywords(self):
        self._base_retrieve_tweets_from_claims(self._retrieve_tweets_from_claims_by_keywords)

    def _retrieve_tweets_from_claims_by_content(self, claims):
        for i, claim in enumerate(claims):
            tweets = self._retrieve_old_tweets(claim, claim.description)
            msg = "Processing claims {0}/{1} Retreived {2} tweets".format(str(i + 1), len(claims), self._retrieved)
            print(msg)
        print()

    def _retrieve_tweets_from_claims_by_keywords(self, claims):
        num_of_claims = len(claims)
        for i, claim in enumerate(claims):
            keywords_str = claim.keywords
            keywords = keywords_str.split(",")
            retrieved_tweets_count = 0
            for keyword in keywords:
                tweets = self._retrieve_old_tweets(claim, keyword.lower().strip())
                retrieved_tweets_count += self._retrieved

            msg = "Processing claims {0}/{1}, Retreived {2} tweets".format(str(i + 1), num_of_claims, retrieved_tweets_count)
            print(msg)

    def _base_retrieve_tweets_from_claims(self, retrieve_tweets_function):
        claims = self._db.get_claims()
        self._claim_dict = {claim.claim_id: claim for claim in claims}
        retrieve_tweets_function(claims)

        self._db.addPosts(self._posts)
        self._db.insert_or_update_authors_from_posts(self._domain, {}, {})
        self._db.add_claim_connections(self._claim_post_connections)
        before_dict = self._claim_id_tweets_id_before_dict
        after_dict = self._claim_id_tweets_id_after_dict
        output_folder_path = self._output_folder_full_path
        self._export_csv_files_for_statistics(output_folder_path, before_dict, after_dict, self._claim_dict)

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
        original_claim_id = unicode(claim.claim_id)
        claim_verdict = claim.verdict

        posts_per_claim, claim_connections = self._convert_tweets_to_posts(tweets, original_claim_id, claim_verdict)
        self._posts += posts_per_claim
        self._claim_post_connections += claim_connections
        if len(self._posts) > self._max_num_of_objects_without_saving:
            self._db.addPosts(self._posts)
            self._db.insert_or_update_authors_from_posts(self._domain, {}, {})
            self._db.add_claim_connections(self._claim_post_connections)

            self._posts = []
            self._claim_post_connections = []

    def _convert_tweets_to_posts(self, tweets, original_claim_id, post_type):
        posts = []
        claim_post_connections = []
        seen_tweets = set()
        for tweet in tweets:
            if tweet.id not in seen_tweets:
                seen_tweets.add(tweet.id)
                post = self._convert_tweet_to_post(tweet, post_type)
                claim_post_connection = self._create_claim_post_connection(original_claim_id, post.post_id)

                claim_post_connections.append(claim_post_connection)
                posts.append(post)
        self._retrieved = len(seen_tweets)
        return posts, claim_post_connections

    def _convert_tweet_to_post(self, tweet, post_type):
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
        post.author_guid = compute_author_guid_by_author_name(author_name)
        post_url = tweet.permalink
        post.url = unicode(post_url)

        post_guid = compute_post_guid(post_url, author_name, created_at)
        post.guid = post_guid
        post.post_id = post_guid
        post.domain = self._domain

        post.post_type = post_type
        return post

    def _create_claim_post_connection(self, original_claim_id, post_id):
        claim_post_connection = Claim_Tweet_Connection()
        claim_post_connection.claim_id = original_claim_id
        claim_post_connection.post_id = post_id
        return claim_post_connection

    def _retrieve_old_tweets(self, claim, content):
        datetime_object = claim.verdict_date
        month_interval = timedelta(self._month_interval * 365 / 12)
        start_date = date_to_str(datetime_object - month_interval, "%Y-%m-%d")
        end_date = date_to_str(datetime_object + month_interval, "%Y-%m-%d")
        tweets = []
        try:
            tweets = self._retrieve_tweets_between_dates(claim, content, start_date, end_date)
        except:
            e = sys.exc_info()[0]
            print("tweet content: {0}, error:{1}".format(content, e))

        return tweets

    def _retrieve_tweets_between_dates(self, claim, content, start_date, current_date):
        original_claim_id = unicode(claim.claim_id)
        tweets = self._get_and_add_tweets_by_content_and_date(claim, content, start_date, current_date)
        for tweet in tweets:
            if tweet.date < claim.verdict_date:
                self._claim_id_tweets_id_before_dict[original_claim_id].add(tweet.id)
            else:
                self._claim_id_tweets_id_after_dict[original_claim_id].add(tweet.id)
        return tweets
    # content can be keyword or full content
