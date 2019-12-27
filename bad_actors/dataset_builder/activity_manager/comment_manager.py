from __future__ import print_function
import tweepy
from collections import defaultdict
from datetime import timedelta
from datetime import datetime
from Twitter_API.twitter_api_requester import TwitterApiRequester
from commons.method_executor import Method_Executor


class CommentManager(Method_Executor):
    def __init__(self, db):
        Method_Executor.__init__(self, db)
        self._twitter_api = TwitterApiRequester()

    def _connect_tweepy(self):
        consumer_key = "ylABfKwfNvpadI2wtlGnVkJ2X"
        consumer_secret = "XzrilSALYRebVOq4uO8ynN4K9PyGAK8MgwRUEK7LNeYEBSb0lm"
        access_token_key = "833580558747856897-EsfVQvCZKebd2s3Foyc9OVSQLlQJwEE"
        access_token_secret = "HiOPUEwViN5yFWUJXR640HBQ99AlzaFFsc6UURPRs1AMd"

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token_key, access_token_secret)

        api = tweepy.API(auth, wait_on_rate_limit=True)
        name = "LFC"
        tweets = tweepy.Cursor(api.search, q='to:' + name, result_type='recent', timeout=999999).items(10)
        toreturn = defaultdict(list)
        for i in tweets:
            toreturn[i.in_reply_to_status_id_str].append(i)
        return toreturn

    def _get_replies_by_tweepy(self, tweet_data):

        tweet_id = tweet_data.post_osn_id

        replies = []
        for tweet in tweepy.Cursor(api.search, q='to:' + name, result_type='mixed', timeout=999999).items(10):
            if hasattr(tweet, 'in_reply_to_status_id_str'):
                if (tweet.in_reply_to_status_id_str == tweet_id):
                    replies.append(tweet)

        return replies

    def _get_replies_by_api(self, tweet):

        user = tweet.user.screen_name
        tweet_id = tweet.id

        max_id = None

        while True:

            try:
                q = "q=to%3A" + user + "&since_id=" + str(tweet_id) + "&max_id=" + str(max_id) + "&count=100"
                replies = self._twitter_api.api.GetSearch(raw_query=q, since_id=tweet_id, max_id=max_id, count=100)
            except Exception as e:
                print("failed")
                break

            for reply in replies:

                if reply.in_reply_to_status_id == tweet_id:

                    yield reply
                    # recursive magic to also get the replies to this reply

                    for reply_to_reply in self._get_replies_by_api(reply):
                        yield reply_to_reply
                max_id = reply.id

            if len(replies) != 100:
                break

    def applying_replies_crawling(self):
        all_posts = self._db.get_all_posts()
        posts = []
        for post in all_posts:
            past = post.date + timedelta(days=7)
            if past > datetime.now():
                posts.append(post)
        self._insert_replies(posts)

    def _insert_replies_to_db(self, reply,tweet_data):
        connections = []
        rec = self._db.create_post_from_tweet_data(reply, "comment")
        author_conn = self._db.create_connections(tweet_data.post_id, rec.post_id, "post_comment_connection")
        connections.append(author_conn)
        self._db.addPost(rec)
        self._db.save_author_connections(connections)

    def _insert_replies(self, posts):

        posts_and_replies_from_tweepy = self._connect_tweepy()
        keys = posts_and_replies_from_tweepy.keys()


        for tweet_data in posts:


            print("start working on post:" + str(tweet_data.post_osn_id))
            if tweet_data.post_osn_id in keys:
                replies_from_tweepy = posts_and_replies_from_tweepy[tweet_data.post_osn_id]
                for reply in replies_from_tweepy:
                    self._insert_replies_to_db(reply,tweet_data)

            try:
                tweet = self._twitter_api.get_tweet_by_post_id(tweet_data.post_osn_id)
            except Exception as e:
                print("failed")
                continue

            for reply in self._get_replies_by_api(tweet):
                self._insert_replies_to_db(reply,tweet_data)



        p = 0
