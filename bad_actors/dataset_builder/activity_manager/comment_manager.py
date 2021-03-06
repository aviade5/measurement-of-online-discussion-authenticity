from __future__ import print_function

import urllib
import time
import tweepy
from collections import defaultdict
from datetime import timedelta
from datetime import datetime

from commons.method_executor import Method_Executor


class CommentManager(Method_Executor):
    def __init__(self, db):
        Method_Executor.__init__(self, db)

    def _connect_tweepy(self):
        consumer_key = "ylABfKwfNvpadI2wtlGnVkJ2X"
        consumer_secret = "XzrilSALYRebVOq4uO8ynN4K9PyGAK8MgwRUEK7LNeYEBSb0lm"
        access_token_key = "833580558747856897-EsfVQvCZKebd2s3Foyc9OVSQLlQJwEE"
        access_token_secret = "HiOPUEwViN5yFWUJXR640HBQ99AlzaFFsc6UURPRs1AMd"

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token_key, access_token_secret)

        api = tweepy.API(auth, wait_on_rate_limit=True)
        self.api = api
        name = "Chelsea_HQ"
        tweets = tweepy.Cursor(api.search, q='to:' + name, result_type='recent', timeout=999999).items(10000)
        toreturn = defaultdict(list)
        for i in tweets:
            toreturn[i.in_reply_to_status_id_str].append(i)
        return toreturn

    def get_users_details(self):
        ids = self._db.get_authors_comments()
        total_twitter_users = []
        # prevent duplicates:
        ids = list(set(ids))
        for id in ids:
            try:
                user = api.get_user(user_id=id)
                total_twitter_users.append(user)
            except:
                print("An exception occurred")
                time.sleep(60)
        self._db.convert_twitter_users_to_authors_and_save(total_twitter_users, "", "")
        i = 9

    def _get_replies_by_tweepy(self, tweet_data):

        tweet_id = tweet_data.post_osn_id

        replies = []
        for tweet in tweepy.Cursor(api.search, q='to:' + name, result_type='mixed', timeout=999999).items(10000):
            if hasattr(tweet, 'in_reply_to_status_id_str'):
                if (tweet.in_reply_to_status_id_str == tweet_id):
                    replies.append(tweet)

        return replies

    def _get_replies_by_api(self, tweet):
        replies_results = []
        user = tweet.user.screen_name
        tweet_id = tweet.id

        replies = tweepy.Cursor(self.api.search, q='to:{}'.format(user),
                                since_id=tweet_id, tweet_mode='extended').items()
        while True:
            try:
                reply = replies.next()
                if not hasattr(reply, 'in_reply_to_status_id_str'):
                    continue
                if reply.in_reply_to_status_id == tweet_id:
                    replies_results.append(reply)
            except tweepy.RateLimitError as e:
                print("Twitter api rate limit reached".format(e))
                time.sleep(60)
                continue

            except tweepy.TweepError as e:
                print("Tweepy error occured:{}".format(e))
                break

            except StopIteration:
                break

            except Exception as e:
                print("Failed while fetching replies {}".format(e))
                break

        return replies_results

    def _insert_replies_by_post_ids(self, posts):
        posts_ids = map(lambda x: x.post_osn_id, posts)
        users = set(map(lambda x: x.author, posts))
        since_id = min(posts_ids)
        count = 0
        for user in users:
            count += 1
            print(count)
            replies = tweepy.Cursor(self.api.search, q='to:{}'.format(user),
                                    since_id=since_id, tweet_mode='extended').items()
            while True:
                try:
                    reply = replies.next()
                    if not hasattr(reply, 'in_reply_to_status_id_str'):
                        continue
                    if reply.in_reply_to_status_id in posts_ids:
                        tweet_data = None
                        for item in posts:
                            try:
                                if item.post_osn_id == reply.in_reply_to_status_id:
                                    tweet_data = item
                                    break
                            except Exception as e:
                                pass
                        # tweet_data = next(obj for obj in posts if obj.post_osn_id == reply.in_reply_to_status_id)
                        reply.text = str(tweet_data.title)
                        # self._insert_replies_to_db
                        yield {"reply": reply, "tweet_data": tweet_data}
                except tweepy.RateLimitError as e:
                    print("Twitter api rate limit reached".format(e))
                    time.sleep(60)
                    continue

                except tweepy.TweepError as e:
                    print("Tweepy error occured:{}".format(e))
                    break

                except StopIteration:
                    break

                except Exception as e:
                    print("Failed while fetching replies {}".format(e))
                    import traceback
                    traceback.print_exc()
                    break

    def applying_replies_crawling(self):
        all_posts = self._db.get_all_posts()
        posts = []
        for post in all_posts:
            past = post.date + timedelta(days=7)
            if past > datetime.now()and post.author=="LFCUSA":
                posts.append(post)
        self._insert_replies(posts)

    def _insert_replies_to_db(self, reply, tweet_data):
        connections = []
        user_id = reply.author.id
        if self._db.is_author_exist(user_id) == False:
            self._db.convert_twitter_users_to_authors_and_save(reply.author, "", "")
        rec = self._db.create_post_from_tweet_data(reply, "comment")
        author_conn = self._db.create_connections(tweet_data.post_id, rec.post_id, "post_comment_connection")
        connections.append(author_conn)
        self._db.addPost(rec)
        self._db.save_author_connections(connections)



    def _insert_replies(self, posts):

        posts_and_replies_from_tweepy = self._connect_tweepy()
        keys = posts_and_replies_from_tweepy.keys()

        for data in self._insert_replies_by_post_ids(posts):
            self._insert_replies_to_db(data["reply"], data["tweet_data"])

        for index, tweet_data in enumerate(posts):
            if (index % 200 == 0):
                print("finished:" + str(index) + "out of" + str(len(posts)) + str(index))

            print("start working on post:" + str(tweet_data.post_osn_id))
            if tweet_data.post_osn_id in keys:
                replies_from_tweepy = posts_and_replies_from_tweepy[tweet_data.post_osn_id]
                for reply in replies_from_tweepy:
                    self._insert_replies_to_db(reply, tweet_data)

            try:
                tweet = self._twitter_api.get_tweet_by_post_id(tweet_data.post_osn_id)
            except Exception as e:
                print("failed")
                continue


        p = 0
