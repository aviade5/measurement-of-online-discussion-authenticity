# Created by aviade
# Time: 29/03/2016 16:53
from configuration.config_class import getConfig
# from vendors.twitter import Api
from commons.commons import *
import twitter


class TwitterApiRequester:
    def __init__(self):
        self._config_parser = getConfig()
        self._consumer_key = self._config_parser.eval("TwitterApiRequester", 'consumer_key')
        self._consumer_secret = self._config_parser.eval("TwitterApiRequester", 'consumer_secret')
        self._access_token_key = self._config_parser.eval("TwitterApiRequester", 'access_token_key')
        self._access_token_secret = self._config_parser.eval("TwitterApiRequester", 'access_token_secret')
        self._user_id_1 = self._config_parser.get("TwitterApiRequester", 'access_token_secret')
        self._screen_name_1 = self._config_parser.eval("TwitterApiRequester", 'screen_name')

        self.create_twitter_api(self._consumer_key, self._consumer_secret, self._access_token_key, self._access_token_secret)



    def create_twitter_api(self, consumer_key, consumer_secret, access_token_key, access_token_secret):
        try:
            self.api = twitter.Api(consumer_key=consumer_key,
                                   consumer_secret=consumer_secret,
                                   access_token_key=access_token_key,
                                   access_token_secret=access_token_secret,
                                   sleep_on_rate_limit=True)

            print("The twitter.Api object created")

        except Exception as e:
            raise Exception("Could not connect to twitter, please check that the user details in the config.ini file are correct")



    def verify_credentials(self):
        logging.info("----- api.VerifyCredentials() -------")
        print("----- api.VerifyCredentials() -------")

        authenticated_user = self.api.VerifyCredentials()

        logging.info("The authenticated user is: " + authenticated_user.screen_name)
        print("The authenticated user is: " + authenticated_user.screen_name)
        logging.info(str(authenticated_user))
        print(str(authenticated_user))

        return authenticated_user

    def get_authenticated_user_id(self):
        return self.authenticated_user.id

    def get_timeline_by_user_id(self, user_id):
        logging.info("get_timeline_by_user_id for user_id: " + str(user_id))
        statuses = self.api.GetUserTimeline(user_id=user_id, count=300)
        logging.info("Number of retrieved statuses is: " + str(len(statuses)))
        #self.print_list(statuses)
        return statuses

    def get_timeline_by_screen_name(self, screen_name):
        statuses = self.api.GetUserTimeline(None, screen_name)
        self.print_list(statuses)

    def print_list(self, items):
        list_for_print = "[" + "".join([str(item) + "," for item in items])
        list_for_print = list_for_print[:-1] # remove the last unnecessary last ","
        print list_for_print + "]"

    def get_friends(self):
        friends = self.api.GetFriends()
        self.print_list(friends)
        return friends
        #print([user.name for user in users])

    def post_status_message(self, message):
        status = self.api.PostUpdate(message)
        print(status)
        return status

    def get_followers(self, user_id):
        followers = self.api.GetFollowers(user_id)
        self.print_list(followers)
        return followers
        #print "".join([str(follower) for follower in followers])

    def get_follower_ids_by_user_id(self, user_id):
        print('--- get_follower_ids_by_user_id: ' + str(user_id))

        follower_ids = self.api.GetFollowerIDs(user_id=user_id)

        print("Number of retrieved followers ids is: " + str(len(follower_ids)))
        return follower_ids

    def get_friend_ids_by_user_id(self, user_id):
        friend_ids = self.api.GetFriendIDs(user_id=user_id)
        print(friend_ids)
        print("Number of retrieved friends ids is: " + str(len(friend_ids)))
        return friend_ids


    def get_follower_ids_by_screen_name(self, screen_name):
        follower_ids = self.api.GetFollowerIDs(screen_name=screen_name)
        print("Number of followers ids is :" + str(len(follower_ids)))
        print(follower_ids)
        return follower_ids


    def get_retweeter_ids_by_status_id(self, status_id):
        retweeters = self.api.GetRetweeters(status_id)
        print_list_ids(retweeters)
        length = len(retweeters)
        print("Number of retweeters :" + str(length))
        return retweeters

    def get_retweets_by_status_id(self, status_id):
        retweets = self.api.GetRetweets(status_id)
        self.print_list(retweets)
        return retweets

    def get_retweets_of_me(self):
        my_retweeters = self.api.GetRetweetsOfMe()
        self.print_list(my_retweeters)
        return my_retweeters

    def get_status(self, id):
        status = self.api.GetStatus(id)
        print(status)
        return status

    def get_timeline(self, author_name, maximal_tweets_count_in_timeline):
        timeline = self.api.GetUserTimeline(screen_name=author_name, count=maximal_tweets_count_in_timeline)
        return timeline

    def get_favorites(self):
        self.authenticated_user = self.verify_credentials()
        favorites = self.api.GetFavorites(self.authenticated_user.id)
        self.print_list(favorites)
        return favorites

    def get_user_retweets(self, user_id):
        self.api.GetUserRetweets()

    def get_user_search_by_term(self, term):
        print("------Get user search by term: " + term)
        users_search = self.api.GetUsersSearch(term)
        self.print_list(users_search)
        return users_search

    def get_tweets_by_term(self, term, result_type):
        print("------get_tweets_by_term: " + term)
        tweets = self.api.GetSearch(term, count=100, result_type=result_type)
        self.print_list(tweets)
        return tweets

    def get_tweet_by_post_id(self, post_id):
        return self.get_tweets_by_post_ids([post_id])[0]

    def get_tweets_by_post_ids(self, post_ids):
        return self.api.GetStatuses(post_ids)

    def get_user_by_screen_name(self, screen_name):
        print("------Get user by screen name: " + screen_name)
        twitter_user = self.api.GetUser(None, screen_name)
        print(twitter_user)
        return twitter_user

    def get_user_by_user_id(self, user_id):
        print("---------get_user_by_user_id------------")
        twitter_user = self.api.GetUser(user_id)
        print(twitter_user)
        return twitter_user

    def get_users_by_ids(self, ids):
        print("---------get_users_by_ids------------")
        users = self.api.UsersLookup(ids)
        print("Num of retrieved twitter users is: " + str(len(users)))
        logging.info("Num of retrieved twitter users is: " + str(len(users)))
        return users

    def get_users_by_screen_names(self, screen_names):
        print("---------get_users_by_screen_names------------")
        users = self.api.UsersLookup(screen_name=screen_names)
        print("Num of retrieved twitter users is: " + str(len(users)))
        logging.info("Num of retrieved twitter users is: " + str(len(users)))
        return users

    def get_sleep_time_for_get_users_request(self):
        print("---GetSleepTime /users/lookup ---")
        logging.info("---GetSleepTime /users/lookup ---")

        seconds_to_wait_object = self.api.CheckRateLimit('/users/lookup')
        seconds_to_wait = self.get_seconds_to_wait(seconds_to_wait_object)
        print("Seconds to wait for CheckRateLimit('/users/lookup') is: " + str(seconds_to_wait))
        return seconds_to_wait

    def get_sleep_time_for_get_tweets_by_tweet_ids_request(self):
        print("---GetSleepTime /statuses/lookup ---")
        logging.info("---GetSleepTime statuses/lookup ---")

        seconds_to_wait_object = self.api.CheckRateLimit('/statuses/lookup')
        seconds_to_wait = self.get_seconds_to_wait(seconds_to_wait_object)
        print("Seconds to wait for GetSleepTime('/statuses/lookup') is: " + str(seconds_to_wait))
        return seconds_to_wait

    def get_sleep_time_for_timeline(self):
        logging.info("---GetSleepTime /statuses/user_timeline ---")

        seconds_to_wait_object = self.api.CheckRateLimit('/statuses/user_timeline')
        seconds_to_wait = self.get_seconds_to_wait(seconds_to_wait_object)
        logging.info("Seconds to wait for GetSleepTime('/statuses/user_timeline') is: " + str(seconds_to_wait))
        return seconds_to_wait


    def get_sleep_time_for_get_follower_ids_request(self):
        print("---GetSleepTime /followers/ids ---")
        logging.info("---GetSleepTime /followers/ids ---")

        seconds_to_wait_object = self.api.CheckRateLimit('/statuses/retweeters/ids')
        seconds_to_wait = self.get_seconds_to_wait(seconds_to_wait_object)
        print("Seconds to wait are: " + str(seconds_to_wait))
        logging.info("Seconds to wait for GetSleepTime('/followers/ids') are: " + str(seconds_to_wait))
        return seconds_to_wait

    def get_seconds_to_wait(self, seconds_to_wait_object):
        if seconds_to_wait_object.remaining > 0:
            seconds_to_wait = 0
        else:
            epoch_timestamp = seconds_to_wait_object.reset
            current_timestamp = time.time()
            seconds_to_wait = int(epoch_timestamp - current_timestamp + 5)
        return seconds_to_wait

    def get_sleep_time_for_get_friend_ids_request(self):
        print("---GetSleepTime /friends/ids ---")
        logging.info("---GetSleepTime /friends/ids ---")

        seconds_to_wait_object = self.api.CheckRateLimit('/users/lookup')
        seconds_to_wait = self.get_seconds_to_wait(seconds_to_wait_object)
        print("Seconds to wait are: " + str(seconds_to_wait))
        logging.info("Seconds to wait for GetSleepTime('/friends/ids') are: " + str(seconds_to_wait))
        return seconds_to_wait

    def get_sleep_time_for_get_retweeter_ids_request(self):
        seconds_to_wait_object = self.api.CheckRateLimit('/statuses/retweeters/ids')
        seconds_to_wait = self.get_seconds_to_wait(seconds_to_wait_object)
        print("Seconds to wait are: " + str(seconds_to_wait))
        return seconds_to_wait

    def get_sleep_time_for_twitter_status_id(self):
        seconds_to_wait_object = self.api.CheckRateLimit('/statuses/show/:id')
        seconds_to_wait = self.get_seconds_to_wait(seconds_to_wait_object)
        print("Seconds to wait are: " + str(seconds_to_wait))
        return seconds_to_wait

    def get_sleep_time_for_twitter_timeline_request(self):
        seconds_to_wait_object = self.api.CheckRateLimit('/statuses/user_timeline')
        seconds_to_wait = self.get_seconds_to_wait(seconds_to_wait_object)
        print("Seconds to wait are: " + str(seconds_to_wait))
        return seconds_to_wait

    def init_num_of_get_follower_ids_requests(self):
        self.api.num_of_get_follower_ids_requests = 0
        print("Number of GetFollowerIds requests is: " + str(self.api.num_of_get_follower_ids_requests))

    def init_num_of_get_friend_ids_requests(self):
        self.api._num_of_get_friend_ids_requests= 0
        print("Number of GetFriendIds requests is: " + str(self.api._num_of_get_friend_ids_requests))

    def init_num_of_get_retweeter_ids_requests(self):
        self.api.num_of_get_retweeter_ids_requests = 0
        print("Number of GetFollowerIds requests is: " + str(self.api.num_of_get_retweeter_ids_requests))

    def get_num_of_rate_limit_status_requests(self):
        return self.api.get_num_of_rate_limit_status()

    def get_num_of_get_users_requests(self):
        return self.api.get_num_of_get_users_requests()

    def init_num_of_get_users_requests(self):
        self.api.init_num_of_get_users_requests()