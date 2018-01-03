from commons import commons
from twitter_rest_api.twitter_rest_api import Twitter_Rest_Api
from configuration.config_class import getConfig

import logging
from twitter import TwitterError


class Generic_Twitter_Crawler(object):
    def __init__(self, db):
        # AbstractController.__init__(self, db)
        self._db = db
        self._twitter_rest_api = Twitter_Rest_Api(db)
        self._config_parser = getConfig()
        self._domain = unicode(self._config_parser.get("DEFAULT", "domain"))

    def retrieve_and_save_data_from_twitter_by_terms(self, terms):
        posts, total_twitter_users = self.get_posts_and_authors_by_terms(terms)
        self._db.addPosts(posts)
        self._add_users_to_db(total_twitter_users)

    def retrive_and_save_data_from_twitter_by_post_id(self, post_id, label):
        post, user = self.get_post_and_author_by_post_id(post_id)
        try:
            converted_post = self._db.create_post_from_tweet_data(post, self._domain)
            converted_post.post_type = label
            self._db.addPost(converted_post)
            self._add_users_to_db([user])
            self._db.commit()

        except TwitterError as e:
            exception_response = e[0][0]
            logging.info("e.massage =" + exception_response["message"])
            code = exception_response["code"]
            logging.info("e.code =" + str(exception_response["code"]))

            if code == 88:
                sec = self._twitter_rest_api.get_sleep_time_for_twitter_status_id()
                logging.info("Seconds to wait from catched crush is: " + str(sec))
                if sec != 0:
                    commons.count_down_time(sec)
                    self._num_of_twitter_status_id_requests = 0
                return self.retrive_and_save_data_from_twitter_by_post_id(post_id, label)

    def get_posts_and_authors_by_terms(self, terms):
        term_posts_dictionary = self.get_posts_by_terms(terms)
        total_twitter_users = []
        posts = []
        for term in term_posts_dictionary:
            posts += [self._db.create_post_from_tweet_data(tweet, self._domain) for tweet in term_posts_dictionary[term]]
            total_twitter_users += [post.user for post in term_posts_dictionary[term]]
        return posts, total_twitter_users

    def get_post_and_author_by_post_id(self, post_id):
        post = self._twitter_rest_api.get_post_by_post_id(post_id)
        user = post.user
        return post, user

    def _add_users_to_db(self, total_twitter_users):
        author_type = None
        insertion_type = None
        self._twitter_rest_api.save_authors_and_connections(total_twitter_users, author_type, insertion_type)

    def get_posts_by_terms(self, terms):
        return self._twitter_rest_api.get_posts_by_terms(terms)
