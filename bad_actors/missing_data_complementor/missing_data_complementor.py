#
# Created by Aviad on 03-Jun-16 11:41 AM.
#
from __future__ import print_function

import re
from collections import namedtuple

from twitter import TwitterError

from DB.schema_definition import Post, Author, Post_citation
from commons.commons import *
from commons.commons import get_current_time_as_string, cleanForAuthor
from commons.consts import *
from commons.method_executor import Method_Executor
from preprocessing_tools.abstract_executor import AbstractExecutor
from twitter_rest_api.twitter_rest_api import Twitter_Rest_Api

RetweetData = namedtuple('RetweetData', ['retweet_guid', 'retweet_url', 'tweet_guid', 'tweet_url', 'tweet_author_name',
                                         'tweet_author_guid',
                                         'tweet_date', 'tweet_content', 'tweet_twitter_id', 'tweet_retweet_count',
                                         'tweet_favorite_count'])


class MissingDataComplementor(Method_Executor):
    def __init__(self, db):
        AbstractExecutor.__init__(self, db)
        self._actions = self._config_parser.eval(self.__class__.__name__, "actions")

        self._minimal_num_of_posts = self._config_parser.eval(self.__class__.__name__, "minimal_num_of_posts")
        self._limit_friend_follower_number = self._config_parser.eval(self.__class__.__name__,
                                                                      "limit_friend_follower_number")
        self._maximal_tweets_count_in_timeline = self._config_parser.eval(self.__class__.__name__,
                                                                          "maximal_tweets_count_in_timeline")

        self._found_twitter_users = []
        self._social_network_crawler = Twitter_Rest_Api(db)
        self._suspended_authors = []
        self._max_users_without_saving = self._config_parser.eval(self.__class__.__name__, "max_users_without_saving")
        self._posts = []
        self._authors = []
        self._post_citatsions = []

    def setUp(self):
        pass

    def fill_data_for_followers(self):
        self._fill_data_for_author_connection_type(Author_Connection_Type.FOLLOWER)
        logging.info("---Finished crawl_followers_by_author_ids")

    def fill_data_for_friends(self):
        self._fill_data_for_author_connection_type(Author_Connection_Type.FRIEND)
        logging.info("---Finished crawl_friends_by_author_ids")

    def _fill_data_for_author_connection_type(self, connection_type):
        cursor = self._db.get_followers_or_friends_candidats(connection_type, self._domain,
                                                             self._limit_friend_follower_number)
        followers_or_friends_candidats = self._db.result_iter(cursor)
        followers_or_friends_candidats = [author_id[0] for author_id in followers_or_friends_candidats]
        print("---crawl_followers_by_author_ids---")
        author_type = None
        are_user_ids = True
        insertion_type = DB_Insertion_Type.MISSING_DATA_COMPLEMENTOR
        crawl_users_by_author_ids_func_name = "crawl_users_by_author_ids"
        getattr(self._social_network_crawler, crawl_users_by_author_ids_func_name)(followers_or_friends_candidats,
                                                                                   connection_type, author_type,
                                                                                   are_user_ids, insertion_type)
        self._db.convert_temp_author_connections_to_author_connections(self._domain)

    def crawl_followers_by_author_ids(self, author_ids):
        print("---crawl_followers_by_author_ids---")
        author_type = None
        are_user_ids = True
        inseration_type = DB_Insertion_Type.MISSING_DATA_COMPLEMENTOR
        self._social_network_crawler.crawl_followers_by_twitter_author_ids(author_ids, author_type, are_user_ids,
                                                                           inseration_type)

    def crawl_friends_by_author_ids(self, author_ids):
        print("---crawl_friends_by_author_ids---")
        author_type = None
        are_user_ids = True
        inseration_type = DB_Insertion_Type.MISSING_DATA_COMPLEMENTOR
        self._social_network_crawler.crawl_friends_by_twitter_author_ids(author_ids, author_type, are_user_ids,
                                                                         inseration_type)

    def create_author_screen_names(self):
        screen_names = self._db.get_screen_names_for_twitter_authors_by_posts()
        return screen_names

    def fill_data_for_sources(self):
        print("---complete_missing_information_for_authors_by_screen_names ---")
        logging.info("---complete_missing_information_for_authors_by_screen_names ---")
        # twitter_author_screen_names = self.create_author_screen_names()
        twitter_author_screen_names = self._db.get_missing_data_twitter_screen_names()
        # twitter_author_screen_names = (twitter_author.name for twitter_author in twitter_authors)
        # twitter_author_screen_names = list(twitter_author_screen_names)

        author_type = None
        are_user_ids = False
        inseration_type = DB_Insertion_Type.MISSING_DATA_COMPLEMENTOR
        # retrieve_full_data_for_missing_users
        total_twitter_users = self._social_network_crawler.handle_get_users_request(
            twitter_author_screen_names, are_user_ids, author_type, inseration_type)

        self._social_network_crawler.save_authors_and_connections(total_twitter_users, author_type, inseration_type)

        print("---complete_missing_information_for_authors_by_screen_names was completed!!!!---")
        logging.info("---complete_missing_information_for_authors_by_screen_names was completed!!!!---")
        return total_twitter_users

    def complete_missing_information_for_authors_by_ids(self):
        print("---complete_missing_information_for_authors_by_ids ---")
        logging.info("---complete_missing_information_for_authors_by_ids ---")
        # twitter_author_screen_names = self.create_author_screen_names()
        twitter_author_screen_names = self._db.get_missing_data_twitter_screen_names()
        # twitter_author_screen_names = (twitter_author.name for twitter_author in twitter_authors)
        # twitter_author_screen_names = list(twitter_author_screen_names)

        author_type = None
        are_user_ids = False
        inseration_type = DB_Insertion_Type.MISSING_DATA_COMPLEMENTOR
        # retrieve_full_data_for_missing_users
        total_twitter_users = self._social_network_crawler.handle_get_users_request(
            twitter_author_screen_names, are_user_ids, author_type, inseration_type)
        # return self._found_twitter_users
        print("---complete_missing_information_for_authors was completed!!!!---")
        logging.info("---complete_missing_information_for_authors was completed!!!!---")
        return total_twitter_users

    def mark_suspended_or_not_existed_authors(self):
        suspended_authors = self._db.get_authors_for_mark_as_suspended_or_not_existed()
        for suspended_author in suspended_authors:
            suspended_author.is_suspended_or_not_exists = self._window_start
            self._db.set_inseration_date(suspended_author, DB_Insertion_Type.MISSING_DATA_COMPLEMENTOR)
        self._social_network_crawler.save_authors(suspended_authors)

    def mark_suspended_from_twitter(self):
        self._suspended_authors = []
        suspected_authors = self._db.get_not_suspended_authors(self._domain)
        suspected_authors_names = [author.name for author in suspected_authors]
        chunks = split_into_equal_chunks(suspected_authors_names,
                                         self._social_network_crawler._maximal_user_ids_allowed_in_single_get_user_request)
        total_chunks = list(chunks)
        chunks = split_into_equal_chunks(suspected_authors_names,
                                         self._social_network_crawler._maximal_user_ids_allowed_in_single_get_user_request)
        i = 1
        for chunk_of_names in chunks:
            msg = "\rChunck of author to Twitter: [{0}/{1}]".format(i, len(total_chunks))
            print(msg, end="")
            i += 1
            set_of_send_author_names = set(chunk_of_names)
            set_of_received_author_names = set(
                self._social_network_crawler.get_active_users_names_by_screen_names(chunk_of_names))
            author_names_of_suspendend_or_not_exists = set_of_send_author_names - set_of_received_author_names
            self._update_suspended_authors_by_screen_names(author_names_of_suspendend_or_not_exists)
        self._db.add_authors(self._suspended_authors)

    def _update_suspended_authors_by_screen_names(self, author_names_of_suspendend_or_not_exists):
        for author_name in author_names_of_suspendend_or_not_exists:
            user_guid = compute_author_guid_by_author_name(author_name).replace("-", "")
            suspended_author = self._db.get_author_by_author_guid(user_guid)[0]

            suspended_author.is_suspended_or_not_exists = self._window_start
            suspended_author.author_type = Author_Type.BAD_ACTOR
            self._db.set_inseration_date(suspended_author, DB_Insertion_Type.MISSING_DATA_COMPLEMENTOR)
            self._suspended_authors.append(suspended_author)

            num_of_suspended_authors = len(self._suspended_authors)
            if num_of_suspended_authors == self._max_users_without_saving:
                self._db.add_authors(self._suspended_authors)
                self._suspended_authors = []

    def fill_tweet_retweet_connection(self):
        '''
        Fetches the original tweets being retweeted by our posts.
        Updates the followig tables:
         * Post_Citations table with tweet-retweet connection
         * Posts table with missing tweets
         * Authors with the authors of the missing tweets
        '''
        retweets_with_no_tweet_citation = self._db.get_retweets_with_no_tweet_citation()
        logging.info("Updating tweet-retweet connection of {0} retweets".format(len(retweets_with_no_tweet_citation)))
        self._posts = []
        self._authors = []
        self._post_citatsions = []
        i = 1
        for post_guid, post_url in retweets_with_no_tweet_citation.iteritems():
            # logging.info("Analyzing retweet: {0} - {1}".format(post_guid, post_url))
            msg = "\r Analyzing retweet: {0} - {1} [{2}".format(post_guid, post_url, i) + "/" + str(
                len(retweets_with_no_tweet_citation)) + '] '
            print(msg, end="")
            i += 1
            tweet_data = self.extract_retweet_data(retweet_guid=post_guid, retweet_url=post_url)
            if tweet_data is not None:

                if not self._db.isPostExist(tweet_data.tweet_url):
                    post = Post(guid=tweet_data.tweet_guid, post_id=tweet_data.tweet_guid, url=tweet_data.tweet_url,
                                date=str_to_date(tweet_data.tweet_date),
                                title=tweet_data.tweet_content, content=tweet_data.tweet_content,
                                post_osn_id=tweet_data.tweet_twitter_id,
                                retweet_count=tweet_data.tweet_retweet_count,
                                favorite_count=tweet_data.tweet_favorite_count,
                                author=tweet_data.tweet_author_name, author_guid=tweet_data.tweet_author_guid,
                                domain=self._domain,
                                original_tweet_importer_insertion_date=unicode(get_current_time_as_string()))
                    self._posts.append(post)

                if not self._db.is_author_exists(tweet_data.tweet_author_guid, self._domain):
                    author = Author(name=tweet_data.tweet_author_name,
                                    domain=self._domain,
                                    author_guid=tweet_data.tweet_author_guid,
                                    original_tweet_importer_insertion_date=unicode(get_current_time_as_string()))
                    self._authors.append(author)

                if not self._db.is_post_citation_exist(tweet_data.retweet_guid, tweet_data.tweet_guid):
                    post_citation = Post_citation(post_id_from=tweet_data.retweet_guid,
                                                  post_id_to=tweet_data.tweet_guid, url_from=tweet_data.retweet_url,
                                                  url_to=tweet_data.tweet_url)
                    self._post_citatsions.append(post_citation)

        self.update_tables_with_tweet_retweet_data(self._posts, self._authors, self._post_citatsions)

    def extract_retweet_data(self, retweet_guid, retweet_url):
        '''
        :param retweet_guid: the guid of the retweet
        :param retweet_url: the url of the retweet
        :return: a RetweetData holding the data of the retweet
        '''
        try:
            retweet_id = self.extract_tweet_id(retweet_url)
            if retweet_id is None:
                return None

            retweet_status = self._social_network_crawler.get_status_by_twitter_status_id(retweet_id)
            tweet_status_dict = retweet_status.AsDict()
            if 'retweeted_status' in tweet_status_dict:
                tweet_status_dict = tweet_status_dict['retweeted_status']
                tweet_post_twitter_id = unicode(str(tweet_status_dict['id']))
                tweet_author_name = unicode(tweet_status_dict['user']['screen_name'])
                tweet_url = unicode(generate_tweet_url(tweet_post_twitter_id, tweet_author_name))
                tweet_creation_time = unicode(tweet_status_dict['created_at'])
                tweet_str_publication_date = unicode(extract_tweet_publiction_date(tweet_creation_time))
                tweet_guid = unicode(compute_post_guid(post_url=tweet_url, author_name=tweet_author_name,
                                                       str_publication_date=tweet_str_publication_date))
                tweet_author_guid = unicode(compute_author_guid_by_author_name(tweet_author_name))
                tweet_author_guid = unicode(tweet_author_guid.replace("-", ""))
                tweet_content = unicode(tweet_status_dict['text'])
                tweet_retweet_count = unicode(tweet_status_dict['retweet_count'])
                tweet_favorite_count = unicode(tweet_status_dict['favorite_count'])

                retweet_data = RetweetData(retweet_guid=retweet_guid, retweet_url=retweet_url, tweet_guid=tweet_guid,
                                           tweet_url=tweet_url, tweet_author_name=tweet_author_name,
                                           tweet_author_guid=tweet_author_guid,
                                           tweet_date=tweet_str_publication_date, tweet_content=tweet_content,
                                           tweet_twitter_id=tweet_post_twitter_id,
                                           tweet_retweet_count=tweet_retweet_count,
                                           tweet_favorite_count=tweet_favorite_count)
                return retweet_data
            else:
                return None

        except TwitterError as e:
            exception_response = e[0][0]
            logging.info("e.massage =" + exception_response["message"])
            code = exception_response["code"]
            logging.info("e.code =" + str(exception_response["code"]))

            self.update_tables_with_tweet_retweet_data(self._posts, self._authors, self._post_citatsions)
            self._posts = []
            self._authors = []
            self._post_citatsions = []

            if code == 88:
                sec = self._social_network_crawler.get_sleep_time_for_twitter_status_id()
                logging.info("Seconds to wait from catched crush is: " + str(sec))
                if sec != 0:
                    count_down_time(sec)
                    self._num_of_twitter_status_id_requests = 0
                return self._social_network_crawler.get_status(retweet_id)

        except Exception as e:
            logging.error("Cannot fetch data for retweet: {0}. Error message: {1}".format(retweet_url, e.message))
            return None

    def extract_tweet_id(self, post_url):
        pattern = re.compile("http(.*)://twitter.com/(.*)/statuses/(.*)")
        extracted_info = pattern.findall(post_url)
        if extracted_info == []:
            pattern = re.compile("http(.*)://twitter.com/(.*)/status/(.*)")
            extracted_info = pattern.findall(post_url)
        if len(extracted_info[0]) < 2:
            return None
        else:
            return extracted_info[0][2]

    def update_tables_with_tweet_retweet_data(self, posts, authors, post_citatsions):
        self._db.addPosts(posts)
        self._db.add_authors(authors)
        self._db.addReferences(post_citatsions)

    def fill_authors_time_line(self):
        '''
        Fetches the posts for the authors that are given under authors_twitter_ids_for_timeline_filling in the config file +
        update the db
        '''
        self._db.create_authors_index()
        self._db.create_posts_index()
        author_screen_names_number_of_posts = self._db.get_author_screen_names_and_number_of_posts(
            self._minimal_num_of_posts)
        author_screen_names_number_of_posts_dict = self._create_author_screen_name_number_of_posts_dictionary(
            author_screen_names_number_of_posts)
        index = 1
        for author_name in author_screen_names_number_of_posts_dict:
            print("Get timeline for {0} : {1}/{2}".format(author_name, str(index),
                                                          str(len(author_screen_names_number_of_posts_dict))))
            index += 1
            posts = []
            logging.info("Fetching timeline for author: " + str(author_name))
            posts_counter = 0
            try:
                posts_needed_from_osn = self._minimal_num_of_posts - author_screen_names_number_of_posts_dict[
                    author_name]
                timeline = self._social_network_crawler.get_timeline_by_author_name(author_name, posts_needed_from_osn)
                # logging.info("Retrived timeline lenght: " + str(len(timeline)))
                if timeline is not None:
                    for post in timeline:
                        tweet_post_twitter_id = str(post.id)
                        tweet_url = generate_tweet_url(tweet_post_twitter_id, author_name)
                        tweet_creation_time = post.created_at
                        tweet_str_publication_date = extract_tweet_publiction_date(tweet_creation_time)
                        tweet_guid = compute_post_guid(post_url=tweet_url, author_name=author_name,
                                                       str_publication_date=tweet_str_publication_date)
                        if self._db.contains_post(tweet_url):
                            continue
                        posts_counter = posts_counter + 1
                        tweet_author_guid = compute_author_guid_by_author_name(author_name)
                        tweet_author_guid = cleanForAuthor(tweet_author_guid)
                        tweet_content = post.text
                        post = self._db.create_post_from_tweet_data(post, self._domain)
                        posts.append(post)
            except Exception as e:
                logging.error("Cannot fetch data for author: {0}. Error message: {1}".format(author_name, e.message))
            logging.info("Number of posts inserted for author {0}: {1}".format(author_name, posts_counter))
            self._db.addPosts(posts)

    def assign_manually_labeled_authors(self):
        self._db.assign_manually_labeled_authors()

    def delete_acquired_authors(self):
        self._db.delete_acquired_authors()
        self._db.delete_posts_with_missing_authors()

    def delete_manually_labeled_authors(self):
        self._db.delete_manually_labeled_authors()
        self._db.delete_posts_with_missing_authors()

    def assign_acquired_and_crowd_turfer_profiles(self):
        self._db.assign_crowdturfer_profiles()
        self._db.assign_acquired_profiles()

    def _create_author_screen_name_number_of_posts_dictionary(self, author_screen_names_number_of_posts):
        author_screen_names_number_of_posts_dict = {}
        for record in author_screen_names_number_of_posts:
            author_screen_name = record[0]
            num_of_posts = record[1]
            author_screen_names_number_of_posts_dict[author_screen_name] = num_of_posts
        logging.info("Number of users to retrieve timelines: " + str(len(author_screen_names_number_of_posts_dict)))
        return author_screen_names_number_of_posts_dict
