from __future__ import print_function

import urllib

from bs4 import BeautifulSoup
from commons.method_executor import Method_Executor
from preprocessing_tools.Topic_Term_Manager import Topic_Term_Manager
from generic_twitter_crawler import Generic_Twitter_Crawler
from twitter_rest_api.twitter_rest_api import Twitter_Rest_Api


class Twitter_Crawler(Method_Executor):
    def __init__(self, db):
        Method_Executor.__init__(self, db)
        # taken from http://techslides.com/hacking-the-google-trends-api
        self._url = "https://trends.google.com/trends/hottrends/atom/feed?pn=p1"
        self._retrieve_news_by_keywords = self._config_parser.eval(self.__class__.__name__, "retrieve_news_by_keywords")
        self._num_of_top_terms = self._config_parser.eval(self.__class__.__name__, "num_of_top_terms")
        self._generic_twitter_crawler = Generic_Twitter_Crawler(self._db)
        self._topic_term_manager = Topic_Term_Manager(db)
        self._twitter_rest_api = Twitter_Rest_Api(db)

    def get_most_popular_posts_by_google_trends(self):
        while True:
            terms = self._get_popular_terms_from_google_trends()
            self._generic_twitter_crawler.retrieve_and_save_data_from_twitter_by_terms(terms)

    def get_posts_by_config_keywords(self):
        terms = self._retrieve_news_by_keywords
        self._generic_twitter_crawler.retrieve_and_save_data_from_twitter_by_terms(terms)

    def get_posts_by_topic_with_most_posts(self):
        terms = self._topic_term_manager.get_term_from_db_with_most_posts()
        self._generic_twitter_crawler.retrieve_and_save_data_from_twitter_by_terms(terms)

    def _get_popular_terms_from_google_trends(self):
        html_doc = urllib.urlopen(self._url)
        soup = BeautifulSoup(html_doc, 'html.parser')
        popular_terms = soup.find_all('title')[1:]
        return [term.text for term in popular_terms]

    def get_posts_by_top_terms_in_each_topic(self):
        'The function returns Twitter posts by top ten terms in each topic. It requires to run Autotopic Executor prior to this'
        # topic = 1, terms = ['isis', 'belgium',...]
        topic_top_terms_dict = self._topic_term_manager.get_topic_top_terms_dictionary(self._num_of_top_terms)

        while True:
            for topic, top_terms in topic_top_terms_dict.iteritems():
                self._generic_twitter_crawler.retrieve_and_save_data_from_twitter_by_terms(top_terms)
