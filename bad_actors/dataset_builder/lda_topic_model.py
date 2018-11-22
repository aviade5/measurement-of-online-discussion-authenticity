from __future__ import print_function

from preprocessing_tools.abstract_executor import AbstractExecutor

try:
    from gensim import *

    lda_model = models.ldamodel
except:
    print("WARNING! gensim is not available! This module is not usable.")

from operator import itemgetter

from DB.schema_definition import *
import logging


class LDATopicModel(AbstractExecutor):
    """Imports topics from files."""

    def __init__(self, db):

        AbstractExecutor.__init__(self, db)

        self._num_of_terms_in_topic = self._config_parser.eval(self.__class__.__name__, "num_of_terms_in_topic")
        self.num_topics = self._config_parser.eval(self.__class__.__name__, "number_of_topics")
        self.stopword_file = self._config_parser.get(self.__class__.__name__, "stopword_file")
        self.stemlanguage = self._config_parser.get(self.__class__.__name__, "stem_language")
        self.topics = []
        self.topic = None
        self.post_id_to_topic_id = None
        self.topic_id_to_topics = {}
        self.model = None
        self.dictionary = None
        self.corpus = None

    def execute(self, window_start):
        logging.info("LDATopicModel execute window_start %s" % self._window_start)
        # self._db.deleteTopics(None)
        self.cleanUp()
        curr_posts = self._db.getPostsListWithoutEmptyRowsByDomain(self._domain)

        if len(curr_posts) > 0:
            post_id_to_words = self._create_post_id_to_content_words(curr_posts)
            self.calculate_topics(post_id_to_words)

    def _create_post_id_to_content_words(self, curr_posts):
        post_id_to_content = {post[0]: post[6] for post in curr_posts}
        post_id_to_ngrams = {}
        for doc_id, content in post_id_to_content.iteritems():
            if content is not None:
                content = clean_tweet(content)
                words = clean_content_by_nltk_stopwords(content, self.stemlanguage)
                words = stem_content_using_stemmer(self.stemlanguage, words)
                post_id_to_ngrams[doc_id] = calc_ngrams(words, 1, 1)
        return post_id_to_ngrams

    def cleanUp(self, window_start=None):
        self._db.deleteTopics(None)

    def calculate_topics(self, post_id_to_words):
        words = post_id_to_words.values()
        self.dictionary = corpora.Dictionary(words)
        self.corpus = [self.dictionary.doc2bow(content_words) for content_words in words]
        self.model = lda_model.LdaModel(self.corpus, num_topics=self.num_topics)
        self.topic_id_to_topics = {}
        self.topics = []
        for topic_id in xrange(self.model.num_topics):
            self.add_to_db_topic_object(topic_id)

        # Create PostTopicMapping table
        post_to_topic_id = {}
        post_topic_mappings = []
        for post_id in post_id_to_words:
            content_words = post_id_to_words[post_id]
            bow = self.dictionary.doc2bow(content_words)
            topic_id_to_probability = self.model.get_document_topics(bow)
            post_to_topic_id[post_id] = {probability[0]: probability[1] for probability in topic_id_to_probability}
            max_topic_probability = max(topic_id_to_probability, key=lambda item: item[1])
            ptm = self._db.create_post_topic_mapping_obj(max_topic_probability, post_id)
            post_topic_mappings.append(ptm)
        self._db.addPostTopicMappings(post_topic_mappings)
        self._db.delete_author_topic_mapping()
        self.create_author_topic_mapping_table(post_to_topic_id)

    def create_author_topic_mapping_table(self, post_to_topic_id):
        authors = self._db.get_authors_by_domain(self._domain)
        self._db.create_author_topic_mapping_table(self.num_topics)
        author_guid_posts_dict = self._db.get_author_guid_post_dict()
        author_topic_mapping_items = []
        for i, author in enumerate(authors):
            if i % 1000 == 0 or i >= len(authors) - 1:
                msg = "\rGenerate author_topic_mapping {0}/{1}".format(str(i + 1), str(len(authors)))
                print(msg, end="")
            posts_by_domain = author_guid_posts_dict[author.author_guid]
            topics_probabilities = [0.0] * self.num_topics
            for post in posts_by_domain:
                for key in post_to_topic_id[post.post_id]:
                    topics_probabilities[key] = post_to_topic_id[post.post_id][key]

            author_topic_mapping_items.append((author.author_guid, topics_probabilities))

        print()
        self._db.insert_into_author_toppic_mappings(author_topic_mapping_items)

    def add_to_db_topic_object(self, topic_id):
        topic = self.model.show_topic(topic_id, self._num_of_terms_in_topic)
        topic = sorted(topic, reverse=True)
        topic_words_ids = map(itemgetter(0), topic)
        topic_words_ids = map(int, topic_words_ids)
        term_id_term_txt = [self._db.create_term(word_id, self.dictionary[word_id]) for word_id in topic_words_ids]
        topic_id_term_id = [self._db.create_topic_item(topic_id, topic_item[0], topic_item[1]) for topic_item in topic]
        self._db.add_terms(term_id_term_txt)
        self._db.add_topic_items(topic_id_term_id)
