from preprocessing_tools.abstract_controller import AbstractController
from nltk.corpus import stopwords
import string
import nltk
import pandas as pd

try:
    from gensim import *

    lda_model = models.ldamodel
except:
    print "WARNING! gensim is not available! This module is not usable."

from operator import itemgetter

from DB.schema_definition import *
import logging


class LDATopicModel(AbstractController):
    """Imports topics from files."""

    def __init__(self, db):

        AbstractController.__init__(self, db)
        nltk.download('stopwords')
        self._num_of_terms_in_topic = self._config_parser.eval(self.__class__.__name__, "num_of_terms_in_topic")
        self.num_topics = self._config_parser.eval(self.__class__.__name__, "number_of_topics")
        self.stopword_file = self._config_parser.get(self.__class__.__name__, "stopword_file")
        self.stemlanguage = self._config_parser.get(self.__class__.__name__, "stem_language")
        self._removed_keywords_file = self._config_parser.eval(self.__class__.__name__, "removed_keywords_file")
        self._post_topic_probability_table_path = self._config_parser.eval(self.__class__.__name__, "post_topic_probability_table_path")
        self._to_print_top_10_terms = self._config_parser.eval(self.__class__.__name__, "to_print_top_10_terms")
        self._output_file = self._config_parser.eval(self.__class__.__name__, "output_path")

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
        curr_posts = self._db.getPostsListWithoutEmptyRowsByDate(self._window_start, self._window_end)

        if len(curr_posts) > 0:
            post_id_to_words = self._create_post_id_to_content_words(curr_posts)
            self.calculate_topics(post_id_to_words)
        self._db.session.commit()
        if self._to_print_top_10_terms:
            self._print_top_k_terms_per_topic(10)
        pass

    def _create_post_id_to_content_words(self, curr_posts):
        post_id_to_content = {post[2]: post[6] for post in curr_posts}
        post_id_to_ngrams = {}
        for doc_id, content in post_id_to_content.iteritems():
            if content is not None:
                words = self._clean_content(content)
                #words = clean_content_to_set_of_words(self.stopword_file, content, self.stemlanguage)
                words = ' '.join(words)
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

        self._post_id_topic_probility_dataframe = pd.DataFrame()

        for post_id in post_id_to_words:
            content_words = post_id_to_words[post_id]
            bow = self.dictionary.doc2bow(content_words)
            topic_id_to_probability = self.model.get_document_topics(bow, minimum_probability=0)

            self._fill_post_id_topic_probility_dataframe(post_id, topic_id_to_probability)

            post_to_topic_id[post_id] = {probability[0]: probability[1] for probability in topic_id_to_probability}
            max_topic_probability = max(topic_id_to_probability, key=lambda item: item[1])
            ptm = self._db.create_post_topic_mapping_obj(max_topic_probability, post_id)
            post_topic_mappings.append(ptm)

        #export to csv
        self._post_id_topic_probility_dataframe.to_csv(self._post_topic_probability_table_path)

        # export to table in DB
        engine = self._db.engine

        self._post_id_topic_probility_dataframe.to_sql(name="post_id_topic_probability", con=engine, if_exists='replace')

        self._db.addPostTopicMappings(post_topic_mappings)
        self._db.delete_author_topic_mapping()
        self.create_author_topic_mapping_table(post_to_topic_id)

    def create_author_topic_mapping_table(self, post_to_topic_id):
        authors = self._db.get_authors_by_domain(self._domain)
        self._db.create_author_topic_mapping_table(self.num_topics)
        for author in authors:
            posts_by_domain = self._db.get_posts_by_author_guid(author.author_guid)
            topics_probabilities = [0] * self.num_topics
            for post in posts_by_domain:
                for key in post_to_topic_id[post.guid]:
                    topics_probabilities[key] = post_to_topic_id[post.guid][key]

            self._db.insert_into_author_toppic_mapping(str(author.author_guid), topics_probabilities)

    def add_to_db_topic_object(self, topic_id):
        topic = self.model.show_topic(topic_id, self._num_of_terms_in_topic)
        topic = sorted(topic, reverse=True)
        topic_words_ids = map(itemgetter(0), topic)
        topic_words_ids = map(int, topic_words_ids)
        term_id_term_txt = [self._db.create_term(word_id, self.dictionary[word_id]) for word_id in topic_words_ids]
        topic_id_term_id = [self._db.create_topic_item(topic_id, topic_item[0], topic_item[1]) for topic_item in topic]
        self._db.add_terms(term_id_term_txt)
        self._db.add_topic_items(topic_id_term_id)

    def _clean_content(self, content):
        content = content.lower()
        exclude = set(string.punctuation)
        content = ''.join(ch for ch in content if ch not in exclude)

        content = content.replace('&amp;', '&')
        content = content.replace(',', '')
        content = content.replace('!', '')
        content = content.replace('-', '')
        content = content.replace('.', '')
        content = re.sub(r'http\S+', '', content)
        content = content.replace("<em>", "")
        content = content.replace("</em>", "")

        words = content.split()
        words = [word for word in words if word not in stopwords.words('english')]

        keywords_to_remove = set_of_stopwords(self._removed_keywords_file)
        words = [word for word in words if word not in keywords_to_remove]

        #stemmer = PorterStemmer()
        #words = self._stem_tokens(words, stemmer)
        return words

    def _stem_tokens(self, tokens, stemmer):
        stemmed = []
        for item in tokens:
            stemmed.append(stemmer.stem(item))
        return stemmed

    def _fill_post_id_topic_probility_dataframe(self, post_id, topic_id_to_probability):
        topics = [probability[0] for probability in topic_id_to_probability]
        probability_per_topic = [probability[1] for probability in topic_id_to_probability]
        post_ids = [post_id] * len(topic_id_to_probability)

        dict = {}
        dict['topics'] = topics
        dict['post_ids'] = post_ids
        dict['probability_per_topic'] = probability_per_topic

        df = pd.DataFrame(dict)
        df = df.pivot(index='post_ids', columns='topics', values='probability_per_topic')

        self._post_id_topic_probility_dataframe = self._post_id_topic_probility_dataframe.append(df)

    def _print_top_k_terms_per_topic(self, k):
        with open(self._output_file+'lda_top_10.csv', 'wb') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(['topic'])
            for topic in range(self.num_topics):
                top = self._db._get_top_terms_by_topic_id(topic, k)
                row_str = str(top)
                csv_writer.writerow(top)
                print "topic "+str(topic)+": "+row_str
