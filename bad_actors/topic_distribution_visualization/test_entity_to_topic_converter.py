from collections import defaultdict
from unittest import TestCase

from DB.schema_definition import Post, Claim_Tweet_Connection, Author, DB
from commons.commons import clean_tweet, clean_content_by_nltk_stopwords
from topic_distribution_visualization.entity_to_topic_converter import EntityToTopicConverter


class TestEntityToTopicConverter(TestCase):
    def setUp(self):
        self._db = DB()
        self._db.setUp()
        self._posts = []
        self._post_dictionary = {}
        self._authors = []
        self._add_author(u'test author')
        self._preprocess_visualization = EntityToTopicConverter(self._db)

    def tearDown(self):
        self._db.session.close_all()
        self._db.deleteDB()
        self._db.session.close()

    def test_generate_topics_no_topics(self):
        arg = {'source': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content',
                          "where_clauses": [{"field_name": "domain", "value": "Claim"}]},
               'connection': {},
               'destination': {}}
        source_id_target_elements_dict = self._preprocess_visualization._get_source_id_target_elements(arg)
        self._preprocess_visualization.generate_topics_tables(source_id_target_elements_dict, arg)
        topics = self._db.get_topics()
        self.assertEqual(topics, [])

    def test_generate_topics_from_1_claim(self):
        self._add_post(u"test author", u'claim1', u'claim1 content', u'Claim')
        self._db.session.commit()
        arg = {'source': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content',
                          "where_clauses": [{"field_name": "domain", "value": "Claim"}]},
               'connection': {},
               'destination': {}}
        source_id_target_elements_dict = self._preprocess_visualization._get_source_id_target_elements(arg)
        self._preprocess_visualization.generate_topics_tables(source_id_target_elements_dict, arg)
        self._preprocess_visualization.save_topic_entities()

        self.assertTopicInserted(u'claim1')

    def test_generate_topics_from_1_claim_and_remove_stop_words(self):
        self._add_post(u"test author", u'claim1', u'claim1 go to the house', u'Claim')
        arg = {'source': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content',
                          "where_clauses": [{"field_name": "domain", "value": "Claim"}]},
               'connection': {},
               'destination': {}}
        self._db.session.commit()
        self._preprocess_visualization._remove_stop_words = True
        source_id_target_elements_dict = self._preprocess_visualization._get_source_id_target_elements(arg)
        self._preprocess_visualization.generate_topics_tables(source_id_target_elements_dict, arg)
        self._preprocess_visualization.save_topic_entities()

        self.assertTopicInserted(u'claim1')

    def test_generate_topics_from_5_claims(self):
        self._add_post(u"test author", u'claim1', u'claim1 content', u'Claim')
        self._add_post(u"test author", u'claim2', u'claim2 content', u'Claim')
        self._add_post(u"test author", u'claim3', u'claim3 content move', u'Claim')
        self._add_post(u"test author", u'claim4', u'claim4 dif data', u'Claim')
        self._add_post(u"test author", u'claim5', u'claim5 some boring text', u'Claim')
        self._db.session.commit()
        arg = {'source': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content',
                          "where_clauses": [{"field_name": "domain", "value": "Claim"}]},
               'connection': {},
               'destination': {}}
        source_id_target_elements_dict = self._preprocess_visualization._get_source_id_target_elements(arg)
        self._preprocess_visualization.generate_topics_tables(source_id_target_elements_dict, arg)
        self._preprocess_visualization.save_topic_entities()

        self.assertTopicInserted(u'claim1')
        self.assertTopicInserted(u'claim2')
        self.assertTopicInserted(u'claim3')
        self.assertTopicInserted(u'claim4')
        self.assertTopicInserted(u'claim5')

    def test_generate_post_topic_mapping_no_claim(self):
        arg = {'source': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content',
                          "where_clauses": [{"field_name": "domain", "value": "Claim"}]},
               'connection': {},
               'destination': {}}
        source_id_target_elements_dict = self._preprocess_visualization._get_source_id_target_elements(arg)
        self._preprocess_visualization.generate_post_topic_mapping(source_id_target_elements_dict, arg)
        mappings = self._db.get_post_topic_mapping()
        self.assertEqual(0, len(mappings))

    def test_generate_post_topic_mapping_1_claim(self):
        self._add_post(u"test author", u'claim1', u'claim1 content', u'Claim')
        self._add_post(u"test author", u'post1', u'post1 content of data', u'Microblog')
        self._add_post(u"test author", u'post2', u'post2  bla bla', u'Microblog')
        self._add_post(u"test author", u'post3', u'post3 noting  new', u'Microblog')
        self._add_claim_tweet_connection(u'claim1', u'post1')
        self._add_claim_tweet_connection(u'claim1', u'post2')
        self._add_claim_tweet_connection(u'claim1', u'post3')
        self._db.session.commit()
        arg = {'source': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content',
                          "where_clauses": [{"field_name": "domain", "value": "Claim"}]},
               'connection': {'table_name': 'claim_tweet_connection', 'source_id': 'claim_id',
                              'target_id': 'post_id', },
               'destination': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content',
                               "where_clauses": [{"field_name": "domain", "value": "Microblog"}]}}
        source_id_target_elements_dict = self._preprocess_visualization._get_source_id_target_elements(arg)
        self._preprocess_visualization.generate_topics_tables(source_id_target_elements_dict, arg)
        self._preprocess_visualization.generate_post_topic_mapping(source_id_target_elements_dict, arg)
        self._preprocess_visualization.save_topic_entities()

        mappings = self._db.get_post_topic_mapping()
        mappings = [(tm.post_id, tm.max_topic_id, tm.max_topic_dist) for tm in mappings]
        topic_id = self._preprocess_visualization.get_source_id_topic_dictionary()[u'claim1']
        self.assertEqual(3, len(mappings))
        self.assertSetEqual({('post1', topic_id, 1.0), ('post2', topic_id, 1.0), ('post3', topic_id, 1.0)},
                            set(mappings))

    def test_generate_post_topic_mapping_2_claim(self):
        self._add_post(u"test author", u'claim1', u'claim1 content', u'Claim')
        self._add_post(u"test author", u'claim2', u'claim1 content', u'Claim')
        self._add_post(u"test author", u'post1', u'post1 content of data', u'Microblog')
        self._add_post(u"test author", u'post2', u'post2  bla bla', u'Microblog')
        self._add_post(u"test author", u'post3', u'post3 noting  new', u'Microblog')
        self._add_post(u"test author", u'post4', u'post4  bla bla', u'Microblog')
        self._add_post(u"test author", u'post5', u'post5 noting  new', u'Microblog')
        self._add_claim_tweet_connection(u'claim1', u'post1')
        self._add_claim_tweet_connection(u'claim1', u'post2')
        self._add_claim_tweet_connection(u'claim1', u'post3')
        self._add_claim_tweet_connection(u'claim2', u'post4')
        self._add_claim_tweet_connection(u'claim2', u'post5')
        self._db.session.commit()
        arg = {'source': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content',
                          "where_clauses": [{"field_name": "domain", "value": "Claim"}]},
               'connection': {'table_name': 'claim_tweet_connection', 'source_id': 'claim_id',
                              'target_id': 'post_id', },
               'destination': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content',
                               "where_clauses": [{"field_name": "domain", "value": "Microblog"}]}}
        source_id_target_elements_dict = self._preprocess_visualization._get_source_id_target_elements(arg)
        self._preprocess_visualization.generate_topics_tables(source_id_target_elements_dict, arg)
        self._preprocess_visualization.generate_post_topic_mapping(source_id_target_elements_dict, arg)
        self._preprocess_visualization.save_topic_entities()

        mappings = self._db.get_post_topic_mapping()
        mappings = [(tm.post_id, tm.max_topic_id, tm.max_topic_dist) for tm in mappings]
        topic_id1 = self._preprocess_visualization.get_source_id_topic_dictionary()[u'claim1']
        topic_id2 = self._preprocess_visualization.get_source_id_topic_dictionary()[u'claim2']
        self.assertEqual(5, len(mappings))
        self.assertSetEqual(
            {('post1', topic_id1, 1.0), ('post2', topic_id1, 1.0), ('post3', topic_id1, 1.0), ('post4', topic_id2, 1.0),
             ('post5', topic_id2, 1.0)}, set(mappings))

    def test__generate_author_topic_mapping_2_claim(self):
        self._add_author(u'test author2')
        self._add_post(u"test author", u'claim1', u'claim1 content', u'Claim')
        self._add_post(u"test author2", u'claim2', u'claim1 content', u'Claim')
        self._add_post(u"test author", u'post1', u'post1 content of data', u'Microblog')
        self._add_post(u"test author", u'post2', u'post2  bla bla', u'Microblog')
        self._add_post(u"test author", u'post3', u'post3 noting  new', u'Microblog')
        self._add_post(u"test author", u'post4', u'post4  bla bla', u'Microblog')
        self._add_post(u"test author", u'post5', u'post5 noting  new', u'Microblog')
        self._add_claim_tweet_connection(u'claim1', u'post1')
        self._add_claim_tweet_connection(u'claim1', u'post2')
        self._add_claim_tweet_connection(u'claim1', u'post3')
        self._add_claim_tweet_connection(u'claim2', u'post4')
        self._add_claim_tweet_connection(u'claim2', u'post5')
        self._db.session.commit()
        arg = {'source': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content',
                          "where_clauses": [{"field_name": "domain", "value": "Claim"}]},
               'connection': {'table_name': 'claim_tweet_connection', 'source_id': 'claim_id',
                              'target_id': 'post_id', },
               'destination': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content',
                               "where_clauses": [{"field_name": "domain", "value": "Microblog"}]}}
        self._preprocess_visualization._domain = u"Microblog"
        source_id_target_elements_dict = self._preprocess_visualization._get_source_id_target_elements(arg)
        self._preprocess_visualization.generate_topics_tables(source_id_target_elements_dict, arg)
        self._preprocess_visualization.generate_post_topic_mapping(source_id_target_elements_dict, arg)
        self._preprocess_visualization.generate_author_topic_mapping()
        self._preprocess_visualization.save_topic_entities()
        mapping = self._db.get_author_topic_mapping()
        self.assertEqual(2, len(mapping))
        self.assertSetEqual({(u'test author', 0.6, 0.4), (u'test author2', 0, 0)}, set(mapping))

    def test_visualization(self):
        self._add_author(u'test author2', u"bad_actor")
        self._add_post(u"test author", u'claim1', u'claim1 content', u'Claim')
        self._add_post(u"test author2", u'claim2', u'claim2 content', u'Claim')
        self._add_post(u"test author", u'post1', u'post1 content of data', u'Microblog')
        self._add_post(u"test author", u'post2', u'post2  bla bla', u'Microblog')
        self._add_post(u"test author", u'post3', u'post3 noting  new', u'Microblog')
        self._add_post(u"test author2", u'post4', u'post4  bla bla', u'Microblog')
        self._add_post(u"test author2", u'post5', u'post5 noting  new', u'Microblog')
        self._add_claim_tweet_connection(u'claim1', u'post1')
        self._add_claim_tweet_connection(u'claim1', u'post2')
        self._add_claim_tweet_connection(u'claim1', u'post4')
        self._add_claim_tweet_connection(u'claim2', u'post3')
        self._add_claim_tweet_connection(u'claim2', u'post5')
        self._db.session.commit()
        self._preprocess_visualization._domain = u"Microblog"
        self._preprocess_visualization.execute()

        author_topic_mapping = self._db.get_author_topic_mapping()
        post_topic_mappings = self._db.get_post_topic_mapping()
        post_topic_mappings = [(tm.post_id, tm.max_topic_id, tm.max_topic_dist) for tm in post_topic_mappings]
        topic_id1 = self._preprocess_visualization.get_source_id_topic_dictionary()[u'claim1']
        topic_id2 = self._preprocess_visualization.get_source_id_topic_dictionary()[u'claim2']
        self.assertEqual(2, len(author_topic_mapping))
        self.assertSetEqual({(u'test author', 0.666666666667, 0.333333333333), (u'test author2', 0.5, 0.5)},
                            set(author_topic_mapping))
        self.assertSetEqual(
            {('post1', topic_id1, 1.0), ('post2', topic_id1, 1.0), ('post3', topic_id2, 1.0), ('post4', topic_id1, 1.0),
             ('post5', topic_id2, 1.0)}, set(post_topic_mappings))

    def test_double_execution_visualization(self):
        self._add_author(u'test author2', u"bad_actor")
        self._add_post(u"test author", u'claim1', u'claim1 content', u'Claim')
        self._add_post(u"test author2", u'claim2', u'claim2 content', u'Claim')
        self._add_post(u"test author", u'post1', u'post1 content of data', u'Microblog')
        self._add_post(u"test author", u'post2', u'post2  bla bla', u'Microblog')
        self._add_post(u"test author", u'post3', u'post3 noting  new', u'Microblog')
        self._add_post(u"test author2", u'post4', u'post4  bla bla', u'Microblog')
        self._add_post(u"test author2", u'post5', u'post5 noting  new', u'Microblog')
        self._add_claim_tweet_connection(u'claim1', u'post1')
        self._add_claim_tweet_connection(u'claim1', u'post2')
        self._add_claim_tweet_connection(u'claim1', u'post4')
        self._add_claim_tweet_connection(u'claim2', u'post3')
        self._add_claim_tweet_connection(u'claim2', u'post5')
        self._db.session.commit()
        self._preprocess_visualization._domain = u"Microblog"
        self._preprocess_visualization.execute()
        self._preprocess_visualization.execute()

        author_topic_mapping = self._db.get_author_topic_mapping()
        post_topic_mappings = self._db.get_post_topic_mapping()
        post_topic_mappings = [(tm.post_id, tm.max_topic_id, tm.max_topic_dist) for tm in post_topic_mappings]
        topic_id1 = self._preprocess_visualization.get_source_id_topic_dictionary()[u'claim1']
        topic_id2 = self._preprocess_visualization.get_source_id_topic_dictionary()[u'claim2']
        self.assertEqual(2, len(author_topic_mapping))
        self.assertSetEqual({(u'test author', 0.666666666667, 0.333333333333), (u'test author2', 0.5, 0.5)},
                            set(author_topic_mapping))
        self.assertSetEqual(
            {('post1', topic_id1, 1.0), ('post2', topic_id1, 1.0), ('post3', topic_id2, 1.0), ('post4', topic_id1, 1.0),
             ('post5', topic_id2, 1.0)}, set(post_topic_mappings))

    def assertTopicInserted(self, claim_id):
        topics = self._db.get_topics()
        terms = self._db.get_terms()
        topic_dict = defaultdict(set)
        term_dict = {term.term_id: term.description for term in terms}
        for topic_id, term_id, prob in topics:
            topic_dict[topic_id].add(term_dict[term_id])
        topic_id = self._preprocess_visualization.get_source_id_topic_dictionary()[claim_id]
        self.assertIn(topic_id, topic_dict)
        expected = set(clean_tweet(self._post_dictionary[claim_id].content).split(' '))
        if self._preprocess_visualization._remove_stop_words:
            expected = set(clean_content_by_nltk_stopwords(self._post_dictionary[claim_id].content).split(' '))
        self.assertSetEqual(expected, topic_dict[topic_id])

    def _add_author(self, author_guid, type=u"good_actor"):
        author = Author()
        author.author_guid = author_guid
        author.author_full_name = author_guid
        author.author_screen_name = author_guid
        author.name = author_guid
        author.domain = u'Microblog'
        author.author_type = type
        self._db.add_author(author)
        self._authors.append(author)

    def _add_post(self, author_guid, title, content, domain=u'Microblog'):
        post = Post()
        post.author = author_guid
        post.author_guid = author_guid
        post.content = content
        post.title = title
        post.domain = domain
        post.post_id = title
        post.guid = post.post_id
        post.is_detailed = True
        post.is_LB = False
        self._db.addPost(post)
        self._posts.append(post)
        self._post_dictionary[post.post_id] = post

    def _add_claim_tweet_connection(self, claim_id, post_id):
        connection = Claim_Tweet_Connection()
        connection.claim_id = claim_id
        connection.post_id = post_id
        self._db.add_claim_connections([connection])
        pass
