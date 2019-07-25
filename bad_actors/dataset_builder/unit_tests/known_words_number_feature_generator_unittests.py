import unittest

from DB.schema_definition import *
from dataset_builder.feature_extractor.known_words_number_feature_generator import Known_Words_Number_Feature_generator


class Known_Words_Number_Feature_generator_Unittests(unittest.TestCase):
    def setUp(self):
        self._config_parser = getConfig()
        self._db = DB()
        self._db.setUp()
        self._posts = []
        self._add_author(u'this is a test author')
        self._post_dictionary = {}

    def tearDown(self):
        self._db.session.close_all()
        self._db.deleteDB()
        self._db.session.close()

    def test_case_simple(self):
        self._add_post(u'post1', u'when\'s why\'ll')
        self._add_post(u'post2', u'you\'re too good to be true')
        self._execute_module()

        self.assert_both_fields(1.5, 0)

    def test_case_no_words_in_one_post(self):
        self._add_post(u'post1', u'wont dog')
        self._add_post(u'post2', u'')
        self._execute_module()
        self.assert_both_fields(0.5, 0.5)

    def test_case_no_words(self):
        self._add_post(u'post1', u' ')
        self._add_post(u'post2', u'')
        self._execute_module()
        self.assert_both_fields(0, 0)

    def test_case_one_word_in_each_post(self):
        self._add_post(u'post1', u'won\'t')
        self._add_post(u'post2', u'dog')
        self._execute_module()
        self.assert_both_fields(0.5, 0.5)

    def test_case_many_subjects(self):
        self._add_post(u'post1', u'kid reasons who girl guy')
        self._execute_module()
        self.assert_both_fields(0, 5)

    def test_case_many_constractions(self):
        self._add_post(u'post1', u'wont wouldnt, wouldve youd, youl!l, youre')
        self._execute_module()
        self.assert_both_fields(6, 0)

    def tes_load_known_words_from_file_to_dict(self):
        params = self._get_params()
        self._known_words_number_feature_generator = Known_Words_Number_Feature_generator(self._db, **params)
        self._known_words_number_feature_generator._load_known_words_to_dict(u'subjects')
        words_loaded = self._known_words_number_feature_generator._words_dict
        self.assertTrue('dog' in words_loaded)
        self.assertTrue('guy' in words_loaded)
        self.assertTrue('people' in words_loaded)
        self.assertTrue('you' in words_loaded)

    def test_load_known_words_from_not_exists_file_to_dict(self):
        params = self._get_params()
        self._known_words_number_feature_generator = Known_Words_Number_Feature_generator(self._db, **params)
        self._known_words_number_feature_generator._load_known_words_to_dict(u'no_such_file_exists')
        words_loaded = self._known_words_number_feature_generator._words_dict
        self.assertEqual(words_loaded, {})

    def test_num_of_known_words_in_empty_sentence(self):
        params = self._get_params()
        self._known_words_number_feature_generator = Known_Words_Number_Feature_generator(self._db, **params)
        kwon_words_count = self._known_words_number_feature_generator._num_of_known_words_in_sentence(u"")
        self.assertEqual(0, kwon_words_count)

    def test_num_of_known_words_in_sentence_with_no_known_word(self):
        params = self._get_params()
        self._known_words_number_feature_generator = Known_Words_Number_Feature_generator(self._db, **params)
        self._known_words_number_feature_generator._load_known_words_to_dict(u'subjects')
        kwon_words_count = self._known_words_number_feature_generator._num_of_known_words_in_sentence(u"test sentence")
        self.assertEqual(0, kwon_words_count)

    def test_num_of_known_words_in_sentence_with_3_known_word(self):
        params = self._get_params()
        self._known_words_number_feature_generator = Known_Words_Number_Feature_generator(self._db, **params)
        self._known_words_number_feature_generator._load_known_words_to_dict(u'subjects')
        kwon_words_count = self._known_words_number_feature_generator._num_of_known_words_in_sentence(
            u"man parent they sentence")
        self.assertEqual(3, kwon_words_count)

    def test_num_of_known_words_in_sentence_with_1_known_word_3_times(self):
        params = self._get_params()
        self._known_words_number_feature_generator = Known_Words_Number_Feature_generator(self._db, **params)
        self._known_words_number_feature_generator._load_known_words_to_dict(u'subjects')
        kwon_words_count = self._known_words_number_feature_generator._num_of_known_words_in_sentence(
            u"man man man sentence")
        self.assertEqual(3, kwon_words_count)

    def test_count_avg_known_words_for_5_sentences(self):
        params = self._get_params()
        self._known_words_number_feature_generator = Known_Words_Number_Feature_generator(self._db, **params)
        self._known_words_number_feature_generator._load_known_words_to_dict(u'subjects')
        sentences = [u"man man man sentence",
                     u"man parent they sentence",
                     u"man thoughts hello sentence",
                     u"ways what sentence",
                     u"hi sentence"]
        avg_known_words = self._known_words_number_feature_generator._count_avg_known_words(sentences)
        self.assertAlmostEqual(10.0 / 5.0, avg_known_words, 0.000001)

    def test_case_post_represent_by_posts(self):
        self._add_post(u'post1', u'the claim', u'Claim')
        self._add_post(u'post2', u'dog cat pig man')  # 2
        self._add_post(u'post3', u'TV is the best guys')  # 1
        self._add_claim_tweet_connection(u'post1', u'post2')
        self._add_claim_tweet_connection(u'post1', u'post3')
        self._db.session.commit()
        params = self._get_params()
        self._known_words_number_feature_generator = Known_Words_Number_Feature_generator(self._db, **params)
        self._known_words_number_feature_generator._load_known_words_to_dict(u'subjects')
        arg = {'source': {'table_name': 'posts', 'id': 'post_id'},
               'connection': {'table_name': 'claim_tweet_connection', 'source_id': 'claim_id', 'target_id': 'post_id',
                              },
               'destination': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content'}}
        author_id_texts_dict = self._known_words_number_feature_generator._get_source_id_target_elements(arg)
        expected = {u'post1': [self._post_dictionary[u'post2'], self._post_dictionary[u'post3']]}
        self.assertSourceIdElementsDict(author_id_texts_dict, expected)

    def test_case_post_from_claim_represent_by_posts(self):
        self._add_post(u'post1', u'the claim1', u'Claim')
        self._add_post(u'post4', u'the claim4', u'Microblog')
        self._add_post(u'post2', u'dog cat pig man')  # 2
        self._add_post(u'post3', u'TV is the best guys')  # 1
        self._add_claim_tweet_connection(u'post1', u'post2')
        self._add_claim_tweet_connection(u'post1', u'post3')
        self._add_claim_tweet_connection(u'post4', u'post2')
        self._add_claim_tweet_connection(u'post4', u'post3')
        self._db.session.commit()
        params = self._get_params()
        self._known_words_number_feature_generator = Known_Words_Number_Feature_generator(self._db, **params)
        self._known_words_number_feature_generator._load_known_words_to_dict(u'subjects')
        arg = {'source': {'table_name': 'posts', 'id': 'post_id',
                          "where_clauses": [{"field_name": "domain", "value": "Microblog"}]},
               'connection': {'table_name': 'claim_tweet_connection', 'source_id': 'claim_id', 'target_id': 'post_id'},
               'destination': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content'}}
        author_id_texts_dict = self._known_words_number_feature_generator._get_source_id_target_elements(arg)
        expected = {u'post4': [self._post_dictionary[u'post2'], self._post_dictionary[u'post3']]}
        self.assertSourceIdElementsDict(author_id_texts_dict, expected)

    def assertSourceIdElementsDict(self, author_id_texts_dict, expected):
        self.assertEqual(str(dict(author_id_texts_dict)), str(dict(expected)))

    def test_case_post_represent_by_posts_from_microblog(self):
        self._add_post(u'post1', u'the claim1', u'Claim')
        self._add_post(u'post2', u'dog cat pig man')  # 2
        self._add_post(u'post4', u'the claim4', u'Microblog')
        self._add_post(u'post3', u'TV is the best guys')  # 1
        self._add_claim_tweet_connection(u'post1', u'post2')
        self._add_claim_tweet_connection(u'post1', u'post3')
        self._add_claim_tweet_connection(u'post1', u'post4')
        self._add_claim_tweet_connection(u'post4', u'post3')
        self._db.session.commit()
        params = self._get_params()
        self._known_words_number_feature_generator = Known_Words_Number_Feature_generator(self._db, **params)
        self._known_words_number_feature_generator._load_known_words_to_dict(u'subjects')
        arg = {'source': {'table_name': 'posts', 'id': 'post_id',
                          "where_clauses": [{"field_name": "domain", "value": "Microblog"}]},
               'connection': {'table_name': 'claim_tweet_connection', 'source_id': 'claim_id', 'target_id': 'post_id'},
               'destination': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content',
                               "where_clauses": [{"field_name": "domain", "value": "Microblog"}]}}
        author_id_texts_dict = self._known_words_number_feature_generator._get_source_id_target_elements(arg)
        expected = {u'post4': [self._post_dictionary[u'post3']]}
        self.assertSourceIdElementsDict(author_id_texts_dict, expected)

    def test_case_post_from_claim_represent_by_posts_from_microblog(self):
        self._add_post(u'post1', u'the claim1', u'Claim')
        self._add_post(u'post2', u'dog cat pig man')  # 2
        self._add_post(u'post4', u'the claim4', u'Microblog')
        self._add_post(u'post3', u'TV is the best guys')  # 1
        self._add_claim_tweet_connection(u'post1', u'post2')
        self._add_claim_tweet_connection(u'post1', u'post3')
        self._add_claim_tweet_connection(u'post1', u'post4')
        self._add_claim_tweet_connection(u'post4', u'post3')
        self._db.session.commit()
        params = self._get_params()
        self._known_words_number_feature_generator = Known_Words_Number_Feature_generator(self._db, **params)
        self._known_words_number_feature_generator._load_known_words_to_dict(u'subjects')
        arg = {'source': {'table_name': 'posts', 'id': 'post_id',
                          },
               'connection': {'table_name': 'claim_tweet_connection', 'source_id': 'claim_id', 'target_id': 'post_id'},
               'destination': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content',
                               "where_clauses": [{"field_name": "domain", "value": "Microblog"}]}}
        author_id_texts_dict = self._known_words_number_feature_generator._get_source_id_target_elements(arg)
        expected = {u'post1': [self._post_dictionary[u'post2'], self._post_dictionary[u'post3'],
                               self._post_dictionary[u'post4']], u'post4': [self._post_dictionary[u'post3']]}
        self.assertSourceIdElementsDict(author_id_texts_dict, expected)

    def test_get_author_id_to_text_case_one_word_in_each_post(self):
        self._add_post(u'post2', u'dog cat pig man')  # 2
        self._add_post(u'post3', u'TV is the best guys')  # 1
        self._db.session.commit()
        params = self._get_params()
        self._known_words_number_feature_generator = Known_Words_Number_Feature_generator(self._db, **params)
        self._known_words_number_feature_generator._load_known_words_to_dict(u'subjects')
        arg = {'source': {'table_name': 'authors', 'id': 'author_guid',
                          "where_clauses": [{"field_name": "domain", "value": "tests"}]},
               'connection': {'table_name': 'posts', 'source_id': 'author_guid', 'target_id': 'post_id'},
               'destination': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content'}}
        author_id_texts_dict = self._known_words_number_feature_generator._get_source_id_target_elements(arg)
        expected = {u'this is a test author': [self._post_dictionary[u'post2'], self._post_dictionary[u'post3']]}
        self.assertSourceIdElementsDict(author_id_texts_dict, expected)

    def test_get_post_id_to_post_content_case_only_destination_in_args(self):
        self._add_post(u'post2', u'dog cat pig man')  # 2
        self._add_post(u'post3', u'TV is the best guys', u"Claim")  # 1
        self._db.session.commit()
        params = self._get_params()
        self._known_words_number_feature_generator = Known_Words_Number_Feature_generator(self._db, **params)
        self._known_words_number_feature_generator._load_known_words_to_dict(u'subjects')
        arg = {'source': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content',
                          "where_clauses": [{"field_name": "domain", "value": "Claim"}]},
               'connection': {},
               'destination': {}}
        author_id_texts_dict = self._known_words_number_feature_generator._get_source_id_target_elements(arg)
        expected = {u'post3': [self._post_dictionary[u'post3']]}
        self.assertSourceIdElementsDict(author_id_texts_dict, expected)

    def assert_both_fields(self, contrcations, subjects):
        self._generic_test("Known_Words_Number_Feature_generator_count_avg_known_word_from_contractions", contrcations)
        self._generic_test("Known_Words_Number_Feature_generator_count_avg_known_word_from_subjects", subjects)

    def _generic_test(self, attribute, expected_value):
        db_val = self._db.get_author_feature(self._author.author_guid, attribute).attribute_value
        self.assertAlmostEqual(float(db_val), float(expected_value))

    def _execute_module(self):
        self._db.session.commit()
        params = self._get_params()

        self._known_words_number_feature_generator = Known_Words_Number_Feature_generator(self._db, **params)
        self._known_words_number_feature_generator.execute()

    def _get_params(self):
        posts = {self._author.author_guid: self._posts}
        params = params = {'authors': [self._author], 'posts': posts}
        return params

    def _add_author(self, author_guid):
        author = Author()
        author.author_guid = author_guid
        author.author_full_name = u'test author'
        author.name = u'test'
        author.domain = u'tests'
        self._db.add_author(author)
        self._author = author

    def _add_post(self, title, content, domain=u'Microblog'):
        post = Post()
        post.author = self._author.author_guid
        post.author_guid = self._author.author_guid
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
