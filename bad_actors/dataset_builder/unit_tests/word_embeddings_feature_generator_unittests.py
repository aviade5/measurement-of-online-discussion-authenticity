import unittest
from DB.schema_definition import *
from dataset_builder.feature_extractor.word_embeddings_feature_generator import Word_Embeddings_Feature_Generator
from dataset_builder.word_embedding.glove_word_embedding_model_creator import GloveWordEmbeddingModelCreator

class Word_Embeddings_Feature_Generator_Unittests(unittest.TestCase):
    def setUp(self):
        self._config_parser = getConfig()
        self._db = DB()
        self._db.setUp()

        self._is_load_wikipedia_300d_glove_model = True
        self._wikipedia_model_file_path = "data/input/glove/test_glove.6B.300d.txt"
        self._table_name = "wikipedia_model_300d"
        self._word_vector_dict_full_path = "data/output/word_embedding/"
        self._word_vector_dict = {}

        self._author = None
        self._set_author(u'test_user')
        self._posts = []

    def tearDown(self):
        self._db.session.close()
        self._db.deleteDB()
        self._db.session.close()

    def test_0_dimension_of_word(self):
        self._add_post(u'Post1', u'was')
        self._setup_test()
        db_val = self._get_word_dimension(u'was', 0)
        self.assertEquals(db_val, 0.065573)

    def test_single_word_single_dimension_feature_correctness(self):
        self._add_post(u'Post1', u'was')
        self._setup_test()
        db_val = self._db.get_author_feature(self._author.author_guid, u"word_embeddings_np.mean_posts_content_d0").attribute_value
        self.assertAlmostEqual(float(db_val),0.065573, places=4)

    def test_single_word_single_dimension_feature_correctness2(self):
        self._add_post(u'Post1', u'was')
        self._setup_test()
        # db_val = self._db.get_author_feature(self._author.author_guid,
        #                                      u"word_embeddings_dimension_3_posts_content_max").attribute_value
        db_val = self._db.get_author_feature(self._author.author_guid,
                                             u"word_embeddings_max_posts_content_d3").attribute_value
        self.assertEquals(db_val, u'-0.2133')

    def test_two_words_single_dimension_max_feature_correctness(self):
        self._add_post(u'Post1', u'was is')
        self._setup_test()
        # db_val = self._db.get_author_feature(self._author.author_guid,
        #                                      u"word_embeddings_dimension_3_posts_content_max").attribute_value
        db_val = self._db.get_author_feature(self._author.author_guid,
                                             u"word_embeddings_max_posts_content_d3").attribute_value
        was_d3 = self._get_word_dimension(u'was', 3)
        is_d3 = self._get_word_dimension(u'is', 3)
        expected_value = max(float(is_d3), float(was_d3))
        self.assertAlmostEqual(float(db_val), (expected_value), places=4)

    def test_two_words_single_dimention_min_feature_correctness(self):
        self._add_post(u'Post1', u'was is')
        self._setup_test()
        # db_val = self._db.get_author_feature(self._author.author_guid,
        #                                      u"word_embeddings_dimension_201_posts_content_min").attribute_value
        db_val = self._db.get_author_feature(self._author.author_guid,
                                             u"word_embeddings_min_posts_content_d201").attribute_value
        was_d201 = self._get_word_dimension(u'was', 201)
        is_d201 = self._get_word_dimension(u'is', 201)
        expected_value = min(float(is_d201), float(was_d201))
        self.assertAlmostEqual(float(db_val), (expected_value), places=4)

    def test_two_words_single_dimension_np_mean_feature_correctness(self):
        self._add_post(u'Post1', u'was')
        self._add_post(u'Post2', u'is')
        self._setup_test()
        db_val = self._db.get_author_feature(self._author.author_guid,
                                             u"word_embeddings_np.mean_posts_content_d298").attribute_value
        was_d298 = self._get_word_dimension(u'was', 298)
        is_d298 = self._get_word_dimension(u'is', 298)
        expected_value = (float(was_d298)+float(is_d298))/2
        self.assertAlmostEqual(float(db_val), expected_value, places=4)

    def test_four_words_single_dimension_np_mean_feature_correctness(self):
        self._add_post(u'Post1', u'was is')
        self._add_post(u'Post2', u'with said')
        self._setup_test()
        db_val = self._db.get_author_feature(self._author.author_guid,
                                             u"word_embeddings_np.mean_posts_content_d150").attribute_value
        was_d = self._get_word_dimension(u'was', 150)
        is_d = self._get_word_dimension(u'is', 150)
        with_d = self._get_word_dimension(u'with', 150)
        said_d = self._get_word_dimension(u'said', 150)
        expected_value = (float(was_d)+float(is_d)+float(with_d)+float(said_d))/4
        self.assertAlmostEqual(float(db_val), expected_value, places=4)

    def test_single_word_last_dimension_feature_correctness(self):
        self._add_post(u'Post1', u'was')
        self._setup_test()
        db_val = self._db.get_author_feature(self._author.author_guid,
                                             u"word_embeddings_max_posts_content_d299").attribute_value
        self.assertAlmostEqual(float(db_val), -0.080348, places=4)

    def _add_post(self, title, content):
        post = Post()
        post.author = self._author.author_full_name
        post.author_guid = self._author.author_guid
        post.content = content
        post.title = title
        post.domain = u'test'
        post.post_id = len(self._posts)
        post.guid = post.post_id
        self._db.addPost(post)
        self._posts.append(post)

    def _set_author(self, author_guid):
        author = Author()
        author.author_guid = author_guid
        author.author_full_name = u'name'+author_guid
        author.name = u'name'+author_guid
        author.domain = u'test'
        author.author_screen_name = u'author_screen_n'
        self._db.add_author(author)
        self._author = author

    def _setup_test(self):
        self._db.session.commit()
        self._word_embedding_model_creator = GloveWordEmbeddingModelCreator(self._db)
        self._word_embedding_model_creator.execute(None)

        params = {'authors': [self._author], 'posts':self._posts}
        self._word_embedding_feature_generator = Word_Embeddings_Feature_Generator(self._db, **params)
        self._word_embedding_feature_generator.execute()

        self._words = self._db.get_word_embedding_dictionary()

    def _get_word_dimension(self, word, dimension):
        word_vec = self._words[word]
        return word_vec[dimension]