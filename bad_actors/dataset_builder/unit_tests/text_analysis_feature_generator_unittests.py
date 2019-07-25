from __future__ import division
from dataset_builder.feature_extractor.text_analysis_feature_generator import Text_Anlalyser_Feature_Generator
import unittest

from DB.schema_definition import *

class Text_Analysis_Feature_Generator_Unittest(unittest.TestCase):
    def setUp(self):
        self._config_parser = getConfig()
        self._db = DB()
        self._db.setUp()

        self._posts = []
        self._add_author(u'this is a test author')
        self._add_author(u'This author will have no effect')

    def tearDown(self):
        self._db.session.commit()
        self._db.session.close()
        self._db.deleteDB()
        self._db.session.close()

    def test_single_post(self):
        self._add_post(u'post1', u'This Sentence of a TEST Thing')
        self._set_up_test_env()
        self._generic_test(u'num_of_chars_on_avg', 24)
        self._generic_test(u'num_of_quotations_on_avg', 0)
        self._generic_test(u'num_of_uppercase_words_in_post_on_avg', 1)
        self._generic_test(u'number_of_precent_of_uppercased_posts', 0)
        self._generic_test(u'num_of_formal_words_on_avg', 6)
        self._generic_test(u'num_of_informal_words_on_avg', 0)
        self._generic_test(u'precent_of_formal_words_on_avg', 1)
        self._generic_test(u'num_of_question_marks_on_avg', 0)
        self._generic_test(u'num_of_colons_on_avg', 0)
        self._generic_test(u'num_of_comas_on_avg', 0)
        self._generic_test(u'num_of_retweets_on_avg', 0)
        self._generic_test(u'num_of_ellipsis_on_avg', 0)
        self._generic_test(u'num_of_stop_words_on_avg', 2)
        self._generic_test(u'precent_of_stop_words_on_avg', 1/3)

    def test_different_posts(self):
        self._add_post(u'post1', u'THE THE THE THE')
        self._add_post(u'post2', u'you...')
        self._add_post(u'post3', u'')

        self._set_up_test_env()
        self._generic_test(u'num_of_chars_on_avg', 18/3)
        self._generic_test(u'num_of_quotations_on_avg', 0)
        self._generic_test(u'num_of_uppercase_words_in_post_on_avg', 4/3)
        self._generic_test(u'number_of_precent_of_uppercased_posts', 1/3)
        self._generic_test(u'num_of_formal_words_on_avg', 5/3)
        self._generic_test(u'num_of_informal_words_on_avg', 0)
        self._generic_test(u'precent_of_formal_words_on_avg', 2/3)
        self._generic_test(u'num_of_question_marks_on_avg', 0)
        self._generic_test(u'num_of_colons_on_avg', 0)
        self._generic_test(u'num_of_comas_on_avg', 0)
        self._generic_test(u'num_of_retweets_on_avg', 0)
        self._generic_test(u'num_of_ellipsis_on_avg', 1/3)
        self._generic_test(u'num_of_stop_words_on_avg', 1/3)
        self._generic_test(u'precent_of_stop_words_on_avg', 1/3)

    def test_single_empty_post(self):
        self._add_post(u'post1', u'')
        self._set_up_test_env()
        self._generic_test(u'num_of_chars_on_avg', 0)
        self._generic_test(u'num_of_quotations_on_avg', 0)
        self._generic_test(u'num_of_uppercase_words_in_post_on_avg', 0)
        self._generic_test(u'number_of_precent_of_uppercased_posts', 0)
        self._generic_test(u'num_of_formal_words_on_avg', 0)
        self._generic_test(u'num_of_informal_words_on_avg', 0)
        self._generic_test(u'precent_of_formal_words_on_avg', 0)
        self._generic_test(u'num_of_question_marks_on_avg', 0)
        self._generic_test(u'num_of_colons_on_avg', 0)
        self._generic_test(u'num_of_comas_on_avg', 0)
        self._generic_test(u'num_of_retweets_on_avg', 0)
        self._generic_test(u'num_of_ellipsis_on_avg', 0)
        self._generic_test(u'num_of_stop_words_on_avg', 0)
        self._generic_test(u'precent_of_stop_words_on_avg', 0)

    def test_several_empty_posts(self):
        self._add_post(u'post1', u'')
        self._add_post(u'post2', u'')
        self._add_post(u'post3', u'')

        self._set_up_test_env()

        self._generic_test(u'num_of_chars_on_avg', 0)
        self._generic_test(u'num_of_quotations_on_avg', 0)
        self._generic_test(u'num_of_uppercase_words_in_post_on_avg', 0)
        self._generic_test(u'number_of_precent_of_uppercased_posts', 0)
        self._generic_test(u'num_of_formal_words_on_avg', 0)
        self._generic_test(u'num_of_informal_words_on_avg', 0)
        self._generic_test(u'precent_of_formal_words_on_avg', 0)
        self._generic_test(u'num_of_question_marks_on_avg', 0)
        self._generic_test(u'num_of_colons_on_avg', 0)
        self._generic_test(u'num_of_comas_on_avg', 0)
        self._generic_test(u'num_of_retweets_on_avg', 0)
        self._generic_test(u'num_of_ellipsis_on_avg', 0)
        self._generic_test(u'num_of_stop_words_on_avg', 0)
        self._generic_test(u'precent_of_stop_words_on_avg', 0)

    def test_only_signs_single_post(self):
        self._add_post(u'post1', u':/"-/",/')
        self._add_post(u'post2', u'RT /"you/"')
        self._add_post(u'post3', u'')

        self._set_up_test_env()

        self._generic_test(u'num_of_chars_on_avg', 17/3)
        self._generic_test(u'num_of_quotations_on_avg', 2/3)
        self._generic_test(u'num_of_uppercase_words_in_post_on_avg', 1/3)
        self._generic_test(u'number_of_precent_of_uppercased_posts', 0)
        self._generic_test(u'num_of_formal_words_on_avg', 1/3)
        self._generic_test(u'num_of_informal_words_on_avg', 2/3)
        self._generic_test(u'precent_of_formal_words_on_avg', 0.5/3)
        self._generic_test(u'num_of_question_marks_on_avg', 0)
        self._generic_test(u'num_of_colons_on_avg', 1/3)
        self._generic_test(u'num_of_comas_on_avg', 1/3)
        self._generic_test(u'num_of_retweets_on_avg', 1/3)
        self._generic_test(u'num_of_ellipsis_on_avg', 0)
        self._generic_test(u'num_of_stop_words_on_avg', 1/3)
        self._generic_test(u'precent_of_stop_words_on_avg', 0.5/3)

    def test_differance(self):
        self._add_post(u'are', u'you')
        self._add_post(u'ARE', u'YOU')
        self._set_up_test_env()

        self._generic_test(u'diffarence_num_of_characters', 0)
        self._generic_test(u'diffarence_num_of_verbse', -0.5)
        self._generic_test(u'diffarence_num_of_uppercase_words', 0)

    def test_ratio(self):
        self._add_post(u'THE funny ""', u':":" "-" FUN all bad leeroeqwy')

        self._set_up_test_env()
        self._generic_test(u'ratio_num_of_characters', 2.5)
        self._generic_test(u'ratio_num_of_verbse', 0)
        self._generic_test(u'ratio_num_of_nouns', 0)
        self._generic_test(u'ratio_num_of_adj', 1)
        self._generic_test(u'ratio_num_of_quotations', 2)
        self._generic_test(u'ratio_num_of_uppercase_words', 1)
        self._generic_test(u'ratio_num_of_foraml_words', 1.5)
        self._generic_test(u'ratio_num_of_informal_words', 3)
        self._generic_test(u'ratio_num_of_stopwords', 0)


    def _generic_test(self, attribute, expected_value):
        fixed_atter = u'Text_Anlalyser_Feature_Generator_'+attribute
        db_val = self._db.get_author_feature(self._author.author_guid, fixed_atter).attribute_value
        self.assertAlmostEqual(float(db_val), float(expected_value))

    def _set_up_test_env(self):
        self._db.session.commit()
        params = self._get_params()
        self._text_analysis_feature_generator = Text_Anlalyser_Feature_Generator(self._db, **params)
        self._text_analysis_feature_generator.execute()
        pass

    def _get_params(self):
        posts = {self._author.author_guid: self._posts}
        params = {'authors': [self._author], 'posts': posts}
        return params

    def _add_author(self, author_guid):
        author = Author()
        author.author_guid = author_guid
        author.author_full_name = u'test author'
        author.name = u'test'
        author.domain = u'tests'
        self._db.add_author(author)
        self._author = author

    def _add_post(self, title, content):
        post = Post()
        post.author = self._author.author_guid
        post.author_guid = self._author.author_guid
        post.content = content
        post.title = title
        post.domain = u'test'
        post.post_id = len(self._posts)
        post.guid = post.post_id
        self._db.addPost(post)
        self._posts.append(post)
