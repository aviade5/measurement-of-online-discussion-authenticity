# Written by Lior Bass 3/29/2018
# Written by Lior Bass 3/29/2018
import unittest

from DB.schema_definition import DB, Post, Author, date
from dataset_builder.feature_extractor.tf_idf_feature_generator import TF_IDF_Feature_Generator


class TF_IDF_Feature_Generator_Unittests(unittest.TestCase):
    def setUp(self):
        self._db = DB()
        self._db.setUp()
        self._domain = u'test'
        self._posts = []
        self._authors = []

    def tearDown(self):
        self._db.session.close()
        self._db.deleteDB()
        self._db.session.close()

    def execute_module(self):
        authors = self._authors
        posts = self._db.get_posts_by_domain('test')
        parameters = {"authors": authors, "posts": posts, "graphs": []}
        self._module = TF_IDF_Feature_Generator(self._db, **parameters)
        self._module._stemming = False
        self._module.execute(window_start=None)

    def test_tf_idf(self):
        self._add_author(u'1')
        text1 = 'this is a a sample'
        text2 = 'this is another another example example example'
        self._add_post('ta da', text1, '1')
        self._add_author(u'2')
        self._add_post('ta dddda', text2, '2')
        self.execute_module()
        self._module.clear_memo_dicts()
        module_result = self._module.tfidf('example', text2, [text1, text2], {})
        self.assertAlmostEqual(module_result, 0.13, places=2)
        self._module.clear_memo_dicts()
        module_result = self._module.tfidf('example', text1, [text1, text2], {})
        self.assertAlmostEqual(module_result, 0.0, places=2)
        self._module.clear_memo_dicts()
        self.assertAlmostEqual(self._module.tf('this', text1), 0.2)
        self._module.clear_memo_dicts()
        self.assertAlmostEqual(self._module.tf('this', text2), 0.14, places=1)

    def _add_author(self, author_guid):
        author = Author()
        author.author_guid = author_guid
        author.author_full_name = u'test author'
        author.name = u'test'
        author.domain = u'test'
        self._db.add_author(author)
        self._db.session.commit()
        self._authors.append(author)

    def _add_post(self, title, content, author_guid):
        post = Post()
        post.author = author_guid
        post.author_guid = author_guid
        post.content = content
        post.title = title
        post.domain = u'test'
        post.post_id = len(self._posts)
        post.guid = post.post_id
        post.date = date('2020-01-01 23:59:59')
        self._db.addPost(post)
        self._db.session.commit()
        self._posts.append(post)
