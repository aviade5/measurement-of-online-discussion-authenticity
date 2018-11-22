import unittest
from DB.schema_definition import *
from dataset_builder.feature_extractor.sentiment_feature_generator import Sentiment_Feature_Generator


class Sentiment_Feature_Generator_Unittest(unittest.TestCase):
    def setUp(self):
        self._config_parser = getConfig()
        self._db = DB()
        self._db.setUp()

        self._posts = []
        self._add_author(u'this is a test author')

    def tearDown(self):
        self._db.session.close()
        self._db.deleteDB()
        self._db.session.close()

    def test_compund_single_positive_post(self):
        self._add_post(u'Title', u'I like this thing very MUCH')
        self._set_up_test_env()
        db_value = self._get_author_compund_val()
        self.assertGreater(db_value, 0)

    def test_compund_multiple_positive_posts(self):
        self._add_post(u'post1', u'I like this thing very MUCH')
        self._add_post(u'post2', u'This thing is very nice and cozy')
        self._add_post(u'post3', u'This big fluffy thing is the greatest')
        self._set_up_test_env()
        db_value = self._get_author_compund_val()
        self.assertGreater(db_value, 0)

    def test_compund_single_negative_post(self):
        self._add_post(u'Title', u'This thing sucks, it is really really bad')
        self._set_up_test_env()
        db_value = self._get_author_compund_val()
        self.assertLess(db_value, 0)

    def test_compund_multiple_positive_posts2(self):
        self._add_post(u'post1', u'you are the worst ever')
        self._add_post(u'post2', u'the taste was awful ')
        self._add_post(u'post3', u'the quick fast fox jumps over the lazy dog, which is a repulsive way to say ew')
        self._set_up_test_env()
        db_value = self._get_author_compund_val()
        self.assertLess(db_value, 0)

    def test_single_post_with_no_context(self):
        self._add_post(u'post1', u'')
        self._set_up_test_env()
        db_value = self._get_author_compund_val()
        self.assertEquals(db_value, 0.0)

    def test_multiple_posts_with_no_context(self):
        self._add_post(u'post1', u'')
        self._add_post(u'post2', None)
        self._add_post(u'post3', u'')
        self._set_up_test_env()
        db_value = self._get_author_compund_val()
        self.assertEquals(db_value, 0.0)

    def test_multiple_posts_with_multiple_meanings(self):
        self._add_post(u'post1', u'ew this thing sucks')
        self._add_post(u'post2', u'its very nice of you to do this thing')
        self._set_up_test_env()
        pos_val = self._get_authors_positive_val()
        neg_val = self._get_authors_negative_val()
        self.assertGreater(pos_val, 0.0)
        self.assertGreater(neg_val, 0.0)

    def test_single_post_with_positive_and_negative_meaning(self):
        self._add_post(u'post1', u'I hope you are right, you are correct, this thing is plain bad')
        self._set_up_test_env()
        pos_val = self._get_authors_positive_val()
        neg_val = self._get_authors_negative_val()
        self.assertGreater(pos_val, 0.0)
        self.assertGreater(neg_val, 0.0)

    def test_post_with_signs_no_words(self):
        self._add_post(u'post1', u'/.,')
        self._add_post(u'post2', u'------')
        self._add_post(u'post3', u'34556')
        self._add_post(u'post4', u'3-5')
        self._add_post(u'post5', u'F4 asdjgjkfk')
        self._set_up_test_env()
        comp_value = self._get_author_compund_val()
        pos_val = self._get_authors_positive_val()
        neg_val = self._get_authors_negative_val()
        self.assertEquals(comp_value, 0.0)
        self.assertEquals(pos_val, 0.0)
        self.assertEquals(neg_val, 0.0)

    def test_compund_multiple_positive_posts_represent_by_post(self):
        self._add_post(u'post1', u'I like this thing very MUCH', u'Claim')
        self._add_post(u'post2', u'This thing is very nice and cozy')
        self._add_post(u'post3', u'This big fluffy thing is the greatest')
        self._add_claim_tweet_connection(u'post1', u'post2')
        self._add_claim_tweet_connection(u'post1', u'post3')
        params = self._get_params()
        self._sentiment_anlysis = Sentiment_Feature_Generator(self._db, **params)
        self._sentiment_anlysis.execute()
        db_value = self._db.get_author_feature(self._author.author_guid,
                                             u'Sentiment_Feature_Generator_authors_posts_semantic_average_compound')
        self.assertGreater(db_value, 0)

    def _set_up_test_env(self):
        self._db.session.commit()
        params = self._get_params()
        self._sentiment_anlysis = Sentiment_Feature_Generator(self._db, **params)
        self._sentiment_anlysis.execute()

    def _get_params(self):
        posts = {self._author.author_guid: self._posts}
        params = {'authors': [self._author], 'posts': posts}
        return params

    def _get_author_compund_val(self):
        db_val = self._db.get_author_feature(self._author.author_guid,
                                             u'Sentiment_Feature_Generator_authors_posts_semantic_average_compound')
        return float(db_val.attribute_value)

    def _get_authors_positive_val(self):
        db_val = self._db.get_author_feature(self._author.author_guid,
                                             u'Sentiment_Feature_Generator_authors_posts_semantic_average_positive')
        return float(db_val.attribute_value)

    def _get_authors_negative_val(self):
        db_val = self._db.get_author_feature(self._author.author_guid,
                                             u'Sentiment_Feature_Generator_authors_posts_semantic_average_negative')
        return float(db_val.attribute_value)

    def _get_authors_neutral_val(self):
        db_val = self._db.get_author_feature(self._author.author_guid,
                                             u'Sentiment_Feature_Generator_authors_posts_semantic_average_neutral')
        return db_val

    def _add_author(self, author_guid):
        author = Author()
        author.author_guid = author_guid
        author.author_full_name = u'test author'
        author.name = u'test'
        author.domain = u'Microblog'
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
        post.date = str_to_date(u"2018-04-12 10:32:10")
        post.guid = post.post_id
        self._db.addPost(post)
        self._posts.append(post)

    def _add_claim_tweet_connection(self, claim_id, post_id):
        connection = Claim_Tweet_Connection()
        connection.claim_id = claim_id
        connection.post_id = post_id
        self._db.add_claim_connections([connection])
        pass
