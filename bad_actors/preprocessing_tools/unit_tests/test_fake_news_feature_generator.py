from unittest import TestCase

from DB.schema_definition import Author, Post, Claim_Tweet_Connection, DB, Claim
from commons.commons import convert_str_to_unicode_datetime
from preprocessing_tools.fake_news_word_classifier.fake_news_feature_generator import FakeNewsFeatureGenerator


class TestFakeNewsFeatureGenerator(TestCase):

    def setUp(self):
        self._db = DB()

        self._db.setUp()
        self._posts = []
        self._author = None

    def tearDown(self):
        self._db.session.close()

    def test_get_word_count_1_claim_1_comments_no_words(self):
        self._add_author(u'author_guid')
        self._add_claim(u'post0', u'the claim', "2017-06-10 05:00:00")
        self._add_post(u"post1", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post0", u"post1")
        self._db.session.commit()

        self.fake_news_feature_generator = FakeNewsFeatureGenerator(self._db)
        self.fake_news_feature_generator.setUp()
        self.fake_news_feature_generator.execute()

        self.assert_word_dictionary_count(u'post0', {})

        self.assert_word_dictionary_fraction(u'post0', {})

        author_feature = self._db.get_author_feature(u'post0', u'FakeNewsFeatureGenerator_words_count_sum')
        self.assertEqual(u'0', author_feature.attribute_value)
        author_feature = self._db.get_author_feature(u'post0', u'FakeNewsFeatureGenerator_words_fraction_sum')
        self.assertEqual(u'0.0', author_feature.attribute_value)

    def test_get_word_count_1_claim_1_comments_1_words(self):
        self._add_author(u'author_guid')
        self._add_claim(u'post0', u'the claim', "2017-06-10 05:00:00")
        self._add_post(u"post1", u"1 bad word liar", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post0", u"post1")
        self._db.session.commit()

        self.fake_news_feature_generator = FakeNewsFeatureGenerator(self._db)
        self.fake_news_feature_generator.setUp()
        self.fake_news_feature_generator.execute()

        self.assert_word_dictionary_count(u'post0', {u'liar': u'1'})

        self.assert_word_dictionary_fraction(u'post0', {u'liar': u'1.0'})

        author_feature = self._db.get_author_feature(u'post0', u'FakeNewsFeatureGenerator_words_count_sum')
        self.assertEqual(u'1', author_feature.attribute_value)
        author_feature = self._db.get_author_feature(u'post0', u'FakeNewsFeatureGenerator_words_fraction_sum')
        self.assertEqual(u'1.0', author_feature.attribute_value)

    def test_get_word_count_1_claim_4_comments_1_words(self):
        self._add_author(u'author_guid')
        self._add_claim(u'post0', u'the claim', "2017-06-10 05:00:00")
        self._add_post(u"post1", u"1 bad word liar", "2017-06-12 05:00:00")
        self._add_post(u"post2", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_post(u"post3", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_post(u"post4", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post0", u"post1")
        self._add_claim_tweet_connection(u"post0", u"post2")
        self._add_claim_tweet_connection(u"post0", u"post3")
        self._add_claim_tweet_connection(u"post0", u"post4")
        self._db.session.commit()

        self.fake_news_feature_generator = FakeNewsFeatureGenerator(self._db)
        self.fake_news_feature_generator.setUp()
        self.fake_news_feature_generator.execute()

        self.assert_word_dictionary_count(u'post0', {u'liar': u'1'})

        self.assert_word_dictionary_fraction(u'post0', {u'liar': u'0.25'})

        author_feature = self._db.get_author_feature(u'post0', u'FakeNewsFeatureGenerator_words_count_sum')
        self.assertEqual(u'1', author_feature.attribute_value)
        author_feature = self._db.get_author_feature(u'post0', u'FakeNewsFeatureGenerator_words_fraction_sum')
        self.assertEqual(u'0.25', author_feature.attribute_value)

    def test_get_word_count_1_claim_4_comments_8_words(self):
        self._add_author(u'author')
        self._add_claim(u'post0', u'the claim', "2017-06-10 05:00:00")
        self._add_post(u"post1", u"1 liar bad word liar", "2017-06-12 05:00:00")
        self._add_post(u"post2", u"no bad words liar at all liar", "2017-06-12 05:00:00")
        self._add_post(u"post3", u"no liar bad words at all liar", "2017-06-12 05:00:00")
        self._add_post(u"post4", u" liar no liar bad words at all", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post0", u"post1")
        self._add_claim_tweet_connection(u"post0", u"post2")
        self._add_claim_tweet_connection(u"post0", u"post3")
        self._add_claim_tweet_connection(u"post0", u"post4")
        self._db.session.commit()

        self.fake_news_feature_generator = FakeNewsFeatureGenerator(self._db)
        self.fake_news_feature_generator.setUp()
        self.fake_news_feature_generator.execute()

        self.assert_word_dictionary_count(u'post0', {u'liar': u'8'})

        self.assert_word_dictionary_fraction(u'post0', {u'liar': u'2.0'})

        author_feature = self._db.get_author_feature(u'post0', u'FakeNewsFeatureGenerator_words_count_sum')
        self.assertEqual(u'8', author_feature.attribute_value)
        author_feature = self._db.get_author_feature(u'post0', u'FakeNewsFeatureGenerator_words_fraction_sum')
        self.assertEqual(u'2.0', author_feature.attribute_value)

    def test_get_word_count_1_claim_4_comments_8_different_words(self):
        self._add_author(u'author')
        self._add_claim(u'post0', u'the claim', "2017-06-10 05:00:00")
        self._add_post(u"post1", u"1 liar bad word joke", "2017-06-12 05:00:00")
        self._add_post(u"post2", u"no bad words untrue at all liar", "2017-06-12 05:00:00")
        self._add_post(u"post3", u"no joke bad words at all laugh", "2017-06-12 05:00:00")
        self._add_post(u"post4", u" liar no didnt actually bad words at all", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post0", u"post1")
        self._add_claim_tweet_connection(u"post0", u"post2")
        self._add_claim_tweet_connection(u"post0", u"post3")
        self._add_claim_tweet_connection(u"post0", u"post4")
        self._db.session.commit()

        self.fake_news_feature_generator = FakeNewsFeatureGenerator(self._db)
        self.fake_news_feature_generator.setUp()
        self.fake_news_feature_generator.execute()

        self.assert_word_dictionary_count(u'post0',
                                          {u'liar': u'3', u'joke': u'2', u'didnt actually': u'1', u'untrue': u'1',
                                           u'laugh': u'1'})

        self.assert_word_dictionary_fraction(u'post0', {u'liar': u'0.75', u'joke': u'0.5', u'didnt actually': u'0.25',
                                                        u'untrue': u'0.25', u'laugh': u'0.25'})

        author_feature = self._db.get_author_feature(u'post0', u'FakeNewsFeatureGenerator_words_count_sum')
        self.assertEqual(u'3', author_feature.attribute_value)
        author_feature = self._db.get_author_feature(u'post0', u'FakeNewsFeatureGenerator_words_fraction_sum')
        self.assertEqual(u'0.75', author_feature.attribute_value)

    def test_get_claim_type_4_claim(self):
        self._add_author(u'author_guid')
        self._add_claim(u'post0', u'the claim', "2017-06-10 05:00:00")
        self._add_claim(u'post1', u'the claim', "2017-06-10 05:00:00", 'FALSE')
        self._add_claim(u'post2', u'the claim', "2017-06-10 05:00:00", 'pants-fire')
        self._add_claim(u'post3', u'the claim', "2017-06-10 05:00:00", 'mostly-false')
        self._add_claim(u'post4', u'the claim', "2017-06-10 05:00:00", 'TRUE')
        self._add_claim(u'post5', u'the claim', "2017-06-10 05:00:00", 'mostly-true')
        self._add_claim(u'post6', u'the claim', "2017-06-10 05:00:00", 'half_true')
        self._add_claim(u'post7', u'the claim', "2017-06-10 05:00:00", 'unproven')
        self._db.session.commit()

        self.fake_news_feature_generator = FakeNewsFeatureGenerator(self._db)
        self.fake_news_feature_generator._domain = u'Claim'
        self.fake_news_feature_generator.setUp()
        self.fake_news_feature_generator.execute()

        author_feature = self._db.get_author_feature(u'post0', u'FakeNewsFeatureGenerator_claim_verdict')
        self.assertEqual(None, author_feature)
        author_feature = self._db.get_author_feature(u'post1', u'FakeNewsFeatureGenerator_claim_verdict')
        self.assertEqual(u'False', author_feature.attribute_value)
        author_feature = self._db.get_author_feature(u'post2', u'FakeNewsFeatureGenerator_claim_verdict')
        self.assertEqual(u'False', author_feature.attribute_value)
        author_feature = self._db.get_author_feature(u'post3', u'FakeNewsFeatureGenerator_claim_verdict')
        self.assertEqual(u'False', author_feature.attribute_value)
        author_feature = self._db.get_author_feature(u'post4', u'FakeNewsFeatureGenerator_claim_verdict')
        self.assertEqual(u'True', author_feature.attribute_value)
        author_feature = self._db.get_author_feature(u'post5', u'FakeNewsFeatureGenerator_claim_verdict')
        self.assertEqual(u'True', author_feature.attribute_value)
        author_feature = self._db.get_author_feature(u'post6', u'FakeNewsFeatureGenerator_claim_verdict')
        self.assertEqual(None, author_feature)
        author_feature = self._db.get_author_feature(u'post7', u'FakeNewsFeatureGenerator_claim_verdict')
        self.assertEqual(None, author_feature)

    def assert_word_dictionary_count(self, author_guid, values):
        self.assert_dictionary_words(author_guid, u'FakeNewsFeatureGenerator_{0}_count', u'0', values)

    def assert_word_dictionary_fraction(self, author_guid, values):
        self.assert_dictionary_words(author_guid, u'FakeNewsFeatureGenerator_{0}_fraction', u'0.0', values)

    def assert_dictionary_words(self, author_guid, count_template, default_value, values):
        fake_news_dictionary_words = self.fake_news_feature_generator._fake_news_dictionary
        for word in fake_news_dictionary_words:
            word = word.strip().replace(' ', '-')

            author_feature = self._db.get_author_feature(author_guid, count_template.format(word))
            if word in values:
                self.assertEqual(values[word], author_feature.attribute_value)
            else:
                self.assertEqual(default_value, author_feature.attribute_value)

    def _add_author(self, author_guid):
        author = Author()
        author.author_guid = author_guid
        author.author_full_name = u'test author'
        author.author_screen_name = author_guid
        author.name = u'test'
        author.domain = u'tests'
        author.statuses_count = 0
        author.created_at = u"2017-06-14 05:00:00"
        self._db.add_author(author)
        self._author = author

    def _add_post(self, title, content, date_str, domain=u'Microblog', post_type=None):
        post = Post()
        post.author = self._author.author_guid
        post.author_guid = self._author.author_guid
        post.content = content
        post.title = title
        post.domain = domain
        post.post_id = title
        post.guid = post.post_id
        post.date = convert_str_to_unicode_datetime(date_str)
        post.created_at = post.date
        post.post_type = post_type
        self._db.addPost(post)
        self._posts.append(post)


    def _get_params(self):
        posts = {self._author.author_guid: self._posts}
        params = params = {'authors': [self._author], 'posts': posts}
        return params

    def _add_claim_tweet_connection(self, claim_id, post_id):
        connection = Claim_Tweet_Connection()
        connection.claim_id = claim_id
        connection.post_id = post_id
        self._db.add_claim_connections([connection])
        pass

    def _add_claim(self, claim_id, content, date_str, post_type=None):
        claim = Claim()
        claim.claim_id = claim_id
        claim.verdict = post_type
        claim.title = claim_id
        claim.description = content
        claim.verdict_date = convert_str_to_unicode_datetime(date_str)
        claim.url = u"claim url"
        self._db.addPost(claim)
