import csv
from unittest import TestCase

from DB.schema_definition import Author, Post, Claim_Tweet_Connection, DB, Claim
from commons.commons import convert_str_to_unicode_datetime
from preprocessing_tools.fake_news_word_classifier.fake_news_word_classifier import FakeNewsClassifier


class TestFakeNewsClassifier(TestCase):
    def setUp(self):
        self._db = DB()

        self._db.setUp()
        self._posts = []
        self._author = None

    def tearDown(self):
        self._db.session.close()

    def test_classify_by_dictionary_1_FN_1_FP(self):
        self._add_author(u'author')
        self._add_claim(u'post0', u'the claim', "2017-06-10 05:00:00", u'TRUE')
        self._add_post(u"post1", u"1 liar bad word joke", "2017-06-12 05:00:00")
        self._add_post(u"post2", u"no bad words untrue at all liar", "2017-06-12 05:00:00")
        self._add_post(u"post3", u"no joke bad words at all laugh", "2017-06-12 05:00:00")
        self._add_post(u"post4", u" liar no didnt actually bad words at all", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post0", u"post1")
        self._add_claim_tweet_connection(u"post0", u"post2")
        self._add_claim_tweet_connection(u"post0", u"post3")
        self._add_claim_tweet_connection(u"post0", u"post4")

        self._add_author(u'author_guid')
        self._add_claim(u'post5', u'the claim', "2017-06-10 05:00:00", u'FALSE')
        self._add_post(u"post6", u"1 bad word at all", "2017-06-12 05:00:00")
        self._add_post(u"post7", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_post(u"post8", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_post(u"post9", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post5", u"post6")
        self._add_claim_tweet_connection(u"post5", u"post7")
        self._add_claim_tweet_connection(u"post5", u"post8")
        self._add_claim_tweet_connection(u"post5", u"post9")
        self._db.session.commit()

        self.fake_news_feature_classifier = FakeNewsClassifier(self._db)
        self.fake_news_feature_classifier.setUp()
        self.fake_news_feature_classifier.execute()
        output_file_path = self.fake_news_feature_classifier._output_path + '/fake_news_classifier_results.csv'
        output_file = open(output_file_path, 'r')
        reader = csv.DictReader(output_file)
        output_data = reader.next()
        self.assertAlmostEqual(float(output_data['FN (think good but bad)']), 1)
        self.assertAlmostEqual(float(output_data['FP (think bad but good)']), 1)
        self.assertAlmostEqual(float(output_data['accuracy']), 0.0)
        self.assertAlmostEqual(float(output_data['AUC']), 0.0)

    def test_classify_by_dictionary_1_FN_1_FP_and_ignore_1(self):
        self._add_author(u'author')
        self._add_claim(u'post0', u'the claim', "2017-06-10 05:00:00", u'TRUE')
        self._add_post(u"post1", u"1 liar bad word joke", "2017-06-12 05:00:00")
        self._add_post(u"post2", u"no bad words untrue at all liar", "2017-06-12 05:00:00")
        self._add_post(u"post3", u"no joke bad words at all laugh", "2017-06-12 05:00:00")
        self._add_post(u"post4", u" liar no didnt actually bad words at all", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post0", u"post1")
        self._add_claim_tweet_connection(u"post0", u"post2")
        self._add_claim_tweet_connection(u"post0", u"post3")
        self._add_claim_tweet_connection(u"post0", u"post4")

        self._add_author(u'author_guid')
        self._add_claim(u'post5', u'the claim', "2017-06-10 05:00:00", u'FALSE')
        self._add_post(u"post6", u"1 bad word at all", "2017-06-12 05:00:00")
        self._add_post(u"post7", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_post(u"post8", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_post(u"post9", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post5", u"post6")
        self._add_claim_tweet_connection(u"post5", u"post7")
        self._add_claim_tweet_connection(u"post5", u"post8")
        self._add_claim_tweet_connection(u"post5", u"post9")

        self._add_claim(u'post10', u'the claim', "2017-06-10 05:00:00", u'unknown')
        self._add_post(u"post11", u"1 liar bad word joke", "2017-06-12 05:00:00")
        self._add_post(u"post12", u"no bad words untrue at all liar", "2017-06-12 05:00:00")
        self._add_post(u"post13", u"no joke bad words at all laugh", "2017-06-12 05:00:00")
        self._add_post(u"post14", u" liar no didnt actually bad words at all", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post10", u"post11")
        self._add_claim_tweet_connection(u"post10", u"post12")
        self._add_claim_tweet_connection(u"post10", u"post13")
        self._add_claim_tweet_connection(u"post10", u"post14")

        self._db.session.commit()

        self.fake_news_feature_classifier = FakeNewsClassifier(self._db)
        self.fake_news_feature_classifier.setUp()
        self.fake_news_feature_classifier.execute()
        output_file_path = self.fake_news_feature_classifier._output_path + '/fake_news_classifier_results.csv'
        output_file = open(output_file_path, 'r')
        reader = csv.DictReader(output_file)
        output_data = reader.next()
        self.assertAlmostEqual(float(output_data['FN (think good but bad)']), 1)
        self.assertAlmostEqual(float(output_data['FP (think bad but good)']), 1)
        self.assertAlmostEqual(float(output_data['accuracy']), 0.0)
        self.assertAlmostEqual(float(output_data['AUC']), 0.0)

    def test_classify_by_dictionary_0_FN_0_FP(self):
        self._add_author(u'author')
        self._add_claim(u'post0', u'the claim', "2017-06-10 05:00:00", u'FALSE')
        self._add_post(u"post1", u"1 liar bad word joke", "2017-06-12 05:00:00")
        self._add_post(u"post2", u"no bad words untrue at all liar", "2017-06-12 05:00:00")
        self._add_post(u"post3", u"no joke bad words at all laugh", "2017-06-12 05:00:00")
        self._add_post(u"post4", u" liar no didnt actually bad words at all", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post0", u"post1")
        self._add_claim_tweet_connection(u"post0", u"post2")
        self._add_claim_tweet_connection(u"post0", u"post3")
        self._add_claim_tweet_connection(u"post0", u"post4")

        self._add_author(u'author_guid')
        self._add_claim(u'post5', u'the claim', "2017-06-10 05:00:00", u'TRUE')
        self._add_post(u"post6", u"1 bad word at all", "2017-06-12 05:00:00")
        self._add_post(u"post7", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_post(u"post8", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_post(u"post9", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post5", u"post6")
        self._add_claim_tweet_connection(u"post5", u"post7")
        self._add_claim_tweet_connection(u"post5", u"post8")
        self._add_claim_tweet_connection(u"post5", u"post9")
        self._db.session.commit()

        self.fake_news_feature_classifier = FakeNewsClassifier(self._db)
        self.fake_news_feature_classifier.setUp()
        self.fake_news_feature_classifier.execute()
        output_file_path = self.fake_news_feature_classifier._output_path + '/fake_news_classifier_results.csv'
        output_file = open(output_file_path, 'r')
        reader = csv.DictReader(output_file)
        output_data = reader.next()
        self.assertAlmostEqual(float(output_data['FN (think good but bad)']), 0)
        self.assertAlmostEqual(float(output_data['FP (think bad but good)']), 0)
        self.assertAlmostEqual(float(output_data['accuracy']), 1.0)
        self.assertAlmostEqual(float(output_data['AUC']), 1.0)

    def test_classify_by_dictionary_1_FN_0_FP_3_claims(self):
        self._add_author(u'author')
        self._add_claim(u'post0', u'the claim', "2017-06-10 05:00:00", u'FALSE')
        self._add_post(u"post1", u"1 liar bad word joke", "2017-06-12 05:00:00")
        self._add_post(u"post2", u"no bad words untrue at all liar", "2017-06-12 05:00:00")
        self._add_post(u"post3", u"no joke bad words at all laugh", "2017-06-12 05:00:00")
        self._add_post(u"post4", u" liar no didnt actually bad words at all", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post0", u"post1")
        self._add_claim_tweet_connection(u"post0", u"post2")
        self._add_claim_tweet_connection(u"post0", u"post3")
        self._add_claim_tweet_connection(u"post0", u"post4")

        self._add_author(u'author_guid')
        self._add_claim(u'post5', u'the claim', "2017-06-10 05:00:00", u'TRUE')
        self._add_post(u"post6", u"1 bad word at all", "2017-06-12 05:00:00")
        self._add_post(u"post7", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_post(u"post8", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_post(u"post9", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post5", u"post6")
        self._add_claim_tweet_connection(u"post5", u"post7")
        self._add_claim_tweet_connection(u"post5", u"post8")
        self._add_claim_tweet_connection(u"post5", u"post9")

        self._add_author(u'author_guid')
        self._add_claim(u'post10', u'the claim', "2017-06-10 05:00:00", u'FALSE')
        self._add_post(u"post11", u"1 bad word at all", "2017-06-12 05:00:00")
        self._add_post(u"post12", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_post(u"post13", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_post(u"post14", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post10", u"post11")
        self._add_claim_tweet_connection(u"post10", u"post12")
        self._add_claim_tweet_connection(u"post10", u"post13")
        self._add_claim_tweet_connection(u"post10", u"post14")
        self._db.session.commit()

        self.fake_news_feature_classifier = FakeNewsClassifier(self._db)
        self.fake_news_feature_classifier.setUp()
        self.fake_news_feature_classifier.execute()
        output_file_path = self.fake_news_feature_classifier._output_path + '/fake_news_classifier_results.csv'
        output_file = open(output_file_path, 'r')
        reader = csv.DictReader(output_file)
        output_data = reader.next()
        self.assertAlmostEqual(float(output_data['FN (think good but bad)']), 1)
        self.assertAlmostEqual(float(output_data['FP (think bad but good)']), 0)
        self.assertAlmostEqual(float(output_data['accuracy']), 0.666666, places=4)
        self.assertAlmostEqual(float(output_data['AUC']), 0.75)

    def test_classify_by_dictionary_0_FN_1_FP_3_claims(self):
        self._add_author(u'author')
        self._add_claim(u'post0', u'the claim', "2017-06-10 05:00:00", u'FALSE')
        self._add_post(u"post1", u"1 liar bad word joke", "2017-06-12 05:00:00")
        self._add_post(u"post2", u"no bad words untrue at all liar", "2017-06-12 05:00:00")
        self._add_post(u"post3", u"no joke bad words at all laugh", "2017-06-12 05:00:00")
        self._add_post(u"post4", u" liar no didnt actually bad words at all", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post0", u"post1")
        self._add_claim_tweet_connection(u"post0", u"post2")
        self._add_claim_tweet_connection(u"post0", u"post3")
        self._add_claim_tweet_connection(u"post0", u"post4")

        self._add_claim(u'post5', u'the claim', "2017-06-10 05:00:00", u'TRUE')
        self._add_post(u"post6", u"1 bad word at all", "2017-06-12 05:00:00")
        self._add_post(u"post7", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_post(u"post8", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_post(u"post9", u"no bad words at all", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post5", u"post6")
        self._add_claim_tweet_connection(u"post5", u"post7")
        self._add_claim_tweet_connection(u"post5", u"post8")
        self._add_claim_tweet_connection(u"post5", u"post9")

        self._add_claim(u'post10', u'the claim', "2017-06-10 05:00:00", u'TRUE')
        self._add_post(u"post11", u"1 liar bad word joke", "2017-06-12 05:00:00")
        self._add_post(u"post12", u"no bad words untrue at all liar", "2017-06-12 05:00:00")
        self._add_post(u"post13", u"no joke bad words at all laugh", "2017-06-12 05:00:00")
        self._add_post(u"post14", u" liar no didnt actually bad words at all", "2017-06-12 05:00:00")
        self._add_claim_tweet_connection(u"post10", u"post11")
        self._add_claim_tweet_connection(u"post10", u"post12")
        self._add_claim_tweet_connection(u"post10", u"post13")
        self._add_claim_tweet_connection(u"post10", u"post14")
        self._db.session.commit()

        self.fake_news_feature_classifier = FakeNewsClassifier(self._db)
        self.fake_news_feature_classifier.setUp()
        self.fake_news_feature_classifier.execute()
        output_file_path = self.fake_news_feature_classifier._output_path + '/fake_news_classifier_results.csv'
        output_file = open(output_file_path, 'r')
        reader = csv.DictReader(output_file)
        output_data = reader.next()
        self.assertAlmostEqual(float(output_data['FN (think good but bad)']), 0)
        self.assertAlmostEqual(float(output_data['FP (think bad but good)']), 1)
        self.assertAlmostEqual(float(output_data['accuracy']), 0.666666, places=4)
        self.assertAlmostEqual(float(output_data['AUC']), 0.75)

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
