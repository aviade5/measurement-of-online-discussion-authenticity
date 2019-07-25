from unittest import TestCase
import pandas as pd
from DB.schema_definition import *
from preprocessing_tools.table_to_csv_exporter import TableToCsvExporter


class TestTableToCsvExporter(TestCase):
    def setUp(self):
        self._db = DB()

        self._db.setUp()
        self._posts = []
        self._author = None

    def tearDown(self):
        self._db.session.close()

    def test_export_tables_empty_db(self):
        self._table_to_csv_exporter = TableToCsvExporter(self._db)
        self._table_to_csv_exporter.setUp()
        self._table_to_csv_exporter.execute()
        csv_path = self._table_to_csv_exporter._output_path
        posts_table_name = self._table_to_csv_exporter._posts_table_name + '.csv'
        authors_table_name = self._table_to_csv_exporter._authors_table_name + '.csv'
        claims_table_name = self._table_to_csv_exporter._claims_table_name + '.csv'
        claim_tweet_connection_table_name = self._table_to_csv_exporter._claim_tweet_connection_table_name + '.csv'

        self._assert_table_rows_count(0, csv_path + posts_table_name)
        self._assert_table_rows_count(0, csv_path + authors_table_name)
        self._assert_table_rows_count(0, csv_path + claims_table_name)
        self._assert_table_rows_count(0, csv_path + claim_tweet_connection_table_name)

    def test_export_tables_db_with_data(self):
        self._add_author(u'a1')
        self._add_post(u'p1', u'tweet1', domain=u'Microblog')
        self._add_post(u'p2', u'tweet2', domain=u'Microblog')
        self._add_post(u'p3', u'tweet3', domain=u'Microblog')
        self._add_claim(u'c1', u'claim1 title', u'description1')

        self._add_author(u'a2')
        self._add_post(u'p5', u'tweet1', domain=u'Microblog')
        self._add_post(u'p6', u'tweet2', domain=u'Microblog')
        self._add_claim(u'c2', u'claim2 title', u'description2')

        self._add_claim_tweet_connection(u'p4', u'p1')
        self._add_claim_tweet_connection(u'p4', u'p2')
        self._add_claim_tweet_connection(u'p7', u'p3')
        self._add_claim_tweet_connection(u'p7', u'p5')
        self._add_claim_tweet_connection(u'p7', u'p6')

        self._db.commit()

        self._table_to_csv_exporter = TableToCsvExporter(self._db)
        self._table_to_csv_exporter.setUp()
        self._table_to_csv_exporter.execute()
        csv_path = self._table_to_csv_exporter._output_path
        posts_table_name = self._table_to_csv_exporter._posts_table_name + '.csv'
        authors_table_name = self._table_to_csv_exporter._authors_table_name + '.csv'
        claims_table_name = self._table_to_csv_exporter._claims_table_name + '.csv'
        claim_tweet_connection_table_name = self._table_to_csv_exporter._claim_tweet_connection_table_name + '.csv'

        self._assert_table_rows_count(5, csv_path + posts_table_name)
        self._assert_table_rows_count(2, csv_path + authors_table_name)
        self._assert_table_rows_count(2, csv_path + claims_table_name)
        self._assert_table_rows_count(5, csv_path + claim_tweet_connection_table_name)

    def _assert_table_rows_count(self, rows_count, table_path):
        table = pd.DataFrame.from_csv(table_path)
        self.assertEqual(len(table), rows_count)

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

    def _add_post(self, post_id, content, date_str=u"2017-06-14 05:00:00", domain=u'Microblog', post_type=None):
        post = Post()
        post.author = self._author.author_guid
        post.author_guid = self._author.author_guid
        post.content = content
        post.title = post_id
        post.domain = domain
        post.post_id = post_id
        post.guid = post.post_id
        post.date = convert_str_to_unicode_datetime(date_str)
        post.created_at = post.date
        post.post_type = post_type
        self._db.addPost(post)
        self._posts.append(post)

    def _add_claim(self, claim_id, title, description=u'd', keywords=u'key', verdict=u'false',
                   verdict_date=u'2017-06-14 05:00:00'):
        claim = Claim()
        claim.claim_id = claim_id
        claim.title = title
        claim.description = description
        claim.guid = claim_id
        claim.url = u'url'
        claim.keywords = keywords
        claim.author = self._author.author_guid
        claim.verdict = verdict
        claim.verdict_date = convert_str_to_unicode_datetime(verdict_date)
        self._db.addPost(claim)

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
