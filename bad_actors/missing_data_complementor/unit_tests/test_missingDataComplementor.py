from unittest import TestCase

from DB.schema_definition import *
from configuration.config_class import getConfig
from missing_data_complementor.missing_data_complementor import MissingDataComplementor


class MissingDataComplemntorTests(TestCase):
    def setUp(self):
        self.config = getConfig()
        self._db = DB()
        self._db.setUp()
        self._minimal_num_of_posts = self.config.eval("MissingDataComplementor", "minimal_num_of_posts")
        self._missing_data_complemntor = MissingDataComplementor(self._db)

        self._author_guid1 = u'64205a8170453edc8cf5a9f316116573'
        author = Author()
        author.name = u'BillGates'
        author.domain = u'Microblog'
        author.protected = 0
        author.author_guid = self._author_guid1
        author.author_screen_name = u'BillGates'
        author.author_full_name = u'Bill Gates'
        author.statuses_count = 10
        author.author_osn_id = 149159975
        author.followers_count = 12
        author.created_at = datetime.datetime.strptime('2016-04-02 00:00:00', '%Y-%m-%d %H:%M:%S')
        author.missing_data_complementor_insertion_date = datetime.datetime.now()
        author.xml_importer_insertion_date = datetime.datetime.now()
        self._db.add_author(author)

        for i in range(10):
            post = Post()
            post.post_id = u'TestPost' + str(i)
            post.author = u'BillGates'
            post.guid = u'TestPost' + str(i)
            post.url = u'TestPost' + str(i)
            tempDate = u'2016-05-05 00:00:00'
            day = datetime.timedelta(1)
            post.date = datetime.datetime.strptime(tempDate, '%Y-%m-%d %H:%M:%S') + day * i
            post.domain = u'Microblog'
            post.author_guid = self._author_guid1
            post.content = u"InternetTV love it #wow"
            post.xml_importer_insertion_date = datetime.datetime.now()
            self._db.addPost(post)

        self._author_guid2 = u'3824f889f6e435c7bbd482faf0b6d2de'
        author = Author()
        author.name = u'ZachServideo'
        author.domain = u'Microblog'
        author.protected = 0
        author.author_guid = self._author_guid2
        author.author_screen_name = u'ZachServideo'
        author.author_full_name = u'Zach Servideo'
        author.statuses_count = 110
        author.author_osn_id = 40291482
        author.created_at = datetime.datetime.strptime('2016-04-05 00:00:00', '%Y-%m-%d %H:%M:%S')
        author.missing_data_complementor_insertion_date = datetime.datetime.now()
        author.xml_importer_insertion_date = datetime.datetime.now()
        author.friends_count = 12
        self._db.add_author(author)

        for i in range(10, 120):
            post = Post()
            post.post_id = u'TestPost' + str(i)
            post.author = u'ZachServideo'
            post.guid = u'TestPost' + str(i)
            post.url = u'TestPost' + str(i)
            tempDate = u'2016-05-05 00:00:00'
            day = datetime.timedelta(1)
            post.date = datetime.datetime.strptime(tempDate, '%Y-%m-%d %H:%M:%S') + day * i
            post.domain = u'Microblog'
            post.author_guid = self._author_guid2
            post.content = u'OnlineTV love it http://google.com'
            post.xml_importer_insertion_date = datetime.datetime.now()
            self._db.addPost(post)

        self._author_guid3 = u'3ac92c5a478b3bf79a6f72b947aacbee'
        author = Author()
        self._author_3_name = u'AyexBee'
        author.name = self._author_3_name
        author.domain = u'Microblog'
        author.protected = 0
        author.author_guid = self._author_guid3
        author.author_screen_name = u'AyexBee'
        author.author_full_name = u'AxB'
        author.statuses_count = 100
        author.author_osn_id = 100271909
        author.created_at = datetime.datetime.strptime('2016-04-05 00:00:00', '%Y-%m-%d %H:%M:%S')
        author.missing_data_complementor_insertion_date = datetime.datetime.now()
        author.xml_importer_insertion_date = datetime.datetime.now()
        author.followers_count = 12
        self._db.add_author(author)

        for i in range(120, 220):
            post = Post()
            post.post_id = u'TestPost' + str(i)
            post.author = u'AyexBee'
            post.guid = u'TestPost' + str(i)
            post.url = u'TestPost' + str(i)
            tempDate = u'2016-05-05 00:00:00'
            day = datetime.timedelta(1)
            post.date = datetime.datetime.strptime(tempDate, '%Y-%m-%d %H:%M:%S') + day * i
            post.domain = u'Microblog'
            post.author_guid = self._author_guid3
            post.content = u'Security love it http://google.com'
            post.xml_importer_insertion_date = datetime.datetime.now()
            self._db.addPost(post)

        # Author for test_fill_data_for_sources
        author = Author()
        author.name = u"philkernick"
        author.domain = u'Microblog'
        author.protected = 0
        author.author_guid = u"8b06558548043d97903ab5b5c87bf254"
        author.author_screen_name = u"philkernick"
        author.author_full_name = u"Phil Kernick"
        author.xml_importer_insertion_date = datetime.datetime.now()
        self._db.add_author(author)

        # Author for test_mark_suspended_from_twitter
        author = Author()
        author.name = u"dianisimova7642"
        author.domain = u'Microblog'
        author.protected = 0
        self._suspended_author_guid = compute_author_guid_by_author_name(author.name).replace("-", "")
        author.author_guid = self._suspended_author_guid
        author.author_screen_name = author.name
        author.xml_importer_insertion_date = datetime.datetime.now()
        self._db.add_author(author)

        # Author for assign_manually_labeled_authors
        author = Author()
        author.name = u"muloduro"
        author.domain = u'Microblog'
        author.protected = 0
        self._private_author_guid = u"6343dc3298343d4780f6242dd553a2fd"
        author.author_guid = self._private_author_guid
        author.author_screen_name = u"no_name"
        author.author_full_name = u"no_name"
        author.xml_importer_insertion_date = datetime.datetime.now()
        self._db.add_author(author)

        # Post for test_fill_tweet_retweet_connection
        post = Post()
        post.post_id = u'TestPost'
        post.author = u"philkernick"
        post.guid = u'8b065585-4804-3d97-903a-b5b5c87bf254'
        post.url = u'http://twitter.com/philkernick/statuses/799525918083579904'
        tempDate = u'2016-05-05 00:00:00'
        day = datetime.timedelta(1)
        post.date = datetime.datetime.strptime(tempDate, '%Y-%m-%d %H:%M:%S') + day * 1
        post.domain = u'Microblog'
        post.author_guid = u"8b06558548043d97903ab5b5c87bf254"
        post.title = u"RT @virturity: I noticed there is no good visualization of the real Information Security triad, so i made one. You're welcome. #infosec htt"
        post.content = u"RT @virturity: I noticed there is no good visualization of the real &lt;em&gt;Information&lt;/em&gt; &lt;em&gt;Security&lt;/em&gt; triad, so i made one. You're welcome. #infosec htt"
        post.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post)

        # For fill_data_for_followers test
        author_connections = []

        author_connection_1_2 = AuthorConnection()
        author_connection_1_2.source_author_guid = self._author_guid1
        author_connection_1_2.destination_author_guid = self._author_guid2
        author_connection_1_2.connection_type = u"friend"
        author_connection_1_2.weight = 1.0
        author_connection_1_2.insertion_date = datetime.datetime.now()
        author_connections.append(author_connection_1_2)

        author_connection_1_3 = AuthorConnection()
        author_connection_1_3.source_author_guid = self._author_guid1
        author_connection_1_3.destination_author_guid = self._author_guid3
        author_connection_1_3.connection_type = u"follower"
        author_connection_1_3.weight = 1.0
        author_connection_1_3.insertion_date = datetime.datetime.now()
        author_connections.append(author_connection_1_3)

        self._db.add_author_connections(author_connections)

        self._db.commit()

    def tearDown(self):
        self._db.session.close_all()
        self._db.deleteDB()
        self._db.session.close()

    def test_fill_timeline_of_author_below_minimal_posts(self):
        posts_of_author1 = self._db.get_posts_by_author_guid(self._author_guid1)
        self.assertEqual(len(posts_of_author1), 10)
        authors_for_timline_fill = self._db.get_author_screen_names_and_number_of_posts(self._minimal_num_of_posts)
        self._missing_data_complemntor.fill_authors_time_line()
        posts_of_author1 = self._db.get_posts_by_author_guid(self._author_guid1)
        self.assertEqual(len(posts_of_author1), 100)
        self.assertEqual(len(authors_for_timline_fill), 1)
        self._db.session.close()

    def test_fill_timeline_of_author_equal_minimal_posts(self):
        posts_of_author3 = self._db.get_posts_by_author_guid(self._author_guid3)
        self.assertEqual(len(posts_of_author3), 100)
        self._missing_data_complemntor.fill_authors_time_line()
        posts_of_author3 = self._db.get_posts_by_author_guid(self._author_guid3)
        self.assertEqual(len(posts_of_author3), 100)
        self._db.session.close()

    def test_fill_timeline_of_author_higher_minimal_posts(self):
        posts_of_author2 = self._db.get_posts_by_author_guid(self._author_guid2)
        self.assertEqual(len(posts_of_author2), 110)
        self._missing_data_complemntor.fill_authors_time_line()
        posts_of_author2 = self._db.get_posts_by_author_guid(self._author_guid2)
        self.assertEqual(len(posts_of_author2), 110)
        self._db.session.close()

    def test_fill_data_for_sources(self):
        self._missing_data_complemntor.fill_data_for_sources()
        author = self._db.get_author_by_author_guid(u"8b06558548043d97903ab5b5c87bf254")[0]
        self.assertNotEqual(author.author_osn_id, None)
        self.assertEqual(author.missing_data_complementor_insertion_date,
                         date_to_str(self._missing_data_complemntor._window_start))
        self.assertNotEqual(author.description, None)
        self._db.session.close()

    def test_fill_tweet_retweet_connection(self):
        self._missing_data_complemntor.fill_tweet_retweet_connection()
        author_guid = compute_author_guid_by_author_name(u'virturity').replace('-', '')
        post = self._db.get_posts_by_author_guid(author_guid)[0]
        self.assertNotEqual(post.original_tweet_importer_insertion_date, None)
        self.assertEqual(post.author, u'virturity')
        self.assertEqual(post.author_guid, author_guid)
        self._db.session.close()

    def test_fill_data_for_followers(self):
        self._missing_data_complemntor.fill_data_for_followers()
        followers_from_missing_data_complementor = self._db.get_author_connections_by_author_guid(self._author_guid3)
        followers_from_twitter = self._missing_data_complemntor._social_network_crawler._twitter_api_requester.get_follower_ids_by_screen_name(
            self._author_3_name)
        self.assertEqual(len(followers_from_missing_data_complementor), len(followers_from_twitter))
        self._db.session.close()

    def test_fill_data_for_friends(self):
        self._missing_data_complemntor.fill_data_for_friends()
        author_guid = compute_author_guid_by_author_name(u"GeorgeSlefo").replace('-', '')
        author = self._db.get_author_by_author_guid(author_guid)[0]
        self.assertEqual(author.missing_data_complementor_insertion_date,
                         date_to_str(self._missing_data_complemntor._window_start))
        self.assertNotEqual(author.description, None)
        self._db.session.close()

    def test_mark_suspended_from_twitter(self):
        self._missing_data_complemntor.mark_suspended_from_twitter()
        author = self._db.get_author_by_author_guid(self._suspended_author_guid)[0]
        self.assertEqual(author.is_suspended_or_not_exists, self._missing_data_complemntor._window_start)
        self.assertEqual(author.author_type, Author_Type.BAD_ACTOR)

    def test_missing_data_complimentor_with_incorrect_parameters(self):
        self._missing_data_complemntor._actions = ['ggg', 'assign_manually_labeled_authors']
        self._missing_data_complemntor.execute()
        author = self._db.get_author_by_author_guid(self._private_author_guid)[0]
        self.assertEqual(author.author_sub_type, "private")
        self.assertEqual(author.author_type, "good_actor")
        self._db.session.close()