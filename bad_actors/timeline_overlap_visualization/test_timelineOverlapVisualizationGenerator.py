from unittest import TestCase
from DB.schema_definition import DB, Author, Post
from configuration.config_class import getConfig
import datetime
from timeline_overlap_visualization.timeline_overlap_visualization_generator import TimelineOverlapVisualizationGenerator


class TestTimelineOverlapVisualizationGenerator(TestCase):
    def setUp(self):
        self.config = getConfig()
        self._db = DB()
        self._db.setUp()
        self.timeline_overlap = TimelineOverlapVisualizationGenerator()

        author1 = Author()
        author1.name = u'acquired_user'
        author1.domain = u'Microblog'
        author1.author_guid = u'acquired_user'
        author1.author_screen_name = u'acquired_user'
        author1.author_full_name = u'acquired_user'
        author1.author_osn_id = 1
        author1.created_at = datetime.datetime.now()
        author1.missing_data_complementor_insertion_date = datetime.datetime.now()
        author1.xml_importer_insertion_date = datetime.datetime.now()
        author1.author_type = u'bad_actor'
        author1.author_sub_type = u'acquired'
        self._db.add_author(author1)

        for i in range(1,11):
            post1 = Post()
            post1.post_id = u'bad_post'+str(i)
            post1.author = u'acquired_user'
            post1.guid = u'bad_post'+str(i)
            post1.date = datetime.datetime.now()
            post1.domain = u'Microblog'
            post1.author_guid = u'acquired_user'
            post1.content = u'InternetTV love it'+str(i)
            post1.xml_importer_insertion_date = datetime.datetime.now()
            self._db.addPost(post1)

        author = Author()
        author.name = u'TestUser1'
        author.domain = u'Microblog'
        author.author_guid = u'TestUser1'
        author.author_screen_name = u'TestUser1'
        author.author_full_name = u'TestUser1'
        author.author_osn_id = 2
        author.created_at = datetime.datetime.now()
        author.missing_data_complementor_insertion_date = datetime.datetime.now()
        author.xml_importer_insertion_date = datetime.datetime.now()
        self._db.add_author(author)

        for i in range(1, 11):
            post = Post()
            post.post_id = u'TestPost'+str(i)
            post.author = u'TestUser1'
            post.guid = u'TestPost'+str(i)
            post.date = datetime.datetime.now()
            post.domain = u'Microblog'
            post.author_guid = u'TestUser1'
            post.content = u'InternetTV love it'+str(i)
            post.xml_importer_insertion_date = datetime.datetime.now()
            self._db.addPost(post)

        self._db.commit()

    def test_generate_timeline_overlap_csv(self):
        self.timeline_overlap.setUp()
        self.timeline_overlap.generate_timeline_overlap_csv()
        author = self._db.get_author_by_author_guid(u'acquired_user')
        self.assertEqual(author.author_type, u'bad_actor')
        self.assertEqual(author.author_sub_type, u'acquired')
        pass

    def tearDown(self):
        self._db.session.close_all()
        self._db.session.close()
        self._db.deleteDB()
        self._db.session.close()
