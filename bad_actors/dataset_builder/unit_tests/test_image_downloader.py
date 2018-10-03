from unittest import TestCase

import os

from DB.schema_definition import DB, Post
from dataset_builder.image_downloader.image_downloader import Image_Downloader


class TestImageDownloader(TestCase):
    def setUp(self):
        self._db = DB()
        self._db.setUp()

        self._counter = 0
        self._posts = {}

    def tearDown(self):
        self._db.session.close()
        for f_name in os.listdir(self.image_downloader._path_for_downloaded_images):
            os.remove(self.image_downloader._path_for_downloaded_images + '/' + f_name)

    def test_download_image_from_correct_url(self):

        # img_url = 'https://twitter.com/DHSgov/status/1042788536943104000'
        self._add_post(u'p1', u'p1_data', u'https://www.pc.co.il/wp-content/uploads/2018/02/Google-Pay.600.jpg')
        self._db.session.commit()

        self.image_downloader = Image_Downloader(self._db)
        self.image_downloader.setUp()
        self.image_downloader.execute(None)

        post_dict = self._db.get_post_dictionary()
        self.assertIsNotNone(post_dict['p1'].media_path)
        self.assertTrue(os.path.exists(self.image_downloader._path_for_downloaded_images + '/p1.jpg'))

    def test_download_image_from_3_correct_url(self):
        self._add_post(u'p1', u'p1_data', u'https://www.pc.co.il/wp-content/uploads/2018/02/Google-Pay.600.jpg')
        self._add_post(u'p2', u'p2_data', u'https://timedotcom.files.wordpress.com/2017/11/google-doode_pad-thai.png')
        self._add_post(u'p3', u'p3_data', u'https://marketingland.com/wp-content/ml-loads/2012/02/google-good-evil-featured.jpg')
        self._db.session.commit()

        self.image_downloader = Image_Downloader(self._db)
        self.image_downloader.setUp()
        self.image_downloader.execute(None)

        post_dict = self._db.get_post_dictionary()
        self.assertIsNotNone(post_dict['p1'].media_path)
        self.assertIsNotNone(post_dict['p2'].media_path)
        self.assertIsNotNone(post_dict['p3'].media_path)
        self.assertTrue(os.path.exists(self.image_downloader._path_for_downloaded_images + '/p1.jpg'))
        self.assertTrue(os.path.exists(self.image_downloader._path_for_downloaded_images + '/p2.jpg'))
        self.assertTrue(os.path.exists(self.image_downloader._path_for_downloaded_images + '/p3.jpg'))

    def _add_post(self, post_id, content, url, _domain=u'Microblog'):
        post = Post()
        post.author = u'test_user'
        post.author_guid = u'test_user'
        post.content = content
        post.title = post_id
        post.domain = _domain
        post.post_id = post_id
        post.guid = post_id
        post.url = url
        post.source_url = url
        self._db.addPost(post)
        self._posts[post_id] = post
