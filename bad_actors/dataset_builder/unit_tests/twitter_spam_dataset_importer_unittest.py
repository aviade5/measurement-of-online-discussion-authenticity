import unittest
from DB.schema_definition import *
from dataset_builder.twitter_spam_dataset_importer import Twitter_Spam_Dataset_Importer


class Twitter_Spam_Dataset_Importer_Unittest(unittest.TestCase):
    def setUp(self):
        self._config_parser = getConfig()
        self._db = DB()
        self._db.setUp()
        self._twitter_spam_dataset_importer = Twitter_Spam_Dataset_Importer(self._db)
        self._twitter_spam_dataset_importer._domain = u"Microblog"

    def test_twitter_spam_dataset_importer(self):
        self._twitter_spam_dataset_importer.execute(None)
        # authors = self._db.get_authors_by_domain('Microblog')
        authors = self._db.get_all_authors()
        count_authors = len(authors)
        self.assertEquals(count_authors, 4)
        posts = self._db.get_all_posts()
        count_posts = len(posts)
        self.assertEquals(count_posts, 4)

        expected_content = u'@sphinxmagic are you kidding? I HATED the classics! with the exception of Citizen Kane, I wish those films never existed!'
        self.assertEquals(posts[0].content, expected_content)

        self.assertEquals(authors[0].name, u'357r4v3n')
        self.assertEquals(authors[1].name, u'boltcity')

    def tearDown(self):
        self._db.session.close()
        self._db.deleteDB()
        self._db.session.close()
