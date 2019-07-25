# coding=utf-8
# Created by aviade
# Time: 04/05/2016 17:25

from DB.schema_definition import Author
from test_base import TestBase
from commons.commons import *

class TestAuthor(TestBase):
    def setUp(self):
        TestBase.setup(self)
        self.name = u"name"
        self._domain = u"Microblog"
        self.author_guid = unicode(generate_random_guid())
        self.new_author_name = u"new_author_name"
        self.umlaut_author_name = u"loegfo__fuuhde☺☻♥♥♥Ää"
        self.umlaut_author_full_name = u"✫veronica✫Ää"
        self.umlaut_author_description = u"ⒷritainsⒷestⒸhoiceÄ	ä"
        self.umlaut_language = u"カリムスキー☺☻♥♦♣♠•◘○Ä	ä"
        self.umlaut_location = u"猫パンチÄ	ä"


    def testDBSetUp(self):
        TestBase.testDBSetUp(self)
        self.assertTrue("authors" in set(self.inspector.get_table_names()))
        self.clean()


    ########################################################################
    # posts's unit tests
    ########################################################################

    def create_dummy_author(self):

        name = unicode(self.name)
        domain = unicode(self._domain)
        author_guid = unicode(self.author_guid)
        author_full_name = u"author_full_name"
        author_osn_id = unicode(generate_random_guid())
        created_at = u"2016-08-24 10:00"
        statuses_count = 11
        followers_count = 12
        favourites_count = 14
        friends_count = 15
        listed_count = 16
        description = u"description"
        language = u"English"
        location = u"Beer Sheva"
        time_zone = u"Israel"
        url = u"http://google.com"

        author = Author(name=name, domain=domain, author_guid=author_guid, author_full_name=author_full_name,
                        author_osn_id=author_osn_id, created_at=created_at, statuses_count=statuses_count,
                        followers_count=followers_count, favourites_count=favourites_count,
                        friends_count=friends_count, listed_count=listed_count, description=description, language=language, location=location,
                        time_zone=time_zone, url=url)

        return author

    def create_and_insert_author(self):
        author = self.create_dummy_author()
        self.db.addAuthor(author)
        self.db.session.commit()
        return author

    def create_authors_for_assigning_tests(self):
        self.autors_sub_type_author_guid_dict = {
            'private': u'6343dc3298343d4780f6242dd553a2fd',
            'company': u'0db2b25a46203c589db61818cb3bac49',
            'news_feed':u'37e0df45b746342c9c7d80a49e565354',
            'spammer':u'01b5059b6db33133a3a44fb2b8fc3cc2',
            'bot':u'042763a891aa3ec4bd613f5fe34df71c',
            'acquired':u'6be7d7a96bd43afabf40f041044fea9e',
            'crowdturfer':u'6c952b965d7d375192e0107f62cb7f38'
        }

        for sub_type in self.autors_sub_type_author_guid_dict:
            author = Author()
            author.name = unicode(sub_type)
            author.domain = unicode(self._domain)
            author.author_guid = unicode(self.autors_sub_type_author_guid_dict[sub_type])
            self.db.add_author(author)
        self.db.session.commit()

    def create_authors_for_deleting_tests(self):
        self.guid1 = u'83d5812f-ff13-46d8-8c1c-3f17a48c239f'
        author = Author()
        author.name = u'author1'
        author.domain = unicode(self._domain)
        author.author_guid = unicode(self.guid1)
        author.author_type = u'bad_actor'
        author.author_sub_type = u'crowdturfer'
        self.db.add_author(author)


        self.guid2 = u'08fffd68-52f9-45dd-a1ea-7c2a1b0206c4'
        author = Author()
        author.name = u'author2'
        author.domain = unicode(self._domain)
        author.author_guid = unicode(self.guid2)
        author.author_type = u'bad_actor'
        author.author_sub_type = None
        self.db.add_author(author)


        self.guid3 = u'a041d99d-7adc-47ad-a32b-ac24c1e43c03'
        author = Author()
        author.name = u'author3'
        author.domain = self._domain
        author.author_guid = self.guid3
        author.author_type = u'bad_actor'
        author.author_sub_type = u'bot'
        self.db.add_author(author)


        self.guid4 = u'06bc3c1b-0350-428f-b66c-7d476f442643'
        author = Author()
        author.name = u'author4'
        author.domain = self._domain
        author.author_guid = self.guid4
        author.author_type = u'good_actor'
        author.author_sub_type = None
        self.db.add_author(author)


        self.guid5 = u'c5c1d938-1196-4bab-9f5e-23092c7be053'
        author = Author()
        author.name = u'author5'
        author.domain = self._domain
        author.author_guid = self.guid5
        author.author_type = u'bad_actor'
        author.author_sub_type = u'acquired'
        self.db.add_author(author)
        self.db.session.commit()

    def tearDown(self):
        self.db.session.close()
        pass

    def testInsertAuthor(self):
        self.setup()

        self.create_and_insert_author()


        authors = self.db.getAuthorByName(self.name)
        selected_author = authors[0]

        self.assertEqual(self.name, selected_author.name)
        self.assertEqual(self._domain, selected_author.domain)

        self.db.delete_author(self.name, self._domain, self.author_guid)
        self.clean()


    def testDeleteAuthor(self):
        self.setup()

        self.create_and_insert_author()

        self.db.delete_author(self.name, self._domain, self.author_guid)

        records = self.db.get_authors()
        length = len(records)
        self.assertEqual(0, length)
        self.clean()

    def testUpdateAuthor(self):
        self.setup()

        author = self.create_and_insert_author()

        #self.db.update_post(self.previous_text, "text", self.new_text)
        author.author_full_name = self.new_author_name
        self.db.addAuthor(author)

        authors = self.db.getAuthorByName(self.name)
        selected_author = authors[0]

        self.assertIsNotNone(selected_author)
        self.assertEqual(selected_author.author_full_name, self.new_author_name)

        self.db.delete_author(self.name, self._domain, self.author_guid)
        self.clean()

    def testUmlautAuthor(self):
        self.setup()

        umlaut_author = self.create_umlaut_author()
        self.db.addAuthor(umlaut_author)

        authors = self.db.get_author_by_author_guid_and_domain(self.author_guid, self._domain)

        extracted_author = authors[0]
        self.assertEquals(extracted_author.name, self.umlaut_author_name)
        self.assertEquals(extracted_author.author_full_name, self.umlaut_author_full_name)
        self.assertEquals(extracted_author.description, self.umlaut_author_description)
        self.assertEquals(extracted_author.language, self.umlaut_language)
        self.assertEquals(extracted_author.location, self.umlaut_location)

        self.db.delete_author(self.umlaut_author_name, self._domain, self.author_guid)
        self.clean()



    def create_umlaut_author(self):
        umlaut_author = Author()

        umlaut_author.name = self.umlaut_author_name
        umlaut_author.domain = self._domain
        umlaut_author.author_guid = self.author_guid
        umlaut_author.author_full_name = self.umlaut_author_full_name
        umlaut_author.description = self.umlaut_author_description
        umlaut_author.language = self.umlaut_language
        umlaut_author.location = self.umlaut_location

        return umlaut_author

    def test_authors_assignment(self):
        self.setup()
        self.create_authors_for_assigning_tests()
        self.db.assign_private_profiles()
        author = self.db.get_author_by_author_guid_and_domain(self.autors_sub_type_author_guid_dict['private'], self._domain)
        self.assertEqual(author[0].author_sub_type, u'private')

        self.db.assign_company_profiles()
        author = self.db.get_author_by_author_guid_and_domain(self.autors_sub_type_author_guid_dict['company'], self._domain)
        self.assertEqual(author[0].author_sub_type, u'company')

        self.db.assign_news_feed_profiles()
        author = self.db.get_author_by_author_guid_and_domain(self.autors_sub_type_author_guid_dict['news_feed'], self._domain)
        self.assertEqual(author[0].author_sub_type, u'news_feed')

        self.db.assign_spammer_profiles()
        author = self.db.get_author_by_author_guid_and_domain(self.autors_sub_type_author_guid_dict['spammer'], self._domain)
        self.assertEqual(author[0].author_sub_type, u'spammer')

        self.db.assign_bot_profiles()
        author = self.db.get_author_by_author_guid_and_domain(self.autors_sub_type_author_guid_dict['bot'], self._domain)
        self.assertEqual(author[0].author_sub_type, u'bot')

        self.db.assign_acquired_profiles()
        author = self.db.get_author_by_author_guid_and_domain(self.autors_sub_type_author_guid_dict['acquired'], self._domain)
        self.assertEqual(author[0].author_sub_type, u'acquired')

        self.db.assign_crowdturfer_profiles()
        author = self.db.get_author_by_author_guid_and_domain(self.autors_sub_type_author_guid_dict['crowdturfer'], self._domain)
        self.assertEqual(author[0].author_sub_type, u'crowdturfer')
        self.clean()

    def test_delete_acquired_authors(self):
        self.setup()
        self.create_authors_for_deleting_tests()
        self.db.delete_acquired_authors()
        author1 = self.db.get_author_by_author_guid_and_domain(self.guid1, self._domain)
        author2 = self.db.get_author_by_author_guid_and_domain(self.guid2, self._domain)
        author3 = self.db.get_author_by_author_guid_and_domain(self.guid3, self._domain)
        self.assertEqual(author1,[])
        self.assertEqual(author2,[])
        self.assertNotEqual(author3,[])
        self.clean()

    def test_delete_manually_labeled_authors(self):
        self.setup()
        self.create_authors_for_deleting_tests()
        self.db.delete_manually_labeled_authors()
        author1 = self.db.get_author_by_author_guid_and_domain(self.guid1, self._domain)
        author2 = self.db.get_author_by_author_guid_and_domain(self.guid2, self._domain)
        author3 = self.db.get_author_by_author_guid_and_domain(self.guid3, self._domain)
        author4 = self.db.get_author_by_author_guid_and_domain(self.guid4, self._domain)
        self.assertEqual([author1,author2,author3,author4],[[],[],[],[]])
        self.clean()

