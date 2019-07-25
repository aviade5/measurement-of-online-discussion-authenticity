# Created by jorgeaug at 07/07/2016
import unittest
from DB.schema_definition import *
import datetime
from configuration.config_class import getConfig
from dataset_builder.cocitation_graph_builder import GraphBuilder_CoCitation
from dataset_builder.autotopic_executor import AutotopicExecutor
from dataset_builder.topics_graph_builder import GraphBuilder_Topic
from dataset_builder.key_authors_model import KeyAuthorsModel
from dataset_builder.citation_graph_builder import GraphBuilder_Citation
from dataset_builder.feature_extractor.distances_from_targeted_class_feature_generator import \
    DistancesFromTargetedClassFeatureGenerator


class DatasetBuilderTest(unittest.TestCase):
    def setUp(self):
        """
        Setup for the test:
        TestUser 1 posts TestPost 1 and 2
        TestUser 2 posts TestPost 3 and 4, TestPost 3 references TestPost 1
        TestUser 3 posts TestPost 5 and 6, TestPost 5 references TestPost 1
        TestUser 4 posts TestPost 7 and 8, TestPost 7 references TestPost 4
        TestUser 5 posts TestPost 9, 10, 11 and 12. TestPost 9 references TestPost 1 and TestPost 11 references TestPost 4


        """
        self.config = getConfig()
        self._db = DB()
        self._db.setUp()

        self.min_number_cocited_posts = 1

        module = "GraphFeatureGenerator_1"
        self._graph_types = self.config.eval(module, "graph_types")
        self._algorithms = self.config.eval(module, "algorithms")
        self._aggregations = self.config.eval(module, "aggregation_functions")
        self._neighborhood_sizes = self.config.eval(module, "neighborhood_sizes")
        self._graph_directed = self.config.eval(module, "graph_directed")
        self._graph_weights = self.config.eval(module, "graph_weights")

        author1 = Author()
        author1.name = u'TestUser1'
        author1.domain = u'Microblog'
        author1.author_guid = u'TestUser1'
        author1.author_screen_name = u'TestUser1'
        author1.author_full_name = u'TestUser1'
        author1.author_osn_id = 1
        author1.created_at = datetime.datetime.now()
        author1.missing_data_complementor_insertion_date = datetime.datetime.now()
        author1.xml_importer_insertion_date = datetime.datetime.now()
        author1.author_type = u'good_actor'
        author1.author_sub_type = u'private'
        self._db.add_author(author1)

        post1 = Post()
        post1.post_id = u'TestPost1'
        post1.author = u'TestUser1'
        post1.guid = u'TestPost1'
        post1.date = datetime.datetime.now()
        post1.domain = u'Microblog'
        post1.author_guid = u'TestUser1'
        post1.content = u'InternetTV love it'
        post1.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post1)

        post2 = Post()
        post2.post_id = u'TestPost2'
        post2.author = u'TestUser1'
        post2.guid = u'TestPost2'
        post2.date = datetime.datetime.now()
        post2.domain = u'Microblog'
        post2.author_guid = u'TestUser1'
        post2.content = u' InternetTV !!! :) '
        post2.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post2)

        author2 = Author()
        author2.name = u'TestUser2'
        author2.domain = u'Microblog'
        author2.author_guid = u'TestUser2'
        author2.author_screen_name = u'TestUser2'
        author2.author_full_name = u'TestUser2'
        author2.author_osn_id = 2
        author2.created_at = datetime.datetime.now()
        author2.missing_data_complementor_insertion_date = datetime.datetime.now()
        author2.xml_importer_insertion_date = datetime.datetime.now()
        author2.author_type = u'good_actor'
        author2.author_sub_type = u'private'
        self._db.add_author(author2)

        post3 = Post()
        post3.post_id = u'TestPost3'
        post3.author = u'TestUser2'
        post3.guid = u'TestPost3'
        post3.date = datetime.datetime.now()
        post3.domain = u'Microblog'
        post3.author_guid = u'TestUser2'
        post3.content = u'watching InternetTV'
        post3.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post3)

        post4 = Post()
        post4.post_id = u'TestPost4'
        post4.author = u'TestUser2'
        post4.guid = u'TestPost4'
        post4.date = datetime.datetime.now()
        post4.domain = u'Microblog'
        post4.author_guid = u'TestUser2'
        post4.content = u'InternetTV is wonderful'
        post4.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post4)

        author3 = Author()
        author3.name = u'TestUser3'
        author3.domain = u'Microblog'
        author3.author_guid = u'TestUser3'
        author3.author_screen_name = u'TestUser3'
        author3.author_full_name = u'TestUser3'
        author3.author_osn_id = 3
        author3.created_at = datetime.datetime.now()
        author3.missing_data_complementor_insertion_date = datetime.datetime.now()
        author3.xml_importer_insertion_date = datetime.datetime.now()
        author3.author_type = u'good_actor'
        author3.author_sub_type = u'company'
        self._db.add_author(author3)

        post5 = Post()
        post5.post_id = u'TestPost5'
        post5.author = u'TestUser3'
        post5.guid = u'TestPost5'
        post5.date = datetime.datetime.now()
        post5.domain = u'Microblog'
        post5.author_guid = u'TestUser3'
        post5.content = u'bought a new SmartTV'
        post5.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post5)

        post6 = Post()
        post6.post_id = u'TestPost6'
        post6.author = u'TestUser3'
        post6.guid = u'TestPost6'
        post6.date = datetime.datetime.now()
        post6.domain = u'Microblog'
        post6.author_guid = u'TestUser3'
        post6.content = u'waiting for SmartTV'
        post6.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post6)

        author4 = Author()
        author4.name = u'TestUser4'
        author4.domain = u'Microblog'
        author4.author_guid = u'TestUser4'
        author4.author_screen_name = u'TestUser4'
        author4.author_full_name = u'TestUser4'
        author4.author_osn_id = 4
        author4.created_at = datetime.datetime.now()
        author4.missing_data_complementor_insertion_date = datetime.datetime.now()
        author4.xml_importer_insertion_date = datetime.datetime.now()
        author4.author_type = u'bad_actor'
        author4.author_sub_type = u'bot'
        self._db.add_author(author4)

        post7 = Post()
        post7.post_id = u'TestPost7'
        post7.author = u'TestUser4'
        post7.guid = u'TestPost7'
        post7.date = datetime.datetime.now()
        post7.domain = u'Microblog'
        post7.author_guid = u'TestUser4'
        post7.content = u'Cheap SmartTV only 999 '
        post7.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post7)

        post8 = Post()
        post8.post_id = u'TestPost8'
        post8.author = u'TestUser4'
        post8.guid = u'TestPost8'
        post8.date = datetime.datetime.now()
        post8.domain = u'Microblog'
        post8.author_guid = u'TestUser4'
        post8.content = u'Sony 49" SmartTV only 99 '
        post8.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post8)

        author5 = Author()
        author5.name = u'TestUser5'
        author5.domain = u'Microblog'
        author5.author_guid = u'TestUser5'
        author5.author_screen_name = u'TestUser5'
        author5.author_full_name = u'TestUser5'
        author5.author_osn_id = 5
        author5.created_at = datetime.datetime.now()
        author5.missing_data_complementor_insertion_date = datetime.datetime.now()
        author5.xml_importer_insertion_date = datetime.datetime.now()
        author5.author_type = u'good_actor'
        author5.author_sub_type = u'company'
        self._db.add_author(author5)

        post9 = Post()
        post9.post_id = u'TestPost9'
        post9.author = u'TestUser5'
        post9.guid = u'TestPost9'
        post9.date = datetime.datetime.now()
        post9.domain = u'Microblog'
        post9.author_guid = u'TestUser5'
        post9.content = u'today new episode on InternetTV'
        post9.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post9)

        post10 = Post()
        post10.post_id = u'TestPost10'
        post10.author = u'TestUser5'
        post10.guid = u'TestPost10'
        post10.date = datetime.datetime.now()
        post10.domain = u'Microblog'
        post10.author_guid = u'TestUser5'
        post10.content = u'game of thrones InternetTV '
        post10.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post10)

        post11 = Post()
        post11.post_id = u'TestPost11'
        post11.author = u'TestUser5'
        post11.guid = u'TestPost11'
        post11.date = datetime.datetime.now()
        post11.domain = u'Microblog'
        post11.author_guid = u'TestUser5'
        post11.content = u'I need a SmartTV'
        post11.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post11)

        post12 = Post()
        post12.post_id = u'TestPost12'
        post12.author = u'TestUser5'
        post12.guid = u'TestPost12'
        post12.date = datetime.datetime.now()
        post12.domain = u'Microblog'
        post12.author_guid = u'TestUser5'
        post12.content = u'how much for a SmartTV ?'
        post12.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post12)

        author6 = Author()
        author6.name = u'TestUser6'
        author6.domain = u'Microblog'
        author6.author_guid = u'TestUser6'
        author6.author_screen_name = u'TestUser6'
        author6.author_full_name = u'TestUser6'
        author6.author_osn_id = 6
        author6.created_at = datetime.datetime.now()
        author6.missing_data_complementor_insertion_date = datetime.datetime.now()
        author6.xml_importer_insertion_date = datetime.datetime.now()
        author6.author_type = u'good_actor'
        author6.author_sub_type = u'private'
        self._db.add_author(author6)

        post19 = Post()
        post19.post_id = u'TestPost19'
        post19.author = u'TestUser6'
        post19.guid = u'TestPost19'
        post19.date = datetime.datetime.now()
        post19.domain = u'Microblog'
        post19.author_guid = u'TestUser6'
        post19.content = u'InternetTV love it'
        post19.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post19)

        post_cit = Post_citation()
        post_cit.post_id_from = u'TestPost19'
        post_cit.post_id_to = u'TestPost1'
        self._db.session.merge(post_cit)

        post_cit = Post_citation()
        post_cit.post_id_from = u'TestPost3'
        post_cit.post_id_to = u'TestPost1'
        self._db.session.merge(post_cit)

        post_cit = Post_citation()
        post_cit.post_id_from = u'TestPost5'
        post_cit.post_id_to = u'TestPost1'
        self._db.session.merge(post_cit)

        post_cit = Post_citation()
        post_cit.post_id_from = u'TestPost7'
        post_cit.post_id_to = u'TestPost4'
        self._db.session.merge(post_cit)

        post_cit = Post_citation()
        post_cit.post_id_from = u'TestPost9'
        post_cit.post_id_to = u'TestPost1'
        self._db.session.merge(post_cit)

        post_cit = Post_citation()
        post_cit.post_id_from = u'TestPost11'
        post_cit.post_id_to = u'TestPost4'
        self._db.session.merge(post_cit)

        self._db.create_author_post_cite_view()
        self._db.session.commit()

        parameters = {"connection_type": "cocitation", "min_number_of_cocited_posts": 1}
        cocitationgraph_builder = GraphBuilder_CoCitation(self._db)
        cocitationgraph_builder.execute(window_start=None)

        post = Post()
        post.post_id = u'TestPost13'
        post.author = u'TestUser1'
        post.guid = u'TestPost13'
        post.date = datetime.datetime.now()
        post.domain = u'Microblog'
        post.author_guid = u'TestUser1'
        post.content = u'Wow cool InternetTV'
        post.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post)

        post = Post()
        post.post_id = u'TestPost14'
        post.author = u'TestUser2'
        post.guid = u'TestPost14'
        post.date = datetime.datetime.now()
        post.domain = u'Microblog'
        post.author_guid = u'TestUser2'
        post.content = u'InternetTV is so awesome'
        post.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post)

        post = Post()
        post.post_id = u'TestPost15'
        post.author = u'TestUser3'
        post.guid = u'TestPost15'
        post.date = datetime.datetime.now()
        post.domain = u'Microblog'
        post.author_guid = u'TestUser5'
        post.content = u'SmartTV losses this time'
        post.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post)

        post = Post()
        post.post_id = u'TestPost16'
        post.author = u'TestUser4'
        post.guid = u'TestPost16'
        post.date = datetime.datetime.now()
        post.domain = u'Microblog'
        post.author_guid = u'TestUser4'
        post.content = u'cheap remote for SmartTV'
        post.xml_importer_insertion_date = datetime.datetime.now()

        post = Post()
        post.post_id = u'TestPost17'
        post.author = u'TestUser5'
        post.guid = u'TestPost17'
        post.date = datetime.datetime.now()
        post.domain = u'Microblog'
        post.author_guid = u'TestUser5'
        post.content = u'good SmartTV'
        post.xml_importer_insertion_date = datetime.datetime.now()

        post = Post()
        post.post_id = u'TestPost18'
        post.author = u'TestUser5'
        post.guid = u'TestPost18'
        post.date = datetime.datetime.now()
        post.domain = u'Microblog'
        post.author_guid = u'TestUser5'
        post.content = u'nice InternetTV very'
        post.xml_importer_insertion_date = datetime.datetime.now()
        self._db.addPost(post)

        author_con = AuthorConnection()
        author_con.from_author = u'TestUser1'
        author_con.source_author_guid = u'TestUser1'
        author_con.to_author = u'TestUser2'
        author_con.destination_author_guid = u'TestUser2'
        author_con.connection_type = "cocitation"
        self._db.add_author_connection(author_con)

        self._db.commit()

        #########################################################
        """"
        So far each user has the following content
        TestUser1:
            Post 1: InternetTV love it
            Post 2: InternetTV !!! :)
            Post 13: Wow cool internetTV
        TestUser2:
            Post 3: watching InternetTV
            Post 4: InternetTV is wonderful
            Post 14: internetTV is so awesome
        TestUser3:
            Post 5: bought a new SmartTV
            Post 6: waiting for SmartTV
            Post 15: SmartTV losses this time
        TestUser 4:
            Post 7: Cheap SmartTV only 999
            Post 8: Sony 49" SmartTV only 99
            Post 16: cheap remote for SmartTV
        TestUser 5:
            Post 9: today new episode on InternetTV
            Post 10: game of thrones InternetTV
            Post 11: I need a SmartTV
            Post 12: how much for a SmartTV ?
            Post 17: good SmartTV
            Post 18: nice InternetTV very

        We expect users 1, 2 and probably 5 to be linked
        As well as users 3, 4 and 5
        """
        #########################################################

    """
        There should be 5 references in the view author_post_cite
        1) User 2 -> User 1
        2) User 3 -> User 1
        3) User 5 -> User 1
        4) User 4 -> User 2
        5) User 5 -> User 2
    """

    def tearDown(self):
        self._db.deleteDB()
        self._db.session.close()

    def testNumberEntriesInCocitationView(self):
        result = self._db.session.execute("select * from author_post_cite  ")
        cursor = result.cursor
        rows = list(cursor.fetchall())
        test = True
        expectedCitations = {(u'TestUser2', u'TestPost1'), (u'TestUser3', u'TestPost1'), (u'TestUser5', u'TestPost1'),
                             (u'TestUser4', u'TestPost4'), (u'TestUser5', u'TestPost4')
            , (u'TestUser6', u'TestPost1')}
        for row in rows:
            if (not (row in expectedCitations)):
                test = False
        self.assertTrue(test)
        self._db.session.close()

        """
            There should be 8 Edges in the Co-Citation network for these users:
            (TestUser 2 , TestUser 3) and (TestUser 3 , TestUser 2)
            (TestUser 2 , TestUser 5) and (TestUser 5 , TestUser 2)
            (TestUser 3 , TestUser 5) and (TestUser 5 , TestUser 3)
            (TestUser 4 , TestUser 5) and (TestUser 5 , TestUser 4)
            (TestUser 6, TestUser 2) and (TestUser 2, TestUser 6)

        """

    def testNumberEdgesInCocitationGraph(self):
        rows = self._db.get_cocitations(self.min_number_cocited_posts)
        expected_citations = {(u'TestUser2', u'TestUser3', 1), (u'TestUser2', u'TestUser5', 1),
                              (u'TestUser3', u'TestUser5', 1), (u'TestUser4', u'TestUser5', 1),
                              (u'TestUser6', u'TestUser2', 1), (u'TestUser6', u'TestUser3', 1),
                              (u'TestUser6', u'TestUser5', 1)}
        test = True
        all_cocitations = [row for row in rows]
        for cit in expected_citations:
            if not (cit in all_cocitations):
                test = False
        self.assertTrue(test)
        self._db.session.close()

    """
    Topic graph should cotain the following edges:
    1) author 1 ---- author 2 (InternetTV)
    2) author 1 ---- author 5 (InternetTV)
    3) author 2 ---- author 5 (InternetTV)
    4) author 3 ---- author 4 (SmartTV)
    5) author 3 ---- author 5 (SmartTV)
    6) author 4 ---- author 5 (Smart TV)
    """

    def testTopicGraph(self):
        if 1 == 2:
            autotopicExe = AutotopicExecutor(self._db)
            autotopicExe.setUp()
            autotopicExe.execute()

            topicgraph_builder = GraphBuilder_Topic(self._db)
            topicgraph_builder.execute()

            cursor = self._db.get_author_connections_by_type('topic')
            bad_edges = 0
            author_connections = self._db.result_iter(cursor)
            good_edges = 6
            for author_connection in author_connections:
                source_author_osn_id = int(author_connection[0])
                destination_author_osn_id = int(author_connection[1])
                if ((source_author_osn_id == 1 and destination_author_osn_id == 2)
                    or (source_author_osn_id == 1 and destination_author_osn_id == 5)
                    or (source_author_osn_id == 2 and destination_author_osn_id == 5)
                    or (source_author_osn_id == 3 and destination_author_osn_id == 4)
                    or (source_author_osn_id == 3 and destination_author_osn_id == 5)
                    or (source_author_osn_id == 4 and destination_author_osn_id == 5)):
                    good_edges -= 1
                else:
                    bad_edges += 1
            print("bad_edges: " + str(bad_edges))
            self.assertEqual(good_edges, 0)
        else:
            self.assertEquals(0, 0)
        self._db.session.close()

    def testKeyAuthorsModel(self):
        if 1 == 2:
            autotopicExe = AutotopicExecutor(self._db)
            autotopicExe.setUp()
            autotopicExe.execute()
            key_author_moddel = KeyAuthorsModel(self._db)
            key_author_moddel.setUp()
            key_author_moddel.execute()
            key_posts = self._db.get_key_posts()
            key_authors = self._db.get_key_authors()
            self.assertTrue(u'TestPost1' in key_posts, "TestPost1 not a key post")
            self.assertTrue(u'TestPost4' in key_posts, "TestPost4 not a key post")
            self.assertTrue(u'TestUser1' in key_authors, "TestUser1 not a key author")
            self.assertTrue(u'TestUser2' in key_authors, "TestUser2 not a key author")
        else:
            self.assertEquals(0, 0)
        self._db.session.close()

    def testCitaionGraph(self):
        if 1 == 2:
            self._domain = self.config.eval("DEFAULT", "domain")
            citaion_graph = GraphBuilder_Citation(self._db)
            citaion_graph.setUp()
            citaion_graph.execute(None)
            test = True
            rows = self._db.get_citations(self._domain)
            expectedCitations = {(u'TestUser2', u'TestUser1', 1),
                                 (u'TestUser3', u'TestUser1', 1),
                                 (u'TestUser5', u'TestUser1', 1),
                                 (u'TestUser4', u'TestUser2', 1),
                                 (u'TestUser5', u'TestUser2', 1),
                                 (u'TestUser6', u'TestUser1', 1)}
            for row in rows:
                if (not (row in expectedCitations)):
                    test = False
            self.assertTrue(test)
            self._db.session.close()

    def testDistancesFromTargetedClass(self):
        authors = self._db.get_authors_by_domain("Microblog")
        parameters = {"graph_types": self._graph_types,
                      "algorithms": self._algorithms,
                      "aggregation_functions": self._aggregations,
                      "neighborhood_sizes": self._neighborhood_sizes,
                      "graph_directed": self._graph_directed,
                      "graph_weights": self._graph_weights,
                      "authors": authors,
                      "graphs": {}}

        distances = DistancesFromTargetedClassFeatureGenerator(self._db, **parameters)
        distances.execute()

        test = False
        rows = self._db.get_distance_features()
        if len(rows) > 0:
            test = True

        self.assertTrue(test)
        self._db.session.close_all()
