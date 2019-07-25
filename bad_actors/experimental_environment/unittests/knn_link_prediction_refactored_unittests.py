#Written by Lior Bass 12/14/2017

import unittest
from DB.schema_definition import *
from experimental_environment.knnwithlinkprediction_refacatored import KNNWithLinkPrediction_Refactored
from dataset_builder.feature_extractor.link_prediction_feature_extractor import LinkPredictionFeatureExtractor
import csv

from experimental_environment.knnwithlinkprediction_refacatored import KNNWithLinkPrediction_Refactored


class Knn_refactored_Tests(unittest.TestCase):
    def setUp(self):
        self._db = DB()
        self._db.setUp()
        self._knn_algoritem = KNNWithLinkPrediction_Refactored(self._db)
        self._file_location = self._knn_algoritem._result_authors_file_name
        self._domain = u'Microblog'
        self._knn_algoritem._domain = self._domain
        self._bad_actor_type = u'bad_actor'
        self._good_actor_type = u'good_actor'
        self._graph_type = u"cocitation"
        self._train_percent = 50 # need to be 100 to work

    def tearDown(self):
        try:
            os.remove(self._knn_algoritem._result_authors_file_name)
            os.remove(self._knn_algoritem._result_classifier_file_name)
        except:
            logging.info("could not remove files")
        self._db.session.commit()
        self._db.session.close()
        self._db.deleteDB()
        self._db.session.close()


    def test_knn_algoritem_simple_case(self):
        self.create_author("a1", self._good_actor_type) #test
        self.create_author("a2", self._bad_actor_type) #test
        self.create_author("a3", self._bad_actor_type)  #anchor
        self.create_author("a4", self._bad_actor_type)  #anchor

        self.create_conncation("a1","a2", 1)
        self.create_conncation("a2","a3", 1)
        self.create_conncation("a3", "a4", 1)
        self.create_conncation("a4", "a1", 1)
        self.create_conncation("a1", "a3", 1)

        self._knn_algoritem._k = [2]
        self._knn_algoritem._train_percent = 50
        self._knn_algoritem.execute()

        a1_label = self.retrive_label("a1", 2, "majority_voting")
        a2_label = self.retrive_label("a2", 2, "majority_voting")

        self.assertEqual(self._bad_actor_type, a1_label)
        self.assertEqual(self._bad_actor_type, a2_label)

    def test__knn_algoritem_check_weight(self):
        self.create_author("a1", self._good_actor_type) #test
        self.create_author("a4", self._bad_actor_type) #test
        self.create_author("a3", self._good_actor_type)  #anchor
        self.create_author("a2", self._bad_actor_type)  #anchor
        self.create_author("a5", self._bad_actor_type) #anchor
        self.create_author("a6", self._good_actor_type)

        self.create_conncation("a1", "a4", 1.3)

        self.create_conncation("a1","a3",1.5)
        self.create_conncation("a2","a1", 0.2)
        self.create_conncation("a1", "a5", 0.1)

        self.create_conncation("a4", "a3", 0.5)
        self.create_conncation("a4", "a2", 0.7)
        self.create_conncation("a4","a6", 0.1)

        self.create_conncation("a2","a3",2)

        self._knn_algoritem._k = [3, 2]
        self._knn_algoritem._train_percent = 33
        self._knn_algoritem.execute()

        a1_label = self.retrive_label("a1", 3, "majority_voting", 33)
        a1_label_weighted = self.retrive_label("a1", 3, "weighted_majority_voting",33)

        a4_label = self.retrive_label("a4", 3, "majority_voting", 33)
        a4_label_weighted = self.retrive_label("a4", 3, "weighted_majority_voting",33)

        self.assertEqual(self._good_actor_type, a1_label_weighted)
        self.assertEqual(self._bad_actor_type, a1_label)

        self.assertEqual(self._bad_actor_type, a4_label_weighted)
        self.assertEqual(self._good_actor_type, a4_label)

        a1_label = self.retrive_label("a1", 2, "majority_voting", 33)
        a1_label_weighted = self.retrive_label("a1", 2, "weighted_majority_voting", 33)

        a4_label_confidance = self.retrivie_confidence("a4",2, "majority_voting", 33)
        a4_label = self.retrive_label("a4", 2, "majority_voting", 33)
        a4_label_weighted = self.retrive_label("a4", 2, "weighted_majority_voting", 33)

        self.assertEqual(self._good_actor_type, a1_label_weighted)
        self.assertEqual(self._bad_actor_type, a1_label)

        self.assertEqual('0.5', a4_label_confidance)
        self.assertEqual(self._bad_actor_type, a4_label_weighted)
        self.assertEqual(self._bad_actor_type, a4_label)

    def test_big_case(self):
        self.create_author("a1", self._bad_actor_type) #test
        self.create_author("a8", self._bad_actor_type) # test
        self.create_author("a3", self._good_actor_type) #test
        self.create_author("a7", self._good_actor_type) #test
        self.create_author("a6", self._good_actor_type) #test

        self.create_author("a2", self._good_actor_type)#anchor
        self.create_author("a4", self._good_actor_type) #anchor
        self.create_author("a5", self._good_actor_type) #anchor
        self.create_author("a9", self._bad_actor_type) #anchor
        self.create_author("a10", self._bad_actor_type) #anchor

        self.create_conncation("a1", "a2", 3)
        self.create_conncation("a4","a1", 2)
        self.create_conncation("a1","a5", 8)
        self.create_conncation("a9", "a1", 2)
        self.create_conncation("a1", "a8", 1)

        self.create_conncation("a8", "a10", 2)
        self.create_conncation("a9", "a8", 3)
        self.create_conncation("a8", "a2", 1)
        self.create_conncation("a7", "a9", 5)

        self._knn_algoritem._k = [1,2,3]
        self._knn_algoritem._train_percent = self._train_percent
        self._knn_algoritem.execute()

        a1_label = self.retrive_label("a1", 1, "majority_voting")
        a1_label_weighted = self.retrive_label("a1", 1, "weighted_majority_voting")

        a8_label = self.retrive_label("a8", 1, "majority_voting")
        a8_label_weighted = self.retrive_label("a8", 1, "weighted_majority_voting")

        self.assertEqual(self._good_actor_type, a1_label_weighted)
        self.assertEqual(self._good_actor_type, a1_label)

        self.assertEqual(self._bad_actor_type, a8_label_weighted)
        self.assertEqual(self._bad_actor_type, a8_label)

    def retrive_label(self, author_guid, k, desicion_model, percent=None):
        with open(self._file_location) as text_file:
            dr = csv.DictReader(text_file)
            for row in dr:
                _author_guid = row[u'author_guid']
                if percent is None:
                    percent = self._train_percent
                    _train_percent = percent
                else:
                    _train_percent = row[u"train_percent"]
                _k = row[u'k']
                _desicion_model = row[u"desicion_model"]
                label = row[u"label"]
                if _author_guid==unicode(author_guid) and unicode(text(_k))==unicode(k) and _desicion_model == unicode(desicion_model) and unicode(_train_percent) == unicode(percent):
                    return label

    def retrivie_confidence(self, author_guid, k, desicion_model, percent=None):
        with open(self._file_location) as text_file:
            dr = csv.DictReader(text_file)
            for row in dr:
                _author_guid = row[u'author_guid']
                if percent is None:
                    percent = self._train_percent
                    _train_percent = percent
                else:
                    _train_percent = row[u"train_percent"]
                _k = row[u'k']
                _desicion_model = row[u"desicion_model"]
                confidence = row[u"confidence"]
                if _author_guid==unicode(author_guid) and unicode(text(_k))==unicode(k) and _desicion_model == unicode(desicion_model) and unicode(_train_percent) == unicode(percent):
                    return confidence

    def create_author(self, guid, author_type):
        author1 = Author()
        author1.name = unicode(guid)
        author1.domain = u'Microblog'
        author1.author_guid = unicode(guid)
        author1.author_screen_name = u'TestUser1'
        author1.author_type = author_type
        author1.domain = self._domain
        author1.author_osn_id = 1
        self._db.add_author(author1)

    def create_conncation(self, guid_1, guid_2, weight):
        author_connection = AuthorConnection()
        author_connection.source_author_guid = unicode(guid_1)
        author_connection.destination_author_guid = unicode(guid_2)
        author_connection.connection_type = self._graph_type
        author_connection.weight = weight
        author_connection.insertion_date = datetime.datetime.now()
        self._db.add_author_connections([author_connection])