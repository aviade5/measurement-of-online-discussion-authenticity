# Created by aviade                   
# Time: 29/03/2016 16:01

import unittest

import sys


sys.argv = ['', 'configuration/config_test.ini']

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
#
from timeline_overlap_visualization.test_timelineOverlapVisualizationGenerator import TestTimelineOverlapVisualizationGenerator
from Twitter_API.unit_tests.twitter_api_requester_unittests import TestTwitterApiRequester
from preprocessing_tools.unit_tests.xml_importer_unittests import TestXmlImporter
from DB.unit_tests.posts_unittests import TestPost
from DB.unit_tests.authors_unittests import TestAuthor
from bad_actors_collector.unit_tests.bad_actor_collector_unittests import TestBadActorCollector
from dataset_builder.unit_tests.dataset_builder_unittests import DatasetBuilderTest
from dataset_builder.unit_tests.feature_extractor_unittests import FeatureExtractorTest
from missing_data_complementor.unit_tests.test_missingDataComplementor import MissingDataComplemntorTests
from twitter_crawler.unittests.twitter_crawler_tests import TwitterCrawlerTests
from preprocessing_tools.unit_tests.test_app_Importer import TestAppImporter
from preprocessing_tools.unit_tests.test_rank_app_importer import TestRankAppImporter
from dataset_builder.lda_topic_model_test import TestLDATopicModel
from preprocessing_tools.tsv_importer import TestCSVDataImport

if __name__ == "__main__":
    unittest.main()
