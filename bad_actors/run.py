'''
Created on 26  JUN  2016

@author: Jorge Bendahan (jorgeaug@post.bgu.ac.il)

'''
import csv
import logging.config
import os
import time

from DB.schema_definition import DB
from old_tweets_crawler.old_tweets_crawler import OldTweetsCrawler
from preprocessing_tools.csv_to_table_importer import CsvToTableImporter
from preprocessing_tools.fake_news_snopes_importer.fake_news_snopes_importer import FakeNewsSnopesImporter
from bad_actors_collector.bad_actors_collector import BadActorsCollector
from configuration.config_class import getConfig
from data_exporter.data_exporter import DataExporter
from dataset_builder.activity_manager.comment_manager import CommentManager
from dataset_builder.activity_manager.echo_comment_manager import EchoManager
from data_exporter.data_exporter import DataExporter
from dataset_builder.activity_manager.post_manager import PostManager
from dataset_builder.activity_manager.influence_manager import InfluenceManager
from dataset_builder.graph_builders.bag_of_words_graph_builders.bag_of_words_graph_builder_all_combinations import \
    Bag_Of_Words_Graph_Builder_All_Combinations
from dataset_builder.graph_builders.bag_of_words_graph_builders.bag_of_words_graph_builder_k_best import \
    Bag_Of_Words_Graph_Builder_K_Best
from dataset_builder.graph_builders.bag_of_words_graph_builders.bag_of_words_graph_builder_random_combinations import \
    Bag_Of_Words_Graph_Builder_Random_Combinations
from dataset_builder.graph_builders.followers_graph_builder import GraphBuilder_Followers
from dataset_builder.graph_builders.topic_graph_builders.topic_graph_builder_all_combinations import \
    Topic_Graph_Builder_All_Combinations
from dataset_builder.graph_builders.topic_graph_builders.topic_graph_builder_k_best import Topic_Graph_Builder_K_Best
from dataset_builder.graph_builders.topic_graph_builders.topic_graph_builder_random_combinations import \
    Topic_Graph_Builder_Random_Combinations
from dataset_builder.graph_builders.word_embedding_graph_builder import GraphBuilder_Word_Embedding
from dataset_builder.image_downloader.image_downloader import Image_Downloader
from dataset_builder.image_recognition.image_tags_extractor import Image_Tags_Extractor
from dataset_builder.lda_topic_model import LDATopicModel
from dataset_builder.bag_of_words_graph_builder import GraphBuilder_Bag_Of_Words
from dataset_builder.citation_graph_builder import GraphBuilder_Citation
from dataset_builder.cocitation_graph_builder import GraphBuilder_CoCitation
from dataset_builder.common_posts_graph_builder import GraphBuilder_Common_Posts
from dataset_builder.feature_extractor.feature_extractor import FeatureExtractor
from dataset_builder.feature_similarity_graph_builder import GraphBuilder_Feature_Similarity
from dataset_builder.ocr_extractor import OCR_Extractor
from dataset_builder.randomizer.randomizer import Randomizer
from dataset_builder.topic_distribution_builder import TopicDistributionBuilder
from dataset_builder.topics_graph_builder import GraphBuilder_Topic
from dataset_builder.word_embedding.gensim_word_embedding_trainer import GensimWordEmbeddingsModelTrainer
from dataset_builder.word_embedding.glove_word_embedding_model_creator import GloveWordEmbeddingModelCreator
from experimental_environment.experimental_environment import ExperimentalEnvironment
from experimental_environment.kernel_performance_evaluator import Kernel_Performance_Evaluator
from experimental_environment.knnwithlinkprediction import KNNWithLinkPrediction
from experimental_environment.linkprediction_evaluator import LinkPredictionEvaluator
from experimental_environment.load_datasets import Load_Datasets
from experimental_environment.predictor import Predictor
from missing_data_complementor.missing_data_complementor import MissingDataComplementor
from preprocessing_tools.create_authors_table import CreateAuthorTables
from preprocessing_tools.csv_importer import CsvImporter
from preprocessing_tools.data_preprocessor import Preprocessor
from preprocessing_tools.gdlet_news_importer.gdlet_news_importer import GDLET_News_Importer
from preprocessing_tools.json_importer.json_importer import JSON_Importer
from preprocessing_tools.post_citation_creator import PostCitationCreator
from preprocessing_tools.rank_app_importer import RankAppImporter
from preprocessing_tools.xml_importer import XMLImporter
from topic_distribution_visualization.claim_to_topic_converter import ClaimToTopicConverter
from topic_distribution_visualization.topic_distribution_visualization_generator import \
    TopicDistrobutionVisualizationGenerator
from twitter_crawler.twitter_crawler import Twitter_Crawler
#from preprocessing_tools.google_link_by_keyword_importer.google_link_importer import GoogleLinksByKeywords
#rom preprocessing_tools.instagram_crawler.instagram_crawler import InstagramCrawler


###############################################################
# MODULES
###############################################################

modules_dict = {}
modules_dict["DB"] = DB  ## DB is special, it cannot be created using db.
modules_dict["XMLImporter"] = XMLImporter
modules_dict["CreateAuthorTables"] = CreateAuthorTables
modules_dict["RankAppImporter"] = RankAppImporter
modules_dict["JSON_Importer"] = JSON_Importer
modules_dict["CsvImporter"] = CsvImporter
modules_dict["CsvToTableImporter"] = CsvToTableImporter
modules_dict["FakeNewsSnopesImporter"] = FakeNewsSnopesImporter
modules_dict["GDLET_News_Importer"] = GDLET_News_Importer
modules_dict["Twitter_Crawler"] = Twitter_Crawler
#modules_dict["GoogleLinksByKeywords"] = GoogleLinksByKeywords
#modules_dict["InstagramCrawler"] = InstagramCrawler
modules_dict["OldTweetsCrawler"] = OldTweetsCrawler
modules_dict["MissingDataComplementor"] = MissingDataComplementor
modules_dict["Load_Datasets"] = Load_Datasets
modules_dict["Image_Tags_Extractor"] = Image_Tags_Extractor
modules_dict["Image_Downloader"] = Image_Downloader
modules_dict["Preprocessor"] = Preprocessor
modules_dict["ClaimToTopicConverter"] = ClaimToTopicConverter
modules_dict["LDATopicModel"] = LDATopicModel
modules_dict["GloveWordEmbeddingModelCreator"] = GloveWordEmbeddingModelCreator
modules_dict["GensimWordEmbeddingsModelTrainer"] = GensimWordEmbeddingsModelTrainer
modules_dict["OCR_Extractor"] = OCR_Extractor
modules_dict["TopicDistributionBuilder"] = TopicDistributionBuilder
modules_dict["PostCitationCreator"] = PostCitationCreator
modules_dict["CommentManager"] = CommentManager
modules_dict["EchoManager"] = EchoManager
modules_dict["InfluenceManager"] = InfluenceManager
modules_dict["PostManager"] = PostManager
modules_dict["GraphBuilder_Citation"] = GraphBuilder_Citation
modules_dict["GraphBuilder_CoCitation"] = GraphBuilder_CoCitation
modules_dict["Randomizer"] = Randomizer
modules_dict["GraphBuilder_Topic"] = GraphBuilder_Topic
modules_dict["Topic_Graph_Builder_All_Combinations"] = Topic_Graph_Builder_All_Combinations
modules_dict["Topic_Graph_Builder_Random_Combinations"] = Topic_Graph_Builder_Random_Combinations
modules_dict["Topic_Graph_Builder_K_Best"] = Topic_Graph_Builder_K_Best
modules_dict["Bag_Of_Words_Graph_Builder_K_Best"] = Bag_Of_Words_Graph_Builder_K_Best
modules_dict["Bag_Of_Words_Graph_Builder_All_Combinations"] = Bag_Of_Words_Graph_Builder_All_Combinations
modules_dict["Bag_Of_Words_Graph_Builder_Random_Combinations"] = Bag_Of_Words_Graph_Builder_Random_Combinations
modules_dict["GraphBuilder_Word_Embedding"] = GraphBuilder_Word_Embedding
modules_dict["GraphBuilder_Feature_Similarity"] = GraphBuilder_Feature_Similarity
modules_dict["GraphBuilder_Common_Posts"] = GraphBuilder_Common_Posts
modules_dict["GraphBuilder_Followers"] = GraphBuilder_Followers
modules_dict["GraphBuilder_Bag_Of_Words"] = GraphBuilder_Bag_Of_Words
modules_dict["FeatureExtractor"] = FeatureExtractor
modules_dict["BadActorsCollector"] = BadActorsCollector
modules_dict["DataExporter"] = DataExporter
modules_dict["LinkPredictionEvaluator"] = LinkPredictionEvaluator
modules_dict["KNNWithLinkPrediction"] = KNNWithLinkPrediction
modules_dict["Predictor"] = Predictor
modules_dict["Kernel_Performance_Evaluator"] = Kernel_Performance_Evaluator
modules_dict["ExperimentalEnvironment"] = ExperimentalEnvironment
modules_dict["TopicDistrobutionVisualizationGenerator"] = TopicDistrobutionVisualizationGenerator

###############################################################
## SETUP
logging.config.fileConfig(getConfig().get("DEFAULT", "Logger_conf_file"))
config = getConfig()
domain = unicode(config.get("DEFAULT", "domain"))
logging.info("Start Execution ... ")
logging.info("SETUP global variables")

window_start = getConfig().eval("DEFAULT", "start_date")
newbmrk = os.path.isfile("benchmark.csv")
bmrk_file = file("benchmark.csv", "a")
bmrk_results = csv.DictWriter(bmrk_file,
                              ["time", "jobnumber", "config", "window_size", "window_start", "dones", "posts",
                               "authors"] + modules_dict.keys(),
                              dialect="excel", lineterminator="\n")

if not newbmrk:
    bmrk_results.writeheader()

logging.info("CREATE pipeline")
db = DB()
modules_dict["DB"] = lambda x: x
pipeline = []
for module in getConfig().sections():
    parameters = {}
    if modules_dict.get(module):
        pipeline.append(modules_dict.get(module)(db))

logging.info("SETUP pipeline")
bmrk = {"config": getConfig().getfilename(), "window_start": "setup"}

for module in pipeline:
    logging.info("setup module: {0}".format(module))
    T = time.time()
    module.setUp()
    T = time.time() - T
    bmrk[module.__class__.__name__] = T

bmrk_results.writerow(bmrk)
bmrk_file.flush()

clean_authors_features = getConfig().eval("DatasetBuilderConfig", "clean_authors_features_table")
if clean_authors_features:
    db.delete_authors_features()

#check defenition
logging.info("checking module definition")
for module in pipeline:
    if not module.is_well_defined():
        raise Exception("module: "+ module.__class__.__name__ +" config not well defined")
    logging.info("module "+str(module) + " is well defined")

###############################################################
## EXECUTE
bmrk = {"config": getConfig().getfilename(), "window_start": "execute"}
for module in pipeline:
    logging.info("execute module: {0}".format(module))
    T = time.time()
    logging.info('*********Started executing ' + module.__class__.__name__)

    module.execute(window_start)

    logging.info('*********Finished executing ' + module.__class__.__name__)
    T = time.time() - T
    bmrk[module.__class__.__name__] = T

num_of_authors = db.get_number_of_targeted_osn_authors(domain)
bmrk["authors"] = num_of_authors

num_of_posts = db.get_number_of_targeted_osn_posts(domain)
bmrk["posts"] = num_of_posts

bmrk_results.writerow(bmrk)
bmrk_file.flush()

if __name__ == '__main__':
    pass
