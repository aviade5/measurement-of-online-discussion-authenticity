# Created by Jorge Bendahan (jorgeaug@post.bgu.ac.il) at 10/04/2016
# Ben Gurion University of the Neguev - Department of Information Systems Engineering

import logging
import time

from account_properties_feature_generator import AccountPropertiesFeatureGenerator
from behavior_feature_generator import BehaviorFeatureGenerator
from configuration.config_class import getConfig
from distances_from_targeted_class_feature_generator import DistancesFromTargetedClassFeatureGenerator
from graph_feature_generator import GraphFeatureGenerator
from link_prediction_feature_extractor import LinkPredictionFeatureExtractor
from preprocessing_tools.abstract_executor import AbstractExecutor
from syntax_feature_generator import SyntaxFeatureGenerator
'''
This class is responsible for executing all the FeatureGenerator classes and finally calling ArffWriter class which
writes all the generated features to an output file
'''


class FeatureExtractor(AbstractExecutor):
    def __init__(self, db, **kwargs):
        AbstractExecutor.__init__(self, db)
        self.config_parser = getConfig()
        self._pipeline = []
        # self._table_name = self._config_parser.get(self.__class__.__name__, "table_name")

    def setUp(self):

        ###############################################################
        # MODULES
        ###############################################################
        module_dict = {}

        module_dict["SyntaxFeatureGenerator"] = SyntaxFeatureGenerator
        module_dict["BehaviorFeatureGenerator"] = BehaviorFeatureGenerator
        module_dict["GraphFeatureGenerator_1"] = GraphFeatureGenerator
        module_dict["AccountPropertiesFeatureGenerator"] = AccountPropertiesFeatureGenerator
        module_dict["GraphFeatureGenerator_2"] = GraphFeatureGenerator
        module_dict["DistancesFromTargetedClassFeatureGenerator"] = DistancesFromTargetedClassFeatureGenerator

        # LinkPredictionFeatureExtractor must be the latest. Due to the deletion of features of the anchor authors.
        module_dict["LinkPredictionFeatureExtractor"] = LinkPredictionFeatureExtractor

        ###############################################################
        ## SETUP
        logging.config.fileConfig(getConfig().get("DEFAULT", "Logger_conf_file"))
        logging.info("Start Execution ... ")
        logging.info("SETUP global variables")
        window_start = getConfig().eval("DEFAULT", "start_date")
        logging.info("CREATE pipeline")


        authors = self._db.get_authors_by_domain(self._domain)
        posts = self._db.get_posts_by_domain(self._domain)
        graphs = {}

        parameters = {"authors": authors, "posts": posts, "graphs": graphs}



        for module in self._config_parser.sections():
            if module_dict.get(module):
                if module.startswith("GraphFeatureGenerator") or module.startswith("DistancesFromTargetedClassFeatureGenerator"):
                    self._add_graph_features_to_params(module, parameters)

                self._pipeline.append(module_dict.get(module)(self._db, **parameters))


    def execute(self, window_start=None, window_end=None):
        start_time = time.time()
        info_msg = "execute started for " + self.__class__.__name__ + " started at " + str(start_time)

        print info_msg
        logging.info(info_msg)

        for module in self._pipeline:
            logging.info("execute module: {0}".format(module))
            T = time.time()
            logging.info('*********Started executing ' + module.__class__.__name__)

            module.execute()

            logging.info('*********Finished executing ' + module.__class__.__name__)
            T = time.time() - T



        end_time = time.time()
        diff_time = end_time - start_time
        info_msg = "execute finished for " + self.__class__.__name__ + "  in " + str(diff_time)+ " seconds"
        print info_msg
        logging.info(info_msg)

    def _add_graph_features_to_params(self, module, parameters):

        graph_types = self._config_parser.eval(module, "graph_types")
        graph_directed = self._config_parser.eval(module, "graph_directed")
        graph_weights = self._config_parser.eval(module, "graph_weights")
        parameters.update({"graph_types": graph_types,
                           "graph_directed": graph_directed, "graph_weights": graph_weights})

        if self._config_parser.has_option(module, "algorithms"):
            algorithms = self._config_parser.eval(module, "algorithms")
            parameters.update({"algorithms": algorithms})

        if self._config_parser.has_option(module, "aggregation_functions"):
            aggregations = self._config_parser.eval(module, "aggregation_functions")
            parameters.update({"aggregation_functions": aggregations})

        if self._config_parser.has_option(module, "neighborhood_sizes"):
            neighborhood_sizes = self._config_parser.eval(module, "neighborhood_sizes")
            parameters.update({"neighborhood_sizes": neighborhood_sizes})

    def is_well_defined(self):
        #check module definition
        logging.info("start checking feature extractors definition")
        for module in self._pipeline:
            if not module.is_well_defined():
                raise Exception("module:"+ module.__class__.__name__ +" config not well defined")
        return True