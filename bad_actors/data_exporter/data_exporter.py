# Created by jorgeaug
# Time: 13/07/2016 12:52
from arff_writer import ArffWriter
import logging
from configuration.config_class import getConfig
from preprocessing_tools.abstract_executor import AbstractExecutor


class DataExporter(AbstractExecutor):
    def __init__(self, db):
        AbstractExecutor.__init__(self, db)
        self.output_filename = self._config_parser.eval(self.__class__.__name__, "arff_file")
        self._author_type_classes = self._config_parser.eval(self.__class__.__name__, "author_type_classes")

    def setUp(self):
        pass

    def execute(self, window_start):
        logging.info("Data exporter started !!!!")
        writer = ArffWriter(self._db, self._author_type_classes)
        writer.write_author_features_to_arff(self.output_filename)
        logging.info("Data exporter Finished !!!!")

