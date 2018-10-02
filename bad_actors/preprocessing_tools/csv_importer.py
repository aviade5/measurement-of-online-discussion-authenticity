# encoding: CP1252
# need to be added to the system

import csv
from os import listdir

import sys

from DB.schema_definition import *
from commons.commons import *
from post_importer import PostImporter


class CsvImporter(PostImporter):
    def __init__(self, db):
        PostImporter.__init__(self, db)
        config_parser = getConfig()
        self.start_date = config_parser.eval("DEFAULT", "start_date")
        self.end_date = config_parser.eval("DEFAULT", "end_date")
        self._data_folder = self._config_parser.eval(self.__class__.__name__, "data_folder")


    def execute(self, window_start=None):
        self.readFromFolders()
        self._db.insert_or_update_authors_from_posts(self._domain, self._author_classify_dict, self._author_prop_dict)

    def readFromFolders(self):
        all_csv_files = listdir(self._data_folder)

        csv.field_size_limit(sys.maxint)
        for csv_file in all_csv_files:
            f = open(self._data_folder + csv_file)
            logging.info("IMPORT CSV %s" % f)
            self.parse_csv(csv_file, f)

        print ("Insert posts to DB")
        self.insertPostsIntoDB()

    def parse_csv(self, csv_file, f):
        # try:
            reader = csv.DictReader(f, delimiter=',')
            for row in reader:
                post_dict = self.create_post_dict_from_row(row)
                self._listdic.append(post_dict.copy())

        # except:
        #     self.logger.error("Cant encode the post:{0}".format(csv_file))
        #     pass

    def create_post_dict_from_row(self, row):
        guid = unicode(generate_random_guid())
        post_dict = {}
        post_dict["content"] = unicode(row["text"])
        post_dict["date"] = unicode(row["created"])
        post_dict["guid"] = guid.replace('-', '')
        post_dict["author"] = unicode(row["screenName"])
        author_guid = unicode(compute_author_guid_by_author_name(row["screenName"]).replace('-', ''))
        post_dict["author_guid"] = author_guid
        post_dict["references"] = u""
        post_dict["domain"] = self._domain
        post_dict["author_osn_id"] = unicode(row["id"])
        post_dict["url"] = unicode("https://twitter.com/{0}/status/{1}".format(post_dict["author"], row["id"]))
        return post_dict
