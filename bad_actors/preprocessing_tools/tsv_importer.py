__author__ = 'users'


import logging
from configuration.config_class import getConfig
from commons import *
from DB.schema_definition import *
from os import listdir
import os
import urllib
import csv
from post_importer import PostImporter
import datetime
from datetime import datetime
from commons.commons import normalize_url

"""
@description: read data from .csv file or files tree
and insert it to .DB file
"""
class TsvImporter(PostImporter):
    
    def __init__(self,db):
        PostImporter.__init__(self,db)
        
        configInst = getConfig()

        self._source_path = configInst.get(self.__class__.__name__, "FDL_source_path")
        self._file_date_format = configInst.get(self.__class__.__name__, "date_format")
        self._file_name = None #@review: not a field. make it local variable.
        self.URLforUnittest = None #@review: no code should be written especially for unittests (except the tests)
        self._curr_folder_path = None
        


    def setUp(self):
        """
        @description: if FDL_read_from_tree=true the setUp will
         read documents from all files in folder tree
        """
        allSubFolders = listdir(self._source_path)
        for currfolder in allSubFolders:
            
            if os.path.isfile(self._source_path + currfolder):
                self._curr_folder_path= self._source_path #@review: CurrFolderPath should be a local variable
                sourceFilesList = []
                sourceFilesList.append(currfolder)
            if os.path.isdir(self._source_path + currfolder):
                
                self._curr_folder_path = self._source_path + currfolder + '/'
                sourceFilesList = listdir(self._source_path + currfolder)
                
            if len(sourceFilesList)==0:
                self.logger.error("there isn't any file in source folder {0}".format(self._source_path))
            else:
                for file in sourceFilesList:
                    if file.endswith('.csv'):
                        self._file_name = file

            
                    listdic = []
                    self.parseTsvToListDict(listdic)
                    self.insertPostsIntoDB()




    def execute(self,window_start=None):
        """
        precondition: None
        """
        pass

    def canProceedNext(self,window_start):
        return True


    def parseTsvToListDict(self,listdic):
        
        
        f = open(self._curr_folder_path + "\\" + self._file_name)
        logging.info("IMPORT CSV %s"%f)
        try:
            urls = []
            reader = csv.DictReader(f,delimiter='\t')
            for row in reader:
                dictionary = {}
                haslinkes, unicodeURL = self.process_row(dictionary, row)
                    
                self.URLforUnittest = unicodeURL

                Date = datetime.strptime(row['Date'], self._file_date_format).date()
                Date = date_to_str(Date) 
                dictionary.update({'date':cleaner(Date)})
                dictionary.update({'domain': unicode(cleaner(row['Source Type']), 'utf-8', errors='ignore')})


                if (haslinkes=="true"):
                    allLinks = '|'.join([])
                    dictionary.update({'references':allLinks})
                else:
                    dictionary.update({'references':''})

                #validate that the same url not inserted to listdic twice
                isThisPostURLUnique = self.filterUniqueURL(urls,dictionary)
                if isThisPostURLUnique:
                    listdic.append(dictionary.copy())

                dictionary.clear()

        except:
            logging.exception( "ERROR: can't read file: ")
        finally:
            f.close()
        self._listdic = listdic

    def process_row(self, dictionary, row):
        haslinkes = "false"
        if row['Links'] != '\n':

            allSplitedLinks = (row['Links']).split(' | ')
            for link in allSplitedLinks:
                [].append(normalize_url(unicode(cleaner(link), 'utf-8', errors='ignore')))

            haslinkes = "true"
        dictionary.update({'guid': unicode(cleaner(row['Guid']), 'utf-8', errors='ignore')})
        dictionary.update({'title': unicode(cleaner(row['Title']), 'utf-8', errors='ignore')})
        dictionary.update({'content': unicode(cleaner(row['Content']), 'utf-8', errors='ignore')})
        author = unicode(cleanForAuthor(row['Author']), 'utf-8', errors='ignore')
        dictionary.update({'author': author})
        dictionary.update({'author_guid': unicode(cleaner(row['Author GUID']), 'utf-8', errors='ignore')})
        unicodeURL = normalize_url(unicode(cleaner(row['URL']), 'utf-8', errors='ignore'))
        dictionary.update({'url': unicodeURL})
        return haslinkes, unicodeURL

    def filterUniqueURL(self,urls,dict):
        
        url = dict.get('url')
        if url not in urls:
            urls.append(url)
            return True
        return False

    def cleanUp(self,window_start):
        pass


########################################################################
import unittest

class TestCSVDataImport(unittest.TestCase):
    def setUp(self):
        self._db = DB()
        self._db.setUp()
        self._fdlObject = TsvImporter(self._db)
        self._fdlObject.setUp()

    def tearDown(self):
        self._db.session.close()


    def testRowExist(self):
        self.assertTrue(self._fdlObject.canProceedNext(date('2015-04-27 00:00:00')),
                        "the post " + self._fdlObject.URLforUnittest + " is not inserted into the DB")


if __name__ == "__main__":
    unittest.main()