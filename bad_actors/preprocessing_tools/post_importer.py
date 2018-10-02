# Created by aviade      
# Time: 02/05/2016 15:00

from __future__ import print_function
import datetime
from abstract_executor import AbstractExecutor
from commons.commons import *
import time

import logging
import datetime
from DB.schema_definition import *
import timeit


class PostImporter(AbstractExecutor):  # define child class
    # @review: abstract class

    def __init__(self, db):
        AbstractExecutor.__init__(self, db)

        self._listdic = []
        self._author_classify_dict = {}
        'should place the key-values from posts to put in author'
        self._author_prop_dict = {}

    def insertPostsIntoDB(self):

        startInsetToDB = timeit.default_timer()
        postList = []
        updatepostList = []
        postsRefsDictList = []
        listdic = self._listdic
        logging.info("total Posts: " + str(len(listdic)))
        i = 1
        for dictItem in listdic:
            msg = "\r Import post from XML: [{}".format(i) + "/" + str(len(listdic)) + ']'
            print(msg, end="")
            i += 1
            post, xml_importer_insertion_date = self.create_post(dictItem)

            if not self._db.isPostExist(cleaner(post.url), format(post.guid)):

                self._update_existing_post(dictItem, post, postList, postsRefsDictList)
            else:
                self._create_new_post(dictItem, post, postsRefsDictList, updatepostList, xml_importer_insertion_date)
        print("")
        self._db.addPosts(postList)
        # self._db.updatePosts(updatepostList)

        stopInsertToDB = timeit.default_timer()
        self.logger.debug("total time write post from single file to DB: {0}".format(stopInsertToDB - startInsetToDB))

        startInsetToREFList = timeit.default_timer()
        referencesList = self.fromPostsRefsDictListToRefsList(postsRefsDictList)
        stopInsertToREFList = timeit.default_timer()
        self.logger.debug(
            "total time fromPostsRefsDictListToRefsList: {0}".format(stopInsertToREFList - startInsetToREFList))
        startInsetToREF = timeit.default_timer()
        self._db.addReferences(referencesList)
        stopInsertToREF = timeit.default_timer()
        self.logger.debug("total time write REF from single file to DB {0}".format(stopInsertToREF - startInsetToREF))
        self.logger.debug("postMaxDate = {0}".format(self._db.getPostsMaxDate()))

    def _create_new_post(self, dictItem, post, postsRefsDictList, updatepostList, xml_importer_insertion_date):
        is_detiled_post = False
        if (self._db.isPostNotDetailed(cleaner(post.url), format(post.guid))):

            tmp = []
            if (dictItem.get('guid')):
                xml_importer_insertion_date = unicode(get_current_time_as_string())
                is_detiled_post = True

            date = convert_str_to_unicode_datetime(dictItem.get('date'))

            dict = {'guid': unicode(dictItem.get('guid')), 'title': unicode(dictItem.get('title')),
                    'date': date,
                    'content': unicode(dictItem.get('content')), 'author': unicode(dictItem.get('author')),
                    'is_detailed': is_detiled_post, 'is_LB': False, 'domain': unicode(dictItem.get('domain')),
                    'author_guid': unicode(dictItem.get('author_guid')),
                    'xml_importer_insertion_date': xml_importer_insertion_date}
            tmp.append(unicode(post.url))
            tmp.append(dict)
            updatepostList.append(tmp)

            referencesStr = dictItem.get('references')
            if referencesStr != "" or (referencesStr):

                referencesSplited = referencesStr.split('|')
                postRefsDictList = []
                for ref in referencesSplited:
                    refDictItem = {}
                    refDictItem.update({'urlfrom': unicode(post.url)})
                    refDictItem.update({'urlto': unicode(ref)})
                    postRefsDictList.append(refDictItem)
                postsRefsDictList.append(postRefsDictList)

    def _update_existing_post(self, dictItem, post, postList, postsRefsDictList):
        postList.append(post)
        referencesStr = dictItem.get('references')
        if referencesStr != "":

            referencesSplited = referencesStr.split('|')
            postRefsDictList = []
            for ref in referencesSplited:
                refDictItem = {}
                refDictItem.update({'urlfrom': unicode(post.url)})
                refDictItem.update({'urlto': unicode(ref)})
                postRefsDictList.append(refDictItem)
            postsRefsDictList.append(postRefsDictList)

    def create_post(self, dictItem):
        url = unicode(dictItem.get('url'))
        author_name = unicode(dictItem.get('author'))
        publication_date = dictItem.get('date')
        post_id = dictItem.get('post_id')
        if post_id is None:
            post_id = compute_post_guid(url, author_name, publication_date).replace('-', '')
        xml_importer_insertion_date = unicode(get_current_time_as_string())
        date = convert_str_to_unicode_datetime(dictItem.get('date'))
        content = unicode(dictItem.get('content'))
        domain = unicode(dictItem.get('domain'))
        author_guid = unicode(dictItem.get('author_guid'))
        title = self._verify_value(dictItem, 'title')
        post_type = self._verify_value(dictItem, 'post_type')
        post = Post(post_id=unicode(post_id), guid=unicode(dictItem.get('guid')), title=title, url=url, date=date,
                    content=content, author=author_name,
                    domain=domain, author_guid=author_guid, post_type=post_type,
                    xml_importer_insertion_date=xml_importer_insertion_date)
        return post, xml_importer_insertion_date

    def fromPostsRefsDictListToRefsList(self, postsRefsDictList):
        referencesList = []
        newpostslist = []
        urls = []
        i = 1
        for postRefs in postsRefsDictList:
            msg = "\r Possessing post ref: [{}".format(i) + "/" + str(len(postsRefsDictList)) + ']'
            print(msg, end="")
            i += 1
            for refDictItem in postRefs:
                urlfrom = unicode(refDictItem.get('urlfrom'))
                postfrom = self._db.getPostUsingURL(urlfrom)
                idfrom = unicode(postfrom[0].post_id)
                date = postfrom[0].date
                urlto = unicode(refDictItem.get('urlto'))
                if self._db.isRefExist(urlto) == 0:
                    if (urlto not in urls):
                        xml_importer_insertion_date = str_to_date(get_current_time_as_string())
                        urls.append(urlto)
                        post_id = unicode(generate_random_guid())
                        post = Post(post_id=unicode(post_id), url=unicode(urlto), date=date, is_detailed=False,
                                    is_LB=False, xml_importer_insertion_date=xml_importer_insertion_date)
                        newpostslist.append(post)

        if len(postsRefsDictList) != 0: print("")
        self._db.addPosts(list(set(newpostslist)))

        for postRefs in postsRefsDictList:
            for refDictItem in postRefs:
                urlfrom = unicode(refDictItem.get('urlfrom'))
                postfrom = self._db.getPostUsingURL(urlfrom)
                idfrom = postfrom[0].post_id
                urlto = unicode(refDictItem.get('urlto'))

                idto = self._db.getPostUsingURL(urlto)[0].post_id
                reference = Post_citation(post_id_from=idfrom, post_id_to=idto, url_from=urlfrom, url_to=urlto)
                referencesList.append(reference)

        return referencesList

    def _add_property_to_author_prop_dict(self, author_guid, key, value):
        if value is not None:
            if author_guid not in self._author_prop_dict:
                self._author_prop_dict[author_guid] = {}
            self._author_prop_dict[author_guid][key] = value

    def _verify_value(self, dictItem, key):
        value = dictItem.get(key)
        if value is not None:
            return unicode(value)
        return value
