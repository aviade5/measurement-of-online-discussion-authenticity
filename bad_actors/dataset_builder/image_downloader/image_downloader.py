from __future__ import print_function

import os

from preprocessing_tools.abstract_controller import AbstractController
import urllib2
import sys

__author__ = "Aviad Elyashar"

#########
#This module is responsible to download an image using given url and save it
# in designated folder and to update posts table.
#########
class Image_Downloader(AbstractController):
    def __init__(self, db):
        AbstractController.__init__(self, db)
        self._path_for_downloaded_images = self._config_parser.eval(self.__class__.__name__, "path_for_downloaded_images")

    def setUp(self):
        if not os.path.exists(self._path_for_downloaded_images):
            os.makedirs(self._path_for_downloaded_images)

    def execute(self, window_start):
        self._download_images_from_posts()

    def _download_images_from_posts(self):
        posts = self._db.get_posts()
        sources = [(p.post_id, p.source_url) for p in posts]
        self._generic_download_images(sources, 'media_path', 'posts')

    def _generic_download_images(self, id_img_url_tuples, target_field_name, target_table_name):

        objects_to_update = []
        if target_table_name == "posts":
            # post_id_post_dictionary
            object_id_object_dict = self._db.get_post_dictionary()
        else:
            # author_id_author_dictionary
            object_id_object_dict = self._db.get_author_dictionary()
        i = 0
        post_counter = 0
        for source_tuple in id_img_url_tuples:
            i += 1
            # msg = "\r Num of downloaded images: {0}".format(i)
            # print(msg, end="")
            source_tuple_id = source_tuple[0]
            source_url = source_tuple[1]
            target_file = self._path_for_downloaded_images + source_tuple_id + ".jpg"

            if source_url is not None and len(source_url) > 0:
                try:
                    opener = urllib2.build_opener()

                    opener.addheaders = [('User-Agent', 'Mozilla/5.0')]
                    resource = opener.open(source_url)

                    output = open(target_file, "wb")
                    output.write(resource.read())
                    output.close()

                    object = object_id_object_dict[source_tuple_id]
                    setattr(object, target_field_name, source_tuple_id + ".jpg")

                    objects_to_update.append(object)
                    post_counter += 1
                    msg = "\r Num of downloaded images: {0}".format(post_counter)
                    print(msg, end="")

                    if post_counter == 1000:
                        self._db.addPosts(objects_to_update)
                        objects_to_update = []
                        post_counter = 0

                except:
                    print("Unexpected error:", sys.exc_info()[0])
        self._db.addPosts(objects_to_update)

    def download_image(self, img_url):
        pass

