from __future__ import print_function
from collections import defaultdict
import os
from DB.schema_definition import Author
from preprocessing_tools.abstract_controller import AbstractController
import pandas as pd


class ArgumentParser(AbstractController):
    def _get_source_id_target_elements(self, args):
        if 'connection' in args and args['connection'] != {}:
            source_id_target_elements_dict = self._get_source_id_target_elements_dict_by_connection(args)

            # self._write_statistics(args, source_id_target_elements_dict)
        else:
            source_id_target_elements_dict = self._get_source_id_target_elements_dict_by_source(args)
        return source_id_target_elements_dict

    def _write_statistics(self, args, source_id_target_elements_dict):
        print('Write received data by source destination dict to csv named: "source_destination_statistics.csv"')
        source_to_element = self._get_source_id_source_element_dict(source_id_target_elements_dict.keys(), args)
        rows = []
        dest_id_field = args['destination']['id']
        for source_id, elements in source_id_target_elements_dict.iteritems():
            source_date = None
            if args['source']['table_name'] == 'posts':
                source_date = getattr(source_to_element[source_id], 'created_at')
            author_guid = getattr(source_to_element[source_id], 'author_guid')
            for element in elements:
                element_date = None
                element_id = getattr(element, dest_id_field)
                if args['destination']['table_name'] == 'posts':
                    element_date = getattr(element, 'date')
                connection_conditions = []
                if 'where_clauses' in args['connection']:
                    connection_conditions = map(str, args['connection']['where_clauses'])
                rows.append((source_id, source_date, element_id, element_date, author_guid, str(connection_conditions)))
        if rows:
            df = pd.DataFrame(rows)
            output_path = 'data/output/statistics/{0}_statistics'.format(self.__class__.__name__)
            if not os.path.exists(output_path):
                os.makedirs(output_path)
            df.to_csv(output_path + '/source_destination_statistics.csv', mode='a',
                      header=['claim_guid', 'publication_date', 'post_guid', 'date', 'author_guid', 'conditions'])

    def _get_source_id_target_elements_dict_by_connection(self, args):
        source_id_target_element_tuples = self._db.get_elements_by_args(args)
        source_id_target_elements_dict = defaultdict(list)
        i = 1
        print()
        for source_id, target_element in source_id_target_element_tuples:
            msg = "\r load source data {0}".format(str(i))
            print(msg, end="")
            i += 1
            source_id_target_elements_dict[source_id].append(target_element)
        print()
        return source_id_target_elements_dict

    def _get_source_id_target_elements_dict_by_source(self, arg):
        data_table_name = arg['source']['table_name']
        data_id = arg['source']['id']
        source_id_target_elements_dict = defaultdict(list)
        where_clauses = []
        if 'where_clauses' in arg['source']:
            where_clauses = arg['source']['where_clauses']

        target_elements = self._db.get_table_elements_by_where_cluases(data_table_name, where_clauses)
        for element in target_elements:
            source_id = getattr(element, data_id)
            source_id_target_elements_dict[source_id].append(element)
        return source_id_target_elements_dict

    def _get_connection_tuples(self, args):
        connection_table_name = args['connection']['table_name']
        connection_source_id = args['connection']['source_id']
        connection_targeted_id = args['connection']['target_id']
        connection_tuples = self._db.get_records_by_id_targeted_field_and_table_name(connection_source_id,
                                                                                     connection_targeted_id,
                                                                                     connection_table_name,
                                                                                     {})
        return connection_tuples

    def _get_source_ids_set(self, args):
        source_table = args['source']['table_name']
        source_id = args['source']['id']
        where_clauses = {}
        if 'where_clauses' in args['source']:
            where_clauses = args['source']['where_clauses']
        source_table_elements = self._db.get_table_elements_by_where_cluases(source_table, where_clauses)

        return set([getattr(element, source_id) for element in source_table_elements])

    def _get_source_id_source_element_dict(self, source_ids, targeted_fields_dict):
        source_id_source_element_dict = defaultdict()

        source_table_name = targeted_fields_dict['source']['table_name']
        source_table_id = targeted_fields_dict['source']['id']
        elements = self._db.get_table_elements_by_where_cluases(source_table_name, [])
        author_guid_author_dict = self._db.get_author_dictionary()
        id_set = set(source_ids)
        for temp_author in elements:
            source_id = getattr(temp_author, source_table_id)
            if source_id not in id_set or source_id is None:
                continue
            if isinstance(temp_author, Author):
                author = author_guid_author_dict[temp_author.author_guid]
            elif source_table_id == u"author_guid":
                author = author_guid_author_dict[source_id]
            elif hasattr(temp_author, u"author_guid"):
                if getattr(temp_author, u"author_guid") in author_guid_author_dict:
                    author = author_guid_author_dict[getattr(temp_author, u"author_guid")]
                else:
                    author = self._create_defult_author(source_id, targeted_fields_dict, temp_author)
            else:
                # author = self._create_defult_author(source_id, targeted_fields_dict, temp_author)
                author = temp_author
            source = author
            source_id_source_element_dict[source_id] = source
        return source_id_source_element_dict

    def _create_defult_author(self, source_id, targeted_fields_dict, temp_author):
        author = Author()
        author.author_guid = source_id
        author.statuses_count = len(targeted_fields_dict)
        if hasattr(temp_author, 'date'):
            author.created_at = getattr(temp_author, 'date')
        return author