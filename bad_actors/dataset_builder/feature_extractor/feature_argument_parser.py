from __future__ import print_function
from collections import defaultdict

from DB.schema_definition import Author
from preprocessing_tools.abstract_controller import AbstractController


class ArgumentParser(AbstractController):
    def _get_source_id_target_elements(self, args):
        if 'connection' in args and args['connection'] != {}:
            source_id_target_elements_dict = self._get_source_id_target_elements_dict_by_connection(args)
        else:
            source_id_target_elements_dict = self._get_source_id_target_elements_dict_by_source(args)
        return source_id_target_elements_dict

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