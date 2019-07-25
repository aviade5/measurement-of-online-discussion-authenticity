from __future__ import print_function
import logging
import numpy as np
import nltk
from nltk import word_tokenize, map_tag
from nltk.corpus import stopwords
from nltk.corpus import words
from collections import *
from base_feature_generator import BaseFeatureGenerator
from commons import commons
import timeit
from operator import itemgetter


class Text_Anlalyser_Feature_Generator(BaseFeatureGenerator):
    def __init__(self, db, **kwargs):
        BaseFeatureGenerator.__init__(self, db, **kwargs)
        nltk.download('words')
        nltk.download('punkt')
        nltk.download('averaged_perceptron_tagger')
        nltk.download('universal_tagset')
        self._targeted_fields = self._config_parser.eval(self.__class__.__name__, "targeted_fields")
        self._load_stop_words()
        self._words = words.words()
        self._author_id_texts_dict = {}
        self._features = self._config_parser.eval(self.__class__.__name__, "feature_list")
        self._authors = []
        self._multi_field_features = []
        self._authors = self._db.get_author_guid_to_author_dict()

    def execute(self, window_start=None):
        self._execute_single_target_field_execute()
        try:
            self._execute_multi_target_fields_execute()
        except Exception as e:
            logging.info("didn't use execute multi target field")

    def _execute_single_target_field_execute(self):
        for targeted_fields_dict in self._targeted_fields:
            author_id_texts_dict = self.get_source_id_destination_target_field(targeted_fields_dict)
            counter = 0
            total_authors_features = []

            # summary_times = []
            for source_id, target_fields in author_id_texts_dict.iteritems():
                print("\rprocessing author {}/{}".format(str(counter), len(author_id_texts_dict)), end="")
                counter += 1
                for feature_name in self._features:
                    kwargs = {'source_id': source_id, 'target_fields': list(target_fields)}
                    author_feature = self.run_and_create_author_feature(kwargs, source_id, feature_name)
                    if author_feature:
                        total_authors_features.append(author_feature)
            logging.info("processing author " + str(counter))
            self.insert_author_features_to_db(total_authors_features)
            # self._db.add_author_features_fast(authors_features)

    def _execute_multi_target_fields_execute(self):
        counter = 0
        self._multi_field_features = self._config_parser.eval(self.__class__.__name__, u'multi_feature_target_fields')
        self._features = self._config_parser.eval(self.__class__.__name__, u'multi_feature_feature_list')
        self._multi_field_feature_functions = self._config_parser.eval(self.__class__.__name__,
                                                                       u'multi_feature_function_names')
        for multi_feald_feature in self._multi_field_features:
            feature_field1 = multi_feald_feature[0]
            feature_field2 = multi_feald_feature[1]
            author_id_texts_dict1 = self.load_target_field_for_id([feature_field1])
            key_tuple1 = self.get_key_tuple(feature_field1)
            key_tuple2 = self.get_key_tuple(feature_field2)
            author_id_texts_dict2 = self.load_target_field_for_id([feature_field2])
            for post_id in author_id_texts_dict1[key_tuple1].keys():
                if counter % 100 == 0:
                    print("\r multi field, finished processing author " + str(counter), end="")
                counter += 1
                authors_features = []
                author_guid = self._db.get_author_id_by_field_id(multi_feald_feature[0]['id_field'], post_id)
                if author_guid not in self._authors.keys():
                    continue
                author = self._authors[author_guid]
                for feature in self._features:
                    for function in self._multi_field_feature_functions:
                        feature_name = function + "_" + feature
                        first_field_text = author_id_texts_dict1[key_tuple1][post_id]
                        second_field_text = author_id_texts_dict2[key_tuple2][post_id]
                        values_tupple = getattr(self, feature)(first_text_set=first_field_text,
                                                               second_text_set=second_field_text)
                        result = getattr(self, function)(values_tupple)
                        author_feature = self.run_and_create_author_feature_with_given_value(author_guid, result,
                                                                                             feature_name)
                        authors_features.append(author_feature)
            logging.info("finished processing author " + str(counter))
            self.insert_author_features_to_db(authors_features)

    def setUp(self):
        pass

    def cleanUp(self):
        pass

    def _get_target_fields(self, kwargs):
        return kwargs["target_fields"]

    def num_of_chars_on_avg(self, **kwargs):
        target_fields = self._get_target_fields(kwargs)
        return self._calculate_avg_num_of_chars(target_fields)

    def _calculate_avg_num_of_chars(self, target_fields):
        if target_fields is None or len(target_fields) == 0:
            return -1
        counter = 0
        sum = 0
        for target_field in target_fields:
            if target_field is not None:
                stripped_post = target_field.replace(" ", "")
                sum = sum + len(stripped_post)
                counter = counter + 1
        if counter == 0:
            return 0
        return float(sum) / counter

    def num_of_verbse_on_avarage(self, **kwargs):
        target_fields = self._get_target_fields(kwargs)
        return self._avg_num_of_verbs(target_fields)

    def _avg_num_of_verbs(self, target_fields):
        if target_fields is None or len(target_fields) == 0:
            return -1
        res = self._num_of_tag_types_in_sentences(target_fields, 'VERB')
        return float(res) / len(target_fields)


    def num_of_adjectives_on_avg(self, **kwargs):
        target_fields = self._get_target_fields(kwargs)
        return self._avg_num_of_adjectives(target_fields)

    def _avg_num_of_adjectives(self, target_fields):
        if target_fields is None or len(target_fields) == 0:
            return -1
        res = self._num_of_tag_types_in_sentences(target_fields, 'ADJ')
        return float(res) / len(target_fields)

    def num_of_nouns_on_avg(self, **kwargs):
        target_fields = self._get_target_fields(kwargs)
        return self._avg_num_of_nouns(target_fields)

    def _avg_num_of_nouns(self, target_fields):
        if target_fields is None or len(target_fields) == 0:
            return -1
        res = self._num_of_tag_types_in_sentences(target_fields, 'NOUN')
        return float(res) / len(target_fields)

    def _num_of_tag_types_in_sentences(self, sentences, tag_type):
        tags = self._get_pos_tags_for_sentences(sentences)
        return tags.count(tag_type)

    def _get_pos_tags_for_sentences(self, sentences):
        proccessed_tags = deque()
        for sentence in sentences:
            proccessed_tags.extend(nltk.pos_tag(word_tokenize(sentence), tagset='universal'))
        return [tag for word, tag in proccessed_tags]

    def num_of_quotations_on_avg(self, **kwargs):
        target_fields = self._get_target_fields(kwargs)
        return self._avg_num_of_quotations(target_fields)

    def _avg_num_of_quotations(self, target_fields):
        if target_fields is None or len(target_fields) == 0:
            return -1
        res = self._contains_quotations(target_fields)
        return float(res) / len(target_fields)

    def _contains_quotations(self, contents):
        if contents is None:
            return 0
        return ' '.join(contents).count("\"") / 2

    def _load_stop_words(self):
        self._stopwords_set = stopwords.words(u'english')

    def num_of_uppercase_words_in_post_on_avg(self, **kwargs):
        target_fields = self._get_target_fields(kwargs)
        return self._avg_num_of_uppercase_words(target_fields)

    def _avg_num_of_uppercase_words(self, target_fields):
        if target_fields is None or len(target_fields) == 0:
            return -1
        res = self._count_uppercase_words_in_sentences(target_fields)
        return float(res) / len(target_fields)

    def number_of_precent_of_uppercased_posts(self, **kwargs):
        target_fields = self._get_target_fields(kwargs)
        if target_fields is None or len(target_fields) == 0:
            return -1
        counter = 0
        for target_field in target_fields:
            counter = counter + self._are_all_sentence_words_uppercase(target_field)
        return float(counter) / len(target_fields)

    def _are_all_sentence_words_uppercase(self, sentence):
        if sentence is None or sentence == u'':
            return 0
        num_of_uppercase_words = self._count_uppercase_words_in_sentences([sentence])
        total_num_of_words = len(sentence.split())
        if num_of_uppercase_words == total_num_of_words:
            return 1
        else:
            return 0

    def _count_uppercase_words_in_sentences(self, sentences):
        return len(filter(str.isupper, str(' '.join(sentences)).split()))

    def num_of_formal_words_on_avg(self, **kwargs):
        target_fields = self._get_target_fields(kwargs)
        return self._avg_num_of_formal_words(target_fields)

    def _avg_num_of_formal_words(self, target_fields):
        if target_fields is None or len(target_fields) == 0:
            return -1
        sum = self._count_formal_words_in_sentence(' '.join(target_fields))
        return float(sum) / len(target_fields)

    def num_of_informal_words_on_avg(self, **kwargs):
        target_fields = self._get_target_fields(kwargs)
        return self._avg_num_of_informal_words(target_fields)

    def _avg_num_of_informal_words(self, target_fields):
        if target_fields is None or len(target_fields) == 0:
            return -1
        sum = self._count_informal_words_in_sentence(' '.join(target_fields))

        return float(sum) / len(target_fields)

    def precent_of_formal_words_on_avg(self, **kwargs):
        target_fields = self._get_target_fields(kwargs)
        if target_fields is None or len(target_fields) == 0:
            return -1
        sum = np.sum(map(self._precent_of_formal_words_in_sentence, target_fields))
        return float(sum) / len(target_fields)

    def _precent_of_formal_words_in_sentence(self, sentence):
        if sentence is None or sentence == u'':
            return 0
        num_of_formal_words = self._count_formal_words_in_sentence(sentence)
        return float(num_of_formal_words) / len(sentence.split())

    def _precent_of_informal_words_in_sentence(self, sentence):
        return 1 - self._precent_of_formal_words_in_sentence(sentence)

    def _count_formal_words_in_sentence(self, sentence):
        return self._count_words_base(sentence, set.intersection)

    def _count_informal_words_in_sentence(self, sentence):
        return self._count_words_base(sentence, set.difference)

    def _count_words_base(self, sentence, set_operation):
        if sentence is None or sentence == u'':
            return 0
        clean_words = self._clean_words(sentence.split())
        lower_words = map(str.lower, clean_words)
        words_counter = Counter(lower_words)
        counter = sum(map(words_counter.get, set_operation(set(lower_words), set(self._words))))
        return counter

    def _clean_words(self, words_list):
        clean_words = []
        for word in words_list:
            clean_words.append(str(commons.clean_word(word)))
        return clean_words

    def num_of_question_marks_on_avg(self, **kwargs):
        target_fields = self._get_target_fields(kwargs)
        if target_fields is None or len(target_fields) == 0:
            return -1
        selected_sign = '?'
        sum = 0
        for target_field in target_fields:
            sum = sum + self._calculate_num_of_given_signs_by_content(selected_sign, target_field)
        return float(sum) / len(target_fields)

    def _calculate_num_of_given_signs_by_content(self, selected_sign, content):
        if content is None or content == u'':
            return 0
        num_of_signs = content.count(unicode(selected_sign))
        return num_of_signs

    def num_of_colons_on_avg(self, **kwargs):
        target_fields = self._get_target_fields(kwargs)
        if target_fields is None or len(target_fields) == 0:
            return -1
        selected_sign = ':'
        sum = 0
        for target_field in target_fields:
            sum = sum + self._calculate_num_of_given_signs_by_content(selected_sign, target_field)
        return float(sum) / len(target_fields)

    def num_of_comas_on_avg(self, **kwargs):
        target_fields = self._get_target_fields(kwargs)
        if target_fields is None or len(target_fields) == 0:
            return -1
        selected_sign = ','
        sum = 0
        for target_field in target_fields:
            sum = sum + self._calculate_num_of_given_signs_by_content(selected_sign, target_field)
        return float(sum) / len(target_fields)

    def num_of_retweets_on_avg(self, **kwargs):
        target_fields = self._get_target_fields(kwargs)
        if target_fields is None or len(target_fields) == 0:
            return -1
        selected_sign = 'RT'
        sum = 0
        for target_field in target_fields:
            sum = sum + self._calculate_num_of_given_signs_by_content(selected_sign, target_field)
        return float(sum) / len(target_fields)

    def num_of_ellipsis_on_avg(self, **kwargs):
        target_fields = self._get_target_fields(kwargs)
        if target_fields is None or len(target_fields) == 0:
            return -1
        selected_sign = '...'
        sum = 0
        for target_field in target_fields:
            sum = sum + self._calculate_num_of_given_signs_by_content(selected_sign, target_field)
        return float(sum) / len(target_fields)

    def _count_stop_words_in_sentence(self, sentence):
        if sentence is None or sentence == u'':
            return 0
        counter = 0
        clean_words = self._clean_words(sentence.split())
        for word in clean_words:
            if word in self._stopwords_set:
                counter = counter + 1
        return counter

    def _count_precent_of_stop_words_in_sentence(self, sentence):
        if sentence is None or sentence == u'':
            return 0
        num_of_stop_words = self._count_stop_words_in_sentence(sentence)
        num_of_words = len(sentence.split())
        if num_of_stop_words == 0:
            return 0
        return float(num_of_stop_words) / num_of_words

    def num_of_stop_words_on_avg(self, **kwargs):
        target_fields = self._get_target_fields(kwargs)
        return self._avg_num_of_stopwords(target_fields)

    def _avg_num_of_stopwords(self, target_fields):
        if target_fields is None or len(target_fields) == 0:
            return -1
        counter = 0
        sum = 0
        for target_field in target_fields:
            sum = sum + self._count_stop_words_in_sentence(target_field)
            counter = counter + 1
        if counter == 0:
            return 0
        return float(sum) / counter

    def precent_of_stop_words_on_avg(self, **kwargs):
        target_fields = self._get_target_fields(kwargs)
        if target_fields is None or len(target_fields) == 0:
            return -1
        counter = 0
        sum = 0
        for target_field in target_fields:
            sum = sum + self._count_precent_of_stop_words_in_sentence(target_field)
            counter = counter + 1
        if counter == 0:
            return 0
        return float(sum) / counter

    def diffarence(self, tupple):
        return tupple[0] - tupple[1]

    def ratio(self, tupple):
        if tupple[1] == 0:
            return 0.0
        return float(tupple[0]) / tupple[1]

    def num_of_characters(self, first_text_set, second_text_set):
        avg_num1 = self._calculate_avg_num_of_chars(first_text_set)
        avg_num2 = self._calculate_avg_num_of_chars(second_text_set)
        return (avg_num1, avg_num2)

    def num_of_verbse(self, first_text_set, second_text_set):
        avg_num1 = self._avg_num_of_verbs(first_text_set)
        avg_num2 = self._avg_num_of_verbs(second_text_set)
        return (avg_num1, avg_num2)

    def num_of_nouns(self, first_text_set, second_text_set):
        avg_num1 = self._avg_num_of_nouns(first_text_set)
        avg_num2 = self._avg_num_of_nouns(second_text_set)
        return (avg_num1, avg_num2)

    def num_of_adj(self, first_text_set, second_text_set):
        avg_num1 = self._avg_num_of_adjectives(first_text_set)
        avg_num2 = self._avg_num_of_adjectives(second_text_set)
        return (avg_num1, avg_num2)

    def num_of_quotations(self, first_text_set, second_text_set):
        avg_num1 = self._avg_num_of_quotations(first_text_set)
        avg_num2 = self._avg_num_of_quotations(second_text_set)
        return (avg_num1, avg_num2)

    def num_of_uppercase_words(self, first_text_set, second_text_set):
        avg_num1 = self._avg_num_of_uppercase_words(first_text_set)
        avg_num2 = self._avg_num_of_uppercase_words(second_text_set)
        return (avg_num1, avg_num2)

    def num_of_foraml_words(self, first_text_set, second_text_set):
        avg_num1 = self._avg_num_of_formal_words(first_text_set)
        avg_num2 = self._avg_num_of_formal_words(second_text_set)
        return (avg_num1, avg_num2)

    def num_of_informal_words(self, first_text_set, second_text_set):
        avg_num1 = self._avg_num_of_informal_words(first_text_set)
        avg_num2 = self._avg_num_of_informal_words(second_text_set)
        return (avg_num1, avg_num2)

    def num_of_stopwords(self, first_text_set, second_text_set):
        avg_num1 = self._avg_num_of_stopwords(first_text_set)
        avg_num2 = self._avg_num_of_stopwords(second_text_set)
        return (avg_num1, avg_num2)
