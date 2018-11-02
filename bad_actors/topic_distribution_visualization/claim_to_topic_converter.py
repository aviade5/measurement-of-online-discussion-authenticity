from __future__ import print_function
import json
from DB.schema_definition import AuthorConnection
from commons.commons import clean_tweet
from dataset_builder.feature_extractor.feature_argument_parser import ArgumentParser


class ClaimToTopicConverter(ArgumentParser):
    def __init__(self, db):
        super(ClaimToTopicConverter, self).__init__(db)
        self._json_output_path = self._config_parser.eval(self.__class__.__name__, "json_output_path")
        self.term_dictionary = {}
        self._last_term_index = 0
        self._num_topics = 0
        self._claim_id_topic_dict = {}
        self._post_id_to_topic = {}

    def setUp(self):
        self._last_term_index = len(self._db.get_terms())
        self._num_topics = self._db.get_number_of_topics()

    def execute(self, window_start=None):
        print("load data from DB")
        print("Generate topic table")
        self.generate_topics_tables()
        print("Generate post_topic_mapping table")
        self.generate_post_topic_mapping()
        print("Generate author_topic_mapping table")
        self.generate_author_topic_mapping()
        print("Add source_id topic_id to author_connections")
        self._add_claim_id_topic_id_to_author_connections()
        print("Write source_id_topic_id_dict to json")
        self._write_source_id_topic_id_to_json()

    def get_claim_id_topic_dictionary(self):
        return self._claim_id_topic_dict

    def generate_topics_tables(self):
        claims = self._db.get_claims()
        terms = []
        topic_items = []
        for i, claim in enumerate(claims):
            msg = "\r generate topic {0}/{1}".format(str(i + 1), str(len(claims)))
            print(msg, end="")
            self._generate_terms_and_topic_items(claim, topic_items, terms)
        print()
        self._db.add_terms(terms)
        self._db.add_topic_items(topic_items)

    def generate_post_topic_mapping(self):
        claim_id_posts_dict = self._db.get_claim_id_posts_dict()
        post_topic_mappings = []
        self._generate_post_topic_mappings_for_claims(claim_id_posts_dict, post_topic_mappings)
        self._db.addPostTopicMappings(post_topic_mappings)
        pass

    def generate_author_topic_mapping(self):
        post_to_topic_id = self._get_post_id_max_topic_id()
        # self._db.delete_author_topic_mapping()
        self._create_author_topic_mapping_table(post_to_topic_id)

    def _get_post_id_max_topic_id(self):
        return {source_id: {topic_id: 1.0} for source_id, topic_id in self._post_id_to_topic.iteritems()}

    def _write_source_id_topic_id_to_json(self):
        table_name = u'claims'
        id_name = u'claim_id'
        json_str = json.dumps(self._claim_id_topic_dict, ensure_ascii=False)
        json_file = open('{2}{0}_{1}_topic_id_json.json'.format(table_name, id_name, self._json_output_path), 'w')
        json_file.write(json_str)

    def _add_claim_id_topic_id_to_author_connections(self):
        table_name = u'claims'
        id_name = u'claim_id'
        connection_type = u'{0}.{1} to topic id'.format(table_name, id_name)
        author_connections = []
        for source_id, topic_id in self._claim_id_topic_dict.iteritems():
            ac = AuthorConnection()
            ac.source_author_guid = source_id
            ac.destination_author_guid = topic_id
            ac.connection_type = connection_type
            author_connections.append(ac)
        self._db.add_author_connections(author_connections)

    def _update_term_to_term_id_dict(self, topic_terms):
        for term in topic_terms:
            if term not in self.term_dictionary:
                self.term_dictionary[term] = self._last_term_index
                self._last_term_index += 1

    def _generate_terms_and_topic_items(self, claim, topic_items, terms):
        claim_id = claim.claim_id
        topic_content = clean_tweet(claim.description)
        topic_terms = topic_content.split(' ')
        self._update_term_to_term_id_dict(topic_terms)
        term_count = float(len(topic_terms))
        self._num_topics += 1
        topic_id = self._num_topics
        for term in topic_terms:
            term_id = self.term_dictionary[term]
            term_probability = topic_content.count(term) / term_count
            terms.append(self._db.create_term(term_id, term))
            topic_items.append(self._db.create_topic_item(topic_id, term_id, term_probability))
        self._claim_id_topic_dict[claim_id] = self._num_topics

    def _generate_post_topic_mappings_for_claims(self, claim_id_posts_dict, post_topic_mappings):
        i = 0
        for claim_id, posts in claim_id_posts_dict.iteritems():
            if i % 100 == 0:
                msg = "\r generate claim post mappings {0}/{1}".format(str(i), len(claim_id_posts_dict))
                print(msg, end="")
            i += 1
            self._generate_post_topic_mappings_for_claim(claim_id, posts, post_topic_mappings)
        msg = "\r generate post mapping {0}".format(str(i))
        print(msg, end="")
        print()

    def _generate_post_topic_mappings_for_claim(self, claim_id, posts, post_topic_mappings):
        for post in posts:
            max_topic_probability = (self._claim_id_topic_dict[claim_id], 1)
            ptm = self._db.create_post_topic_mapping_obj(max_topic_probability, post.post_id)
            post_topic_mappings.append(ptm)
            self._post_id_to_topic[post.post_id] = self._claim_id_topic_dict[claim_id]

    def _get_arg_value(self, target_fields, arg_name):
        if self._contain_destination_arg(target_fields):
            arg_value = target_fields['destination'][arg_name]
        else:
            arg_value = target_fields['source'][arg_name]
        return arg_value

    def _contain_destination_arg(self, target_fields):
        return "destination" in target_fields and target_fields["destination"] != {}

    def _create_author_topic_mapping_table(self, post_to_topic_id):
        authors = self._db.get_authors_by_domain(self._domain)
        self._db.create_author_topic_mapping_table(self._num_topics)
        author_guid_posts_dict = self._db.get_author_guid_post_dict()
        author_mappings = self._create_author_topic_mappings(author_guid_posts_dict, authors, post_to_topic_id)
        self._db.insert_into_author_toppic_mappings(author_mappings)

    def _create_author_topic_mappings(self, author_guid_posts_dict, authors, post_to_topic_id):
        author_mappings = []
        for i, author in enumerate(authors):
            if i % 100 == 0:
                msg = "\r generate author mapping {0}/{1}".format(str(i + 1), str(len(authors)))
                print(msg, end="")
            posts_by_domain = author_guid_posts_dict[author.author_guid]
            posts_by_domain = [post for post in posts_by_domain if post.domain == self._domain]
            post_count = len(posts_by_domain)
            topics_probabilities = [0] * self._db.get_number_of_topics()
            for post in posts_by_domain:
                for key in post_to_topic_id[post.guid]:
                    topics_probabilities[key - 1] += 1.0 / post_count
            author_mappings.append((str(author.author_guid), topics_probabilities))
        msg = "\r generate author mapping {0}/{1}".format(str(len(authors)), str(len(authors)))
        print(msg, end="")
        print()
        return author_mappings
