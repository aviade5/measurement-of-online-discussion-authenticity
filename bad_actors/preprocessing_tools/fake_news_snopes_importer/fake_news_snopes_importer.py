# encoding: utf-8
# need to be added to the system

from __future__ import print_function
from DB.schema_definition import Post, date, Claim
from commons.commons import compute_post_guid, compute_author_guid_by_author_name
from preprocessing_tools.post_importer import PostImporter
import pandas as pd

__author__ = "Aviad Elyashar"


class FakeNewsSnopesImporter(PostImporter):
    def __init__(self, db):
        PostImporter.__init__(self, db)
        self._input_csv_file = self._config_parser.eval(self.__class__.__name__, "input_csv_file")

        # There is no author so the website would be the author. We should not include this author in the analysis.
        self._author_name = unicode("snopes")

    def execute(self, window_start=None):
        self._read_file_and_create_claims()

    def _read_file_and_create_claims(self):
        snopes_csv_df = pd.read_csv(self._input_csv_file)
        num_of_records = snopes_csv_df.shape[0]

        claims = []
        i = 0
        for index, row in snopes_csv_df.iterrows():
            i += 1
            print("\r Convert row to claim {0}:{1}".format(i, num_of_records), end="")
            claim = self._convert_row_to_claim(row)
            claims.append(claim)

        self._db.addPosts(claims)

    def _convert_row_to_post(self, row):
        post = Post()

        claim_id = unicode(row['claim_id'])
        title = unicode(row['title'], errors='replace')
        post.content = title

        description = unicode(row['description'], errors='replace')
        post.description = description

        url = unicode(row['url'])
        post.url = url

        publication_date = row['publication_date']
        post.date = date(publication_date)

        post_guid = compute_post_guid(self._social_network_url, claim_id, publication_date)
        post.guid = post_guid
        post.post_id = post_guid
        post.domain = self._domain
        post.author = self._author_name
        author_guid = compute_author_guid_by_author_name(self._author_name)
        post.author_guid = author_guid
        post.post_osn_guid = post_guid

        keywords = unicode(row['keywords'])
        post.tags = keywords

        post_type = unicode(row['post_type'])
        post.post_type = post_type

        return post

    def _convert_row_to_claim(self, row):
        claim = Claim()

        claim_id = unicode(row['claim_id'])
        title = unicode(row['title'], errors='replace')
        claim.title = title

        description = unicode(row['description'], errors='replace')
        claim.description = description

        url = unicode(row['url'])
        claim.url = url

        verdict_date = row['publication_date']
        claim.verdict_date = date(verdict_date)

        post_guid = compute_post_guid(self._social_network_url, claim_id, verdict_date)
        claim.claim_id = post_guid

        claim.domain = self._domain

        keywords = unicode(row['keywords'])
        claim.keywords = keywords

        verdict = unicode(row['post_type'])
        claim.verdict = verdict

        return claim

