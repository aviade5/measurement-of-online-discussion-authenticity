from __future__ import print_function
import logging
import os
import sys

import pandas as pd
from preprocessing_tools.abstract_controller import AbstractController
from commons import date_to_str


class Method_Executor(AbstractController):
    def __init__(self, db):
        AbstractController.__init__(self, db)
        self._actions = self._config_parser.eval(self.__class__.__name__, "actions")

    def execute(self, window_start=None):
        for action_name in self._actions:
            try:
                getattr(self, action_name)()
            except AttributeError as e:
                print('\nError: {0}\n'.format(e.message), file=sys.stderr)
                logging.error('Error: {0}'.format(e.message))

    def _export_csv_files_for_statistics(self, output_folder_path, before_dict, after_dict, claims_dict):
        
        if not os.path.exists(output_folder_path):
            os.makedirs(output_folder_path)
        claim_to_tweet_count_rows = []
        claim_to_tweet_id_rows = []
        post_ids = set(before_dict.keys())
        for post_id in post_ids:
            claim = claims_dict[post_id]

            claim_content = claim.content
            original_claim_keywords = claim.tags
            keywords = original_claim_keywords.split(",")
            claim_keywords = "||".join(keywords)
            snopes_publication_date = date_to_str(claim.date)
            tweet_ids_before = before_dict[post_id]
            tweet_ids_after = after_dict[post_id]
            tweet_before_count = len(tweet_ids_before)
            tweet_after_count = len(tweet_ids_after)
            tweet_combined_count = len(tweet_ids_before & tweet_ids_after)
            total_tweet_count = tweet_before_count + tweet_after_count - tweet_combined_count
            claim_to_tweet_count_rows.append(
                (post_id, claim_content, claim_keywords, snopes_publication_date, tweet_before_count, tweet_after_count, tweet_combined_count, total_tweet_count))
            claim_to_tweet_id_rows.append((post_id, claim_content, claim_keywords, snopes_publication_date, ','.join(tweet_ids_before), ','.join(tweet_ids_after)))

        post_id_num_of_tweets_df = pd.DataFrame(claim_to_tweet_count_rows,
                                                columns=['claim_id', 'claim_content', 'claim_keywords', 'snopes_publication_date', 'num_before', 'num_after', 'num_combine', 'total'])
        post_id_num_of_tweets_df.to_csv(output_folder_path + "post_id_num_of_tweets.csv", index=False)

        post_id_tweet_ids_str_df = pd.DataFrame(claim_to_tweet_id_rows,
                                                columns=['claim_id', 'claim_content', 'claim_keywords', 'snopes_publication_date', 'tweet_ids_before', 'tweet_ids_after'])
        post_id_tweet_ids_str_df.to_csv(output_folder_path + "post_id_tweet_ids.csv", index=False)

    def _create_posts_dict(self, claims):
        claims_dict = {}
        for claim in claims:
            claim_id = claim.post_id
            claims_dict[claim_id] = claim
        return claims_dict
