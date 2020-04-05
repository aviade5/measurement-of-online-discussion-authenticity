#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from  itertools import combinations
from scipy.stats import spearmanr
from scipy.stats import kendalltau
import numpy as np
import pandas as pd
import networkx as nx
import csv
from collections import defaultdict
from datetime import datetime
from Twitter_API.twitter_api_requester import TwitterApiRequester
from commons.method_executor import Method_Executor



class InfluenceManager(Method_Executor):
    def __init__(self, db):
        Method_Executor.__init__(self, db)
        self._twitter_api = TwitterApiRequester()
        self._features = self._config_parser.eval(self.__class__.__name__, "features")
        self._group_guid = self._config_parser.eval(self.__class__.__name__, "group_guid")


    def _calculate_RT(self,authors_and_posts):
        wall_rt = [(i[0], i[4]) for i in authors_and_posts]
        df_authors_and_posts = pd.DataFrame(wall_rt, columns=['author_guid', 'wall_RT'])
        return df_authors_and_posts.groupby('author_guid').agg({"wall_RT": [np.sum]})

    def _calculate_partial_posts_count(self,authors_and_posts):
        wall_partial_posts_count = [(i[0], i[1]) for i in authors_and_posts]
        df_authors_and_posts = pd.DataFrame(wall_partial_posts_count, columns=['author_guid', 'wall_partial_posts_count'])
        return df_authors_and_posts.groupby('author_guid').agg({("wall_partial_posts_count","count")})


    def _daily_posts_calculation(self, dates_dic, hours_posts):

        for date in dates_dic:
            hours_posts[date]["morning"] = 0
            hours_posts[date]["night"] = 0
            hours_posts[date]["noon"] = 0
            for post in dates_dic[date]:

                hour_of_post = int(post[1].split(" ")[1].split(":")[0])
                if (6 <= hour_of_post < 14):
                    hours_posts[date]["morning"] = hours_posts[date]["morning"] + 1
                elif (14 <= hour_of_post < 22):
                    hours_posts[date]["noon"] = hours_posts[date]["noon"] + 1
                else:
                    hours_posts[date]["night"] = hours_posts[date]["night"] + 1

        return hours_posts

    def _calculate_team_followers(self,authors_guid):

        team_followers_count = {}
        for author in authors_guid:
            team_followers_count[author] = len(self._db.get_author_connections_by_author_guid(author))

        return pd.DataFrame.from_dict(team_followers_count, orient='index',
                                                         columns=['followers_team_count'])


    def _calculate_wall_avg_post(self, authors_guid):

        full_df_hours_posts = pd.DataFrame()

        for author in authors_guid:

            df_hours_posts = pd.DataFrame()
            author_post = self._db.get_author_posts_by_guid(author)

            if (len(author_post) == 0):
                continue

            hours_posts = defaultdict(lambda: defaultdict(int))
            dates_dic = defaultdict(list)
            author_post.sort(key=lambda x: x[1], reverse=False)

            for p in author_post:
                dates_dic[p[1].split(" ")[0]].append(p)
            hours_posts = self._daily_posts_calculation(dates_dic, hours_posts)

            df = pd.concat({k: pd.Series(v) for k, v in hours_posts.items()}).reset_index()
            df_hours_posts["avg_post_count@morning (6-14)"] = df.loc[df['level_1'] == 'morning'].mean()
            df_hours_posts["avg_post_count@night (22-6)"] = df.loc[df['level_1'] == 'night'].mean()
            df_hours_posts["avg_post_count@noon (14-22)"] = df.loc[df['level_1'] == 'noon'].mean()
            df_hours_posts['author_guid'] = author

            try:
                full_df_hours_posts = pd.merge(df_hours_posts, full_df_hours_posts, how='outer')
            except Exception as e:
                full_df_hours_posts = df_hours_posts if full_df_hours_posts.empty else full_df_hours_posts

        full_df_hours_posts.set_index('author_guid',inplace = True,drop=True)
        return full_df_hours_posts

    def _get_wall_posts(self, authors_guid):
            # wall_posts_guid_ = []
            # for author in authors_guid:
            #     author_post = self._db.get_author_posts_by_guid(author)
            #     wall_posts_guid_.append(author_post)

            #d = [item[0] for sublist in wall_posts_guid_ for item in sublist]
            wall_posts_guid_ = self._db.get_authors_posts_by_guid(authors_guid)
            return [item[0] for item in wall_posts_guid_]

        #return [item[0] for sublist in wall_posts_guid_ for item in sublist]



    def _calculate_team_mentions(self,authors_guid,team_comments,usernames):


        team_mentions_count = {}
        team_comments_keys = self._db.get_content(team_comments)

        for author in authors_guid:
            username = '@'+ usernames[author]
            team_count = 0
            for k in team_comments_keys:

                if username in team_comments_keys[k]:
                    team_count = team_count + 1
            team_mentions_count[author] = team_count


        df = pd.DataFrame.from_dict(team_mentions_count, orient='index', columns=['team_sum_of_mentions'])
        return df


    def _calculate_wall_mentions(self,authors_guid,wall_posts_guid,usernames):

        wall_mentions_count={}

        wall_posts_guid_keys = self._db.get_content(wall_posts_guid)

        for author in authors_guid:
            username = '@'+ usernames[author]
            wall_count=0
            for k in wall_posts_guid_keys:
                if username in wall_posts_guid_keys[k]:
                    wall_count=wall_count+1


            wall_mentions_count[author] = wall_count

        return  pd.DataFrame.from_dict(wall_mentions_count, orient='index', columns=['wall_sum_of_mentions'])

    def _calculate_team_sum_of_comments(self,full_team_comments):
        team_sum_of_comments = [(i[0],i[1])  for i in full_team_comments]
        df_team_comment = pd.DataFrame(team_sum_of_comments,columns=['author_guid','team_sum_of_comments'])
        return df_team_comment.groupby('author_guid').agg({'team_sum_of_comments': 'count'})







    def _calculate_team_sum_of_shares(self,full_team_comments):
        team_sum_of_shares = [(i[0],i[2])  for i in full_team_comments]
        df_team_comment = pd.DataFrame(team_sum_of_shares, columns=['author_guid','team_sum_of_shares'])
        return df_team_comment.groupby('author_guid').agg({"team_sum_of_shares": [np.sum]})

    def _calculate_team_sum_of_favorite(self,full_team_comments):

        team_sum_of_favorite = [(i[0],i[3])  for i in full_team_comments]
        df_team_comment = pd.DataFrame(team_sum_of_favorite, columns=[ 'author_guid','team_sum_of_favorite'])
        return df_team_comment.groupby('author_guid').agg({"team_sum_of_favorite": [np.sum]})

    def  _calculate_wall_avg_of_replies(self,wall_comments_count):


        d= pd.DataFrame(wall_comments_count, columns=['author_guid','post_id','wall_avg_of_replies'])
        return d.groupby('author_guid').agg({"wall_avg_of_replies": [np.mean]})


    def _merge_all_dfs(self, dfs):

        merged_df = dfs[0]

        del dfs[0]
        for df in dfs:
            merged_df = pd.merge(merged_df, df, left_index=True, right_index=True, how='outer')
        return merged_df


    def _calculate_wall_sum_of_shares(self,authors_and_posts):

        wall_sum_of_shares = [(i[0],i[2]) for i in authors_and_posts]
        df_authors_and_posts = pd.DataFrame(wall_sum_of_shares, columns=['author_guid','wall_sum_of_shares'])
        return df_authors_and_posts.groupby('author_guid').agg({"wall_sum_of_shares": [np.sum]})

    def _calculate_wall_avg_of_shares(self,authors_and_posts):

        wall_sum_of_shares = [(i[0],i[2]) for i in authors_and_posts]
        df_authors_and_posts = pd.DataFrame(wall_sum_of_shares, columns=['author_guid','wall_avg_of_shares'])
        d = df_authors_and_posts.groupby('author_guid').agg({"wall_avg_of_shares": [np.mean]})
        return d

    def _calculate_wall_favorite(self,authors_and_posts):

        wall_sum_of_shares = [(i[0],i[3]) for i in authors_and_posts]
        df_authors_and_posts = pd.DataFrame(wall_sum_of_shares, columns=['author_guid','wall_avg_of_favorite'])
        return df_authors_and_posts.groupby('author_guid').agg({"wall_avg_of_favorite": [np.mean]})



    def _create_features(self, df):

      #  print(df.to_string())
      #  df['wall_statuses_count_rank'] = df['wall_statuses_count'].rank(method='dense', ascending=False)

        if 'wall_avg_daily_statuses' in self._features:
            df['wall_avg_daily_statuses'] = df['wall_statuses_count'] / df['created_at']
          #  df['wall_avg_daily_statuses_rank'] = df['wall_avg_daily_statuses'].rank(method='dense', ascending=False)

        # if 'wall_avg_daily_favourites' in self._features:
        #     df['wall_avg_daily_favourites'] = df['wall_full_favourites_count'] / df['created_at']
        #     df['wall_full_favourites_count_rank'] = df['wall_full_favourites_count'].rank(method='dense', ascending=False)
        #     df['wall_avg_daily_favourites_rank'] = df['wall_avg_daily_favourites'].rank(method='dense', ascending=False)

       # if 'wall_full_favourites_count' in self._features:
           # df['wall_avg_favourites (wall_avg_favourites)'] = df['wall_full_favourites_count'] / df['wall_statuses_count']
            #df['wall_avg_favourites_rank'] = df['wall_avg_favourites (wall_avg_favourites)'].rank(method='dense', ascending=False)
           # df['wall_favourites_count_rank'] = df['wall_full_favourites_count'].rank(method='dense', ascending=False)

        if 'followers_team_count' in self._features:
            df['Followers_ratio'] = df['followers_team_count'] / df['followers_count']
            df['Followers_ratio_rank'] = df['Followers_ratio'].rank(method='dense', ascending=False)
            df['followers_count_rank'] = df['followers_count'].rank(method='dense', ascending=False)

        if 'wall_sum_of_mentions' in self._features:

            df['wall_avg_mentions_ratio (mentions/statuses)'] = df['wall_sum_of_mentions'] / df['wall_statuses_count']
            df['wall_avg_mentions_ratio_rank'] = df['wall_avg_mentions_ratio (mentions/statuses)'].rank(method='dense',ascending=False)

            df['wall_sum_of_mentions_rank'] = df['wall_sum_of_mentions'].rank(method='dense',ascending=False)

        if 'wall_avg_of_replies' in self._features:
            df['wall_avg_of_replies_rank'] = df[('wall_avg_of_replies','mean')].rank(method='dense', ascending=False)

        if 'team_sum_of_shares' in self._features:
          df['team_avg_of_shares'] = df[('team_sum_of_shares', 'sum')] / df[('team_sum_of_comments')]
          df['team_sum_of_shares_rank'] = df[('team_sum_of_shares', 'sum')].rank(method='dense', ascending=False)
          df['team_avg_of_shares_rank'] = df['team_avg_of_shares'].rank(method='dense', ascending=False)

        if 'team_sum_of_favorite' in self._features:
            df['team_avg_favourites'] = df[('team_sum_of_favorite', 'sum')] / df[('team_sum_of_comments')]
            df['team_avg_favourites_rank'] = df['team_avg_favourites'].rank(method='dense', ascending=False)

        # if 'wall_avg_post_count_for_morning_noon_night' in self._features:
        #     df['wall_avg_post_count@morning (6-14)_rank'] = df['avg_post_count@morning (6-14)'].rank(method='dense', ascending=False)
        #     df['wall_avg_post_count@night (22-6)_rank'] = df['avg_post_count@night (22-6)'].rank(method='dense',ascending=False)
        #     df['wall_avg_post_count@noon (14-22)_rank'] = df['avg_post_count@noon (14-22)'].rank(method='dense',ascending=False)

        if 'team_sum_of_mentions' in self._features:
            df['team_sum_of_mentions_rank'] = df['team_sum_of_mentions'].rank(method='dense', ascending=False)

        #if 'friends_count' in self._features:
            #df['friends_count_rank'] = df['friends_count'].rank(method='dense', ascending=False)

        if 'wall_sum_of_shares' in self._features:
            df['wall_sum_of_shares_rank'] = df[('wall_sum_of_shares', 'sum')].rank(method='dense', ascending=False)
            df['wall_avg_of_shares_rank'] = df[('wall_avg_of_shares', 'mean')].rank(method='dense', ascending=False)

        if 'wall_avg_of_favorite'  in self._features:
            df['wall_avg_of_favorite_rank'] = df[('wall_avg_of_favorite', 'mean')].rank(method='dense', ascending=False)

        if 'eigenvector' in self._features:
            df['eigenvector_rank'] = df['eigenvector'].rank(method='dense', ascending=False)
        if 'closeness' in self._features:
            df['closeness_rank'] = df['closeness'].rank(method='dense', ascending=False)
        if 'pagerank' in self._features:
            df['pagerank_rank'] = df['pagerank'].rank(method='dense', ascending=False)

        return df


    def _spearman_procces(self, df):

        columns = list(df.columns)
        #columns= ["pagerank_rank","closeness_rank","eigenvector_rank"]
        ranks = []
        for c in columns:
            if "rank" in c:
                ranks.append(c)

        spearman = []
        combos = list(combinations(ranks, 2))
        for combo in combos:
            print(combo)
            value = self.spearmanr_calculation(combo,df[combo[0]].values.tolist(),
                                               df[combo[1]].values.tolist())
            if (value > 0):
                spearman.append(combo)
                #   self.kendalltau_calculation(df[combo[0]].values.tolist(), df[combo[1]].values.tolist())

        col = {}
        for pair in spearman:
            col1, col2 = pair
            if col1 not in col.keys():
                col[col1] = 0
            if col2 not in col.keys():
                col[col2] = 0

        return df, col

    def _create_df_from_scratch(self, authors_guid):


        if (self._group_guid in authors_guid):  # guid of the group
            authors_guid.remove(self._group_guid)

        team_posts = [a.post_id for a in self._db.get_posts_by_author_guid(self._group_guid)]
        full_team_comments = self._db.get_comments(team_posts)
        team_comments = [item[1] for item in full_team_comments]

        authors = self._db.authors_data(authors_guid)
        followers_data = self._db.get_author_connections_by_type("follower")
        usernames = self._db.get_screen_names()
        wall_posts_guid = self._get_wall_posts(authors_guid)

        dfs = []
        followers=[]
        for follower in followers_data:
            followers.append(follower)
        team_graph = self._create_network(followers, authors.keys())

        if 'eigenvector' in self._features:
            dfs.append(self._calculate_eigenvector(team_graph))
            print("Finished calculating eigenvector feature")
        if 'pagerank' in self._features:
            dfs.append(self._calculate_pagerank(team_graph))
            print("Finished calculating pagerank feature")

        if 'closeness' in self._features:
            dfs.append(self._calculate_closeness(team_graph))
            print("Finished calculating closeness feature")


        dfs.append(self._calculate_wall_statuses_count(authors))
        print("Finished calculating wall_statuses_count feature")
        if 'wall_full_favourites_count' in self._features:
            dfs.append(self._calculate_wall_full_favourites_count(authors))
            print("Finished calculating wall_full_favourites_count feature")

        if 'friends_count' in self._features:
            dfs.append(self._calculate_friends_count(authors))
            print("Finished calculating friends_count feature")


        dfs.append(self._calculate_followers_count(authors))
        print("Finished calculating followers_count feature")

        dfs.append(self._calculate_created_at(authors))
        print("Finished calculating created_at feature")




        if 'team_sum_of_mentions' in self._features:
            dfs.append(self._calculate_team_mentions(authors_guid, team_comments, usernames))
            print("Finished calculating team_sum_of_mentions feature")

        if 'wall_sum_of_mentions' in self._features:
            dfs.append(self._calculate_wall_mentions(authors_guid,wall_posts_guid, usernames))
            print("Finished calculating wall_sum_of_mentions feature")


        if 'Followers_ratio' in self._features:
            dfs.append(self._calculate_team_followers(authors_guid))
            print("Finished calculating Followers_ratio feature")

        if 'wall_avg_post_count_for_morning_noon_night' in self._features:
            dfs.append(self._calculate_wall_avg_post(authors_guid))
            print("Finished calculating wall_avg_post_count_for_morning_noon_night feature")





        wall_comments_count = self._db.get_authors_comments_counts(wall_posts_guid)
        authors_and_posts = self._db.get_posts(wall_posts_guid)
        if 'wall_avg_of_replies' in self._features:
            dfs.append(self._calculate_wall_avg_of_replies(wall_comments_count))
            print("Finished calculating wall_avg_of_replies feature")
            print(dfs[11].to_string())
        dfs.append(self._calculate_team_sum_of_comments(full_team_comments))
        dfs.append(self._calculate_partial_posts_count(authors_and_posts))
        if 'wall_sum_of_shares' in self._features:

            dfs.append(self._calculate_RT(authors_and_posts))
            dfs.append(self._calculate_wall_sum_of_shares(authors_and_posts))
            dfs.append(self._calculate_wall_avg_of_shares(authors_and_posts))

            print("Finished calculating wall_sum_of_shares feature")

        if 'wall_avg_of_favorite' in self._features:
            dfs.append(self._calculate_wall_favorite(authors_and_posts))
            print("Finished calculating wall_avg_of_favorite feature")


        if 'team_sum_of_shares' in self._features:
            dfs.append( self._calculate_team_sum_of_shares(full_team_comments))
            print("Finished calculating team_sum_of_shares feature")

        if 'team_sum_of_favorite' in self._features:
            dfs.append(self._calculate_team_sum_of_favorite(full_team_comments))
            print("Finished calculating team_sum_of_favorite feature")








        merged_df = self._merge_all_dfs(dfs)
        print(merged_df.to_string())
        return merged_df

    def influencer_calculation(self):

        authors_guid = self._db.get_all_authors_guid()
        #authors_guid = authors_guid[:3]

        df = self._create_df_from_scratch(authors_guid)
        df = self._create_features(df)

        df = df.replace(np.nan, 0)

        df.to_csv("dataframe.csv")

        columns = list(df.columns)
        ranks = []
        for c in columns:
            if "rank" in c:
                ranks.append(c)

        avg_df_rank = df[ranks].mean(axis=1)
        influence_sorted_df = avg_df_rank.sort_values(ascending=True)
        influence_sorted_df.to_csv("influence_rank.csv")



        df, col = self._spearman_procces(df)

        avg_df = df[col].mean(axis=1)
        sorted_df = avg_df.sort_values(ascending=False)

       # print(sorted_df.to_string())
        #sorted_df.to_csv("influencers.csv")

        rec = []
        values = [list(l) for l in zip(df.values)]
        authors_guid = df.index
        attribute_names = df.columns.tolist()
        for authors_guid_index,l in enumerate(values):
            for index,attribute_value in enumerate(l[0]):
                author_feature = self._db.create_author_feature(authors_guid[authors_guid_index], str(attribute_names[index]), attribute_value)
                rec.append(author_feature)
            self._db.add_author_features(rec)
            rec = []


        i = 9

    def kendalltau_calculation(self, combo,data1, data2):
      with open('Spearmans correlation.csv', mode='ab') as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        coef, p = kendalltau(data1, data2)
        writer.writerow(combo)
        print('Kendall correlation coefficient: %.3f' % coef)
        writer.writerow(['Spearmans correlation coefficient: %.3f' % coef])
        # interpret the significance
        alpha = 0.05
        if p > alpha:
            print('Samples are uncorrelated (fail to reject H0) p=%.3f' % p)
            writer.writerow(['Samples are uncorrelated (fail to reject H0) p=%.3f' % p])

        else:
            print('Samples are correlated (reject H0) p=%.3f' % p)
            writer.writerow(['Samples are correlated (reject H0) p=%.3f' % p])
        return coef

    def spearmanr_calculation(self, combo,data1, data2):
        with open('Spearmans correlation.csv', mode='ab') as file:
            writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)


            data1=list(map(int, data1))
            data2=list(map(int, data2))
            coef, p = spearmanr(data1, data2)
            writer.writerow(combo)
            print('Spearmans correlation coefficient: %.3f' % coef)
            writer.writerow(['Spearmans correlation coefficient: %.3f' % coef])
            # interpret the significance
            alpha = 0.05
            if p > alpha:
                print('Samples are uncorrelated (fail to reject H0) p=%.3f' % p)
                writer.writerow(['Samples are uncorrelated (fail to reject H0) p=%.3f' % p])
            else:
                print('Samples are correlated (reject H0) p=%.3f' % p)
                writer.writerow(['Samples are correlated (reject H0) p=%.3f' % p])
            return coef

    def _create_network(self, data, authors):
        G = nx.Graph()
        for item in data:
            num1 = item[0]
            num2 = item[1]
            if (G.has_node(num1) is False):
                G.add_node(num1)


            if (G.has_node(num2) is False):
                G.add_node(num2)

            if (G.has_edge(num1, num2) is False) and (num1 in authors) and (num2 in authors):
                G.add_edge(num1, num2)


        return G

    def _calculate_eigenvector(self,G):
        eigenvector = nx.eigenvector_centrality(G)
        df_eigenvector = pd.DataFrame.from_dict(eigenvector, orient='index',columns=['eigenvector'])
        return df_eigenvector

    def _calculate_closeness(self,G):
        closeness = nx.closeness_centrality(G)
        df_closeness = pd.DataFrame.from_dict(closeness, orient='index', columns=['closeness'])
        return df_closeness

    def _calculate_pagerank(self,G):
        pagerank = nx.pagerank(G)
        df_pagerank = pd.DataFrame.from_dict(pagerank, orient='index', columns=['pagerank'])
        return df_pagerank

    def _calculate_wall_statuses_count(self,authors):
        wall_statuses_count = {author: authors[author][0] for author in authors}
        return pd.DataFrame.from_dict(wall_statuses_count, orient='index',
                                      columns=['wall_statuses_count'])


    def _calculate_wall_full_favourites_count(self,authors):
        full_favourites_count = {author:authors[author][2] for author in authors}
        return pd.DataFrame.from_dict(full_favourites_count, orient='index', columns=['wall_full_favourites_count'])

    def _calculate_friends_count(self,authors):
        full_friends_count = {author: authors[author][3] for author in authors}
        return pd.DataFrame.from_dict(full_friends_count, orient='index',columns=['friends_count'])

    def _calculate_followers_count(self,authors):
        full_followers_count = {author: authors[author][4] for author in authors}
        return pd.DataFrame.from_dict(full_followers_count, orient='index',
                                      columns=['followers_count'])

    def _calculate_created_at(self,authors):
        full_created_at = {author: authors[author][1] for author in authors}
        df = pd.DataFrame.from_dict(full_created_at, orient='index',columns=['created_at'])
        df['created_at'] = (datetime.now() - df['created_at']).dt.days
        return df




