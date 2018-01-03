# Created by aviade
# Time: 31/03/2016 14:22

def enum(**named_values):
    return type('Enum', (), named_values)

Color = enum(RED='red', GREEN='green', BLUE='blue')
Connection_Type = enum(FOLLOWER=u'follower', RETWEETER=u'retweeter', FRIEND=u'friend', COCITATION=u'cocitation',
                       TOPIC_DISTR_SIMILARITY=u'topic_distr', PROFILE_PROP_SIMILARITY=u'profile_prop')
Author_Type = enum(BAD_ACTOR=u'bad_actor', GOOD_ACTOR=u'good_actor')
Author_Subtype = enum(PRIVATE=u'private',
                      COMPANY=u'company',
                      NEWS_FEED=u'news_feed',
                      SPAMMER=u'spammer',
                      BOT=u'bot',
                      CROWDTURFER=u'crowdturfer',
                      ACQUIRED=u'acquired')

CLASS_TYPE = enum(AUTHOR_TYPE=u'author_type', AUTHOR_SUB_TYPE=u'author_sub_type')

DB_Insertion_Type = enum(BAD_ACTORS_COLLECTOR=u'bad_actors_collector',
                         XML_IMPORTER=u'xml_importer',
                         MISSING_DATA_COMPLEMENTOR=u'missing_data_complementor',
                         BAD_ACTORS_MARKUP=u'bad_actors_markup',
                         MARK_MISSING_BAD_ACTOR_RETWEETERS=u'mark_missing_bad_actor_retweeters')

Author_Connection_Type = enum(FOLLOWER=u'follower', RETWEETER=u'retweeter', FRIEND=u'friend',
                              COMMON_POSTS=u'common_posts', COCITATION=u'cocitation', TOPIC=u'topic',
                              CITATION=u'citation')
Language = enum(ENGLISH=u'eng', GERMAN=u'ger')

Graph_Type = enum(CITATION=u'citation', COCITATION=u'cocitation', TOPIC=u'topic', FRIENDSHIP=u'friendship', FOLLOWER=u'follower')
Domains = enum(MICROBLOG=u'Microblog', BLOG=u'Blog')
Algorithms = enum(CLUSTERING=u'clustering', IN_DEGREE_CENTRALITY=u'in_degree_centrality', OUT_DEGREE_CENTRALITY=u'out_degree_centrality')

DistancesFromTargetedClass = enum(TRAIN_SIZE=u'train_size',
                                  DISTANCES_STATS=u'distances_statistics',
                                  MEAN=u'mean',
                                  MIN=u'min',
                                  CALCULATE_DISTANCES_FOR_UNLABELED=u'calculate_distances_for_unlabeled')

PerformanceMeasures = enum(AUC=u'AUC',
                           ACCURACY=u'Accuracy',
                           RECALL=u'Recall',
                           PRECISION=u'Precision',
                           CONFUSION_MATRIX=u'Confusion_Matrix',
                           SELECTED_FEATURES=u'Selected_Features',
                           CORRECTLY_CLASSIFIED=u"Correctly classified instances",
                           INCORRECTLY_CLASSIFIED=u"Incorrectly classified instances")


Classifiers = enum(RandomForest=u'RandomForest',
                   DecisionTree=u'DecisionTree',
                   AdaBoost=u'AdaBoost',
                   XGBoost=u'XGBoost')


Social_Networks = enum(TWITTER=u'Twitter',
                   TUMBLR=u'Tumblr')


AGGREGATION_FUNCTIONS = enum(AVERAGE=u'average',
                            MIN=u'min',
                            MAX=u'max')
