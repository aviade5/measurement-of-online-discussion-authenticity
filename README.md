# Online Social Network Abuser Detection & Online Discussion Authenticity Detection Framework
Code used in the following papers.

If you are using this code for any research publication, or for preparing a technical report, you must cite the following paper as the source of the code.

Aviad Elyashar, Jorge Bendahan, and Rami Puzis. "Is the News Deceptive? Fake News Detection using Topic Authenticity"

BibTex:

@inproceedings{elyashar2017news,
 author={Elyashar, Aviad and Bendahan, Jorge and Puzis, Rami},
 title     = {Is the News Deceptive? Fake News Detection using Topic Authenticity},  
 booktitle = {The Seventh International Conference on Social Media Technologies, Communication, and Informatics},
 series = {SOTICS 2017},
 year = {2017},
 location = {Athens, Greece},
 pages     = {16--21},
 numpages={6}
 }

### Requirements
1. Python modules
  * numpy
  * Scikit Learn
  * networkx
  * pandas 0.22.0
  * wordcloud
  
  
# Configuration instractions
In order to add a module to the pipline you need to uncomment the module from config.ini file
Most of the argumant in the config.ini are intuitive such as:
1. ['a','b', ..] is a list
2. 4 is a number
3. {'key': value} is a dict

# Now sume of the moduls contains special argumant called *targeted_fields*
This argument purpose is to obtain the input data to a modul using the join operation between DB tables.
This argument recieve list of dicts as followes:
In case you want to use data from only one DB table the dict arg as follows:

[{*'source'*: {*'table_name'*: 'posts', *'id'*: 'post_id', *'target_field'*: 'content', *"where_clauses"*: [{*"field_name"*: "domain", *"value"*: "Microblog"}]}},{..},..]

Where:
'table_name' -> is the table name
'id' -> is the id of the table (its not support multiple ids)
'target_field' -> the field that the module will recive as input for its functions
where_clauses' -> list containing constrains such as {"field_name": "domain", "value": "Microblog"} is all post with "Microblog" in their domain field.

In case you want to use data from join of 3 tables
Each dict must contain 3 keys: 'source', 'connection', 'destination'

{'source': {'table_name': 'claims', 'id': 'claim_id', 'target_field': 'verdict', "where_clauses": []},
 'connection': {'table_name': 'claim_tweet_connection', 'source_id': 'claim_id', 'target_id': 'post_id', }, 
 'destination': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content', "where_clauses": [{"field_name": "domain", "value": "Microblog"}]}}
 
 Where 'source' contains the id table, 'connection' is the table containing the connection and 'destanation' holds the data
 In the given example the result is all the Microblog posts of a claim =>
 {<claim_id1>: [<post1>, <post2>, ...], <claim_id2>: [<post1>, <post2>, ...], ...}
 
 *the connection table must contain the source id and the destination id in order to preform join*
