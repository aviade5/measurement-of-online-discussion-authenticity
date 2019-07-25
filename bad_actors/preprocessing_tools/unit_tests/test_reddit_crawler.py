import types
import unittest
from unittest import TestCase

import praw
import signal

from DB.schema_definition import DB, Post, Author
# from commons.commons import str_to_date, convert_str_to_unicode_datetime
from preprocessing_tools.reddit_crawler.reddit_crawler import RedditCrawler


def timeout(seconds_before_timeout, msg):
    def decorate(f):
        def handler(signum, frame):
            raise Exception("TimeOut: {}".format(msg))

        def new_f(*args, **kwargs):
            old_signal_alarm = signal.signal(signal.SIGALRM, handler)
            signal.alarm(seconds_before_timeout)
            try:
                result = f(*args, **kwargs)
            finally:
                signal.signal(signal.SIGALRM, old_signal_alarm)
                signal.alarm(0)
            return result

        new_f.func_name = f.func_name
        return new_f

    return decorate


class TestRedditCrawler(TestCase):
    def setUp(self):
        self._db = DB()
        self._db.setUp()
        self._posts = []
        self._author = None
        self.reddit_crawler = RedditCrawler(self._db)
        self.reddit_crawler.reddit = RedditStub()

    def tearDown(self):
        self._db.session.close()
        pass

    @timeout(60, 'RedditCrawler')
    def test_export_reddit_data_to_csv(self):
        self.reddit_crawler.export_reddit_data_to_csv()
        post_count = sum([1 + len(post.comments.list()) for post in [submission for submission in self.reddit_crawler.reddit.submissions.itervalues()]])
        self.assertEquals(len(self._db.get_posts()), post_count)


class RedditStub:
    def __init__(self):
        self.submissions = {
            "https://redd.it/ayn7rq": Create_Spoilers_Game_of_Thrones_Best_Duels(),
            "https://redd.it/8a5pf1": Create_Roseanne_Barf_dressed_as_Hitler_making_burnt_Jew_cookies()}

    def submission(self, id=None, url=None):
        return self.submissions[url]


class SubmissionStub:
    def __init__(self):
        self.comments = None
        self.id = None
        self.fullname = None
        self.created = None
        self.created_utc = None
        self.author = None
        self.permalink = None
        self.title = None
        # self.body = None
        self.score = None
        self.upvote_ratio = None
        self.num_comments = None
        self.parent_id = None
        self.stickied = None
        self.is_submitter = None
        self.distinguished = None


class AuthorStub:
    def __init__(self):
        self.name = None
        self.created = None
        self.created_utc = None
        self.is_employee = None
        self.id = None
        self.fullname = None
        self.comment_karma = None
        self.is_gold = None
        self.is_mod = None
        self.link_karma = None


class StubMockup(object):
    def __new__(cls, **attributes):
        result = object.__new__(cls)
        result.__dict__ = attributes
        return result


def Create_Spoilers_Game_of_Thrones_Best_Duels():
    submission = SubmissionStub()
    submission.comments = StubMockup()
    submission.comments.replace_more = types.MethodType(lambda self, **kargs: None, submission)
    submission.comments.list = types.MethodType(lambda self: [
        StubMockup(id=u'ei1z7pq', ups=1, score=1, parent_id=u't3_ayn7rq', stickied=False, is_submitter=False,
                   distinguished=u'moderator', created=1552027475.0, created_utc=1552027475.0,
                   permalink=u'/r/gameofthrones/comments/ayn7rq/spoilers_game_of_thrones_best_duels/ei1z7pq/',
                   body=u'''**Spoiler Warning:** All officially-released show and book content allowed, including trailers and pre-released chapters. No leaked information or paparazzi photos of the set. For more info please check the [spoiler guide](/r/gameofthrones/w/spoiler_guide).\n\n*I am a bot, and this action was performed automatically. Please [contact the moderators of this subreddit](/message/compose/?to=/r/gameofthrones) if you have any questions or concerns.*''',
                   author=Create_AutoModerator()),
        StubMockup(id=u'ei24t7q', ups=1, score=1, parent_id=u't3_ayn7rq', stickied=False, is_submitter=True,
                   distinguished=None, created=1552035845.0, created_utc=1552035845.0,
                   permalink=u'/r/gameofthrones/comments/ayn7rq/spoilers_game_of_thrones_best_duels/ei24t7q/',
                   body=u'''Am really hoping we get CleganeBowl in season 8.''', author=Create_Anna_Lee1()),
        StubMockup(id=u'ei250wm', ups=1, score=1, parent_id=u't1_ei24t7q', stickied=False, is_submitter=False,
                   distinguished=None, created=1552036212.0, created_utc=1552036212.0,
                   permalink=u'/r/gameofthrones/comments/ayn7rq/spoilers_game_of_thrones_best_duels/ei250wm/',
                   body=u'''Has to happen Anna''', author=Create_Marc_Burde())], submission)
    submission.id = u'ayn7rq'
    submission.fullname = u't3_ayn7rq'
    submission.created = 1552027475.0
    submission.created_utc = 1552027475.0
    submission.author = Create_Anna_Lee1()
    submission.score = 1
    submission.upvote_ratio = 0.57
    submission.num_comments = 3
    submission.stickied = False
    submission.title = u'[Spoilers] Game of Thrones Best Duels'
    submission.permalink = u'/r/gameofthrones/comments/ayn7rq/spoilers_game_of_thrones_best_duels/'

    return submission


def Create_AutoModerator():
    author = AuthorStub()
    author.name = u'AutoModerator'
    author.created = 1325741068.0
    author.created_utc = 1325741068.0
    author.is_employee = False
    author.id = u'6l4z3'
    author.fullname = 't2_6l4z3'
    author.comment_karma = 445850
    author.is_gold = True
    author.is_mod = True
    author.link_karma = 1778
    return author


def Create_Anna_Lee1():
    author = AuthorStub()
    author.name = u'Anna_Lee1'
    author.created = 1551091826.0
    author.created_utc = 1551091826.0
    author.is_employee = False
    author.id = u'3ao09624'
    author.fullname = 't2_3ao09624'
    author.comment_karma = 0
    author.is_gold = False
    author.is_mod = False
    author.link_karma = 1
    return author


def Create_Marc_Burde():
    author = AuthorStub()
    author.name = u'Marc_Burde'
    author.created = 1546888307.0
    author.created_utc = 1546888307.0
    author.is_employee = False
    author.id = u'xl71esk'
    author.fullname = 't2_xl71esk'
    author.comment_karma = 48
    author.is_gold = False
    author.is_mod = False
    author.link_karma = 131
    return author


def Create_Roseanne_Barf_dressed_as_Hitler_making_burnt_Jew_cookies():
    submission = SubmissionStub()
    submission.comments = StubMockup()
    submission.comments.replace_more = types.MethodType(lambda self, **kargs: None, submission)
    submission.comments.list = types.MethodType(lambda self: [
        StubMockup(id=u'dww23lk', ups=32, score=32, parent_id=u't3_8a5pf1', stickied=False, is_submitter=False,
                   distinguished=None, created=1522980039.0, created_utc=1522980039.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dww23lk/',
                   body=u'''Washed up old bitch. I guarantee the reason her new show is such a "success" is because 
                   of all the trumptards watching it. \n\nFrankly she can take a long walk off a short pier and take 
                   her president with her.''',
                   author=Create_ForeverAbone_r()),
        StubMockup(id=u'dww3tai', ups=21, score=21, parent_id=u't3_8a5pf1', stickied=False, is_submitter=False,
                   distinguished=None, created=1522981721.0, created_utc=1522981721.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dww3tai/',
                   body=u'''Wow. That's pretty fucking awful. Same old disgusting bitch she always was. Terrible.''',
                   author=Create_luncheonette()),
        StubMockup(id=u'dww1w1c', ups=24, score=24, parent_id=u't3_8a5pf1', stickied=False, is_submitter=True,
                   distinguished=None, created=1522979844.0, created_utc=1522979844.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dww1w1c/',
                   body=u'''Now you know why Trump loves her....''', author=Create_atreddit13()),
        StubMockup(id=u'dwwgefy', ups=12, score=12, parent_id=u't3_8a5pf1', stickied=False, is_submitter=False,
                   distinguished=None, created=1522998387.0, created_utc=1522998387.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dwwgefy/',
                   body=u'''And people defend this with: "It's just a joke about nazis! Why are you so 
                   offended!"\n\nIt's not a joke about nazis. The pun in this joke is not about the nazis. The "oven" 
                   meme is popular among nazis. The only ones that laugh about this are the fucking nazis. ''',
                   author=Create_HoomanGuy()),
        StubMockup(id=u'dww5ijp', ups=7, score=7, parent_id=u't3_8a5pf1', stickied=False, is_submitter=False,
                   distinguished=None, created=1522983428.0, created_utc=1522983428.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dww5ijp/',
                   body=u'''I really liked the original show. It was funny and down to earth, way more realistic than 
                   most sit coms. But clearly you don't have to be a good person to make funny tv. \n\n I wouldn't 
                   watch her new show and obviously this picture is in terrible taste but I also don't like seeing 
                   people call her a bitch on here. That seems more like what the alt right would do. Lets try to 
                   channel our inner Michelle's and go high on this one.''',
                   author=Create_ToasterHands()),
        StubMockup(id=u'dww69z8', ups=8, score=8, parent_id=u't3_8a5pf1', stickied=False, is_submitter=False,
                   distinguished=None, created=1522984238.0, created_utc=1522984238.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dww69z8/',
                   body=u'''You know Roseanne had a good show back in the day.  It was down to earth and really hit 
                   the topics of being lower middle class and struggling to make it.\n\nThere reboot is ok, 
                   but something about the magic of the first is gone....\n\nThen I saw this and you couldn't get me 
                   to watch the new show for all the tea in China.  This is beyond tasteless and down right 
                   disgusting and dangerous.''',
                   author=Create_boot20()),
        StubMockup(id=u'dwwhds4', ups=6, score=6, parent_id=u't3_8a5pf1', stickied=False, is_submitter=False,
                   distinguished=None, created=1523000368.0, created_utc=1523000368.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dwwhds4/',
                   body=u'''This is what passes for "comedy" in 2018?''', author=Create_SenselessDunderpate()),
        StubMockup(id=u'dww251p', ups=7, score=7, parent_id=u't3_8a5pf1', stickied=False, is_submitter=False,
                   distinguished=None, created=1522980077.0, created_utc=1522980077.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dww251p/',
                   body=u'''Wow....Roseanne's Jewish too wtf''', author=Create_GringoEcuadorian1216()),
        StubMockup(id=u'dwxdjlz', ups=2, score=2, parent_id=u't3_8a5pf1', stickied=False, is_submitter=False,
                   distinguished=None, created=1523039000.0, created_utc=1523039000.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dwxdjlz/',
                   body=u'''This woman is a psychiatricly disturbed unfeeling hag.''',
                   author=Create_moderndaycassiusclay()),
        StubMockup(id=u'dwzw61e', ups=2, score=2, parent_id=u't3_8a5pf1', stickied=False, is_submitter=False,
                   distinguished=None, created=1523153181.0, created_utc=1523153181.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dwzw61e/',
                   body=u'''What's interesting is that she did this for a [Jewish magazine as a joke](
                   https://www.snopes.com/fact-check/did-roseanne-pose-as-adolf-hitler-photo-shoot/)''',
                   author=Create_Vandrin()),
        StubMockup(id=u'dx0chem', ups=1, score=1, parent_id=u't3_8a5pf1', stickied=False, is_submitter=False,
                   distinguished=None, created=1523178401.0, created_utc=1523178401.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dx0chem/',
                   body=u'''This has to be recent judging by her turkey neck..''', author=Create_ChaBuddehG()),
        StubMockup(id=u'dwwxzwg', ups=1, score=1, parent_id=u't3_8a5pf1', stickied=False, is_submitter=False,
                   distinguished=None, created=1523025564.0, created_utc=1523025564.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dwwxzwg/',
                   body=u'''Supposedly this from a satire magazine for Jewish people. I don't see the satire in it. I 
                   personally think it's tasteless. It just goes to show the content of her character, and how little 
                   she thinks of mass suffering that occurred during the holocaust, imo. ''',
                   author=None),
        StubMockup(id=u'dwzrq29', ups=0, score=0, parent_id=u't3_8a5pf1', stickied=False, is_submitter=False,
                   distinguished=None, created=1523148158.0, created_utc=1523148158.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dwzrq29/',
                   body=u'''Dang that's pretty funny''', author=Create_Terrycorn()),
        StubMockup(id=u'dww4eva', ups=14, score=14, parent_id=u't1_dww23lk', stickied=False, is_submitter=False,
                   distinguished=None, created=1522982310.0, created_utc=1522982310.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dww4eva/',
                   body=u'''The original series was decent, but not great. Tbh, John Goodman is the only reason I 
                   liked the show. ''',
                   author=Create_WehrabooTears1944()),
        StubMockup(id=u'dww3rnv', ups=0, score=0, parent_id=u't1_dww251p', stickied=False, is_submitter=False,
                   distinguished=None, created=1522981678.0, created_utc=1522981678.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dww3rnv/',
                   body=u'''No she's really not. ''', author=Create_luncheonette()),
        StubMockup(id=u'dww4fr6', ups=0, score=0, parent_id=u't1_dww251p', stickied=False, is_submitter=False,
                   distinguished=None, created=1522982335.0, created_utc=1522982335.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dww4fr6/',
                   body=u'''Isn't she only half? ''', author=Create_WehrabooTears1944()),
        StubMockup(id=u'dww6stm', ups=11, score=11, parent_id=u't1_dww4eva', stickied=False, is_submitter=False,
                   distinguished=None, created=1522984810.0, created_utc=1522984810.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dww6stm/',
                   body=u'''John Goodman is good, he's been in some classic movies and he seems like he's pretty down 
                   to earth......Roseanne? Yeah not so much.''',
                   author=Create_GringoEcuadorian1216()),
        StubMockup(id=u'dww3vm2', ups=4, score=4, parent_id=u't1_dww3rnv', stickied=False, is_submitter=False,
                   distinguished=None, created=1522981787.0, created_utc=1522981787.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dww3vm2/',
                   body=u'''Yes she is. Her last name Barr is a common Jewish last name and I just read her 
                   autobiography.....''',
                   author=Create_GringoEcuadorian1216()),
        StubMockup(id=u'dww4i84', ups=7, score=7, parent_id=u't1_dww4fr6', stickied=False, is_submitter=False,
                   distinguished=None, created=1522982402.0, created_utc=1522982402.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dww4i84/',
                   body=u'''No, I read that both of her parents are Jewish. I believe it said that her maternal 
                   grandparents were Orthodox Jews from Austria-Hungary.''',
                   author=Create_GringoEcuadorian1216()),
        StubMockup(id=u'dww5fuh', ups=1, score=1, parent_id=u't1_dww3vm2', stickied=False, is_submitter=False,
                   distinguished=None, created=1522983350.0, created_utc=1522983350.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dww5fuh/',
                   body=u'''...so she has Hebrew blood, she's not a practicing member of the Jewish faith.''',
                   author=Create_luncheonette()),
        StubMockup(id=u'dww4th1', ups=0, score=0, parent_id=u't1_dww4i84', stickied=False, is_submitter=False,
                   distinguished=None, created=1522982710.0, created_utc=1522982710.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dww4th1/',
                   body=u'''If that's true, then she's a self-hating Jew. There's some out there like that 
                   unfortunately. ''',
                   author=Create_WehrabooTears1944()),
        StubMockup(id=u'dww5ipa', ups=11, score=11, parent_id=u't1_dww5fuh', stickied=False, is_submitter=False,
                   distinguished=None, created=1522983434.0, created_utc=1522983434.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew'
                             u'/dww5ipa/',
                   body=u'''Apparently she used to be practicing. Even then, to do something like this and pretty 
                   much shitting on your grandparents' memory like that is disgusting.''',
                   author=Create_GringoEcuadorian1216()),
        StubMockup(id=u'dww5g7q', ups=6, score=6, parent_id=u't1_dww4th1', stickied=False, is_submitter=False,
                   distinguished=None, created=1522983360.0, created_utc=1522983360.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew/dww5g7q/',
                   body=u'''Yeah, so it is confirmed she's Jewish. This is why her ex-husband Tom Arnold converted to Judaism while they were together. Yeah I guess she is self-hating.''',
                   author=Create_GringoEcuadorian1216()),
        StubMockup(id=u'dww867d', ups=2, score=2, parent_id=u't1_dww5ipa', stickied=False, is_submitter=False,
                   distinguished=None, created=1522986337.0, created_utc=1522986337.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew/dww867d/',
                   body=u'''It's terrible ''', author=Create_luncheonette()),
        StubMockup(id=u'dww5ixz', ups=3, score=3, parent_id=u't1_dww5g7q', stickied=False, is_submitter=True,
                   distinguished=None, created=1522983441.0, created_utc=1522983441.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew/dww5ixz/',
                   body=u'''If I was her I'd hate myself too''', author=Create_atreddit13()),
        StubMockup(id=u'dww6q2x', ups=2, score=2, parent_id=u't1_dww5ixz', stickied=False, is_submitter=False,
                   distinguished=None, created=1522984728.0, created_utc=1522984728.0,
                   permalink=u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew/dww6q2x/',
                   body=u'''Yeah ,I'm probably gonna tune out of this picture, its kind of fucking grossing me out ....''',
                   author=Create_GringoEcuadorian1216())], submission)
    submission.id = u'8a5pf1'
    submission.fullname = u't3_8a5pf1'
    submission.created = 1522979758.0
    submission.created_utc = 1522979758.0
    submission.author = Create_atreddit13()
    submission.score = 49
    submission.upvote_ratio = 0.76
    submission.num_comments = 26
    submission.stickied = False
    submission.title = u'Roseanne Barf dressed as Hitler making "burnt Jew" cookies'
    submission.permalink = u'/r/Fuckthealtright/comments/8a5pf1/roseanne_barf_dressed_as_hitler_making_burnt_jew/'

    return submission


def Create_moderndaycassiusclay():
    author = AuthorStub()
    author.name = u'moderndaycassiusclay'
    author.created = 1325038677.0
    author.created_utc = 1325038677.0
    author.is_employee = False
    author.id = u'6iyzl'
    author.fullname = 't2_6iyzl'
    author.comment_karma = 46746
    author.is_gold = False
    author.is_mod = False
    author.link_karma = 1790
    return author


def Create_WehrabooTears1944():
    author = AuthorStub()
    author.name = u'WehrabooTears1944'
    author.created = 1518813840.0
    author.created_utc = 1518813840.0
    author.is_employee = False
    author.id = u'xdq3j21'
    author.fullname = 't2_xdq3j21'
    author.comment_karma = 67002
    author.is_gold = False
    author.is_mod = False
    author.link_karma = 2568
    return author


def Create_SenselessDunderpate():
    author = AuthorStub()
    author.name = u'SenselessDunderpate'
    author.created = 1499853837.0
    author.created_utc = 1499853837.0
    author.is_employee = False
    author.id = u'6ueq2hr'
    author.fullname = 't2_6ueq2hr'
    author.comment_karma = 45625
    author.is_gold = False
    author.is_mod = False
    author.link_karma = 23619
    return author


def Create_Vandrin():
    author = AuthorStub()
    author.name = u'Vandrin'
    author.created = 1468240469.0
    author.created_utc = 1468240469.0
    author.is_employee = False
    author.id = u'zf150'
    author.fullname = 't2_zf150'
    author.comment_karma = 13677
    author.is_gold = False
    author.is_mod = True
    author.link_karma = 5693
    return author


def Create_atreddit13():
    author = AuthorStub()
    author.name = u'atreddit13'
    author.created = 1461975410.0
    author.created_utc = 1461975410.0
    author.is_employee = False
    author.id = u'xkj9c'
    author.fullname = 't2_xkj9c'
    author.comment_karma = 13277
    author.is_gold = False
    author.is_mod = False
    author.link_karma = 24553
    return author


def Create_ChaBuddehG():
    author = AuthorStub()
    author.name = u'ChaBuddehG'
    author.created = 1507515674.0
    author.created_utc = 1507515674.0
    author.is_employee = False
    author.id = u'8nhw61e'
    author.fullname = 't2_8nhw61e'
    author.comment_karma = 510
    author.is_gold = False
    author.is_mod = False
    author.link_karma = 1
    return author


def Create_ForeverAbone_r():
    author = AuthorStub()
    author.name = u'ForeverAbone-r'
    author.created = 1326241925.0
    author.created_utc = 1326241925.0
    author.is_employee = False
    author.id = u'6mth8'
    author.fullname = 't2_6mth8'
    author.comment_karma = 178608
    author.is_gold = False
    author.is_mod = True
    author.link_karma = 1393
    return author


def Create_GringoEcuadorian1216():
    author = AuthorStub()
    author.name = u'GringoEcuadorian1216'
    author.created = 1515954238.0
    author.created_utc = 1515954238.0
    author.is_employee = False
    author.id = u'rp1lm9u'
    author.fullname = 't2_rp1lm9u'
    author.comment_karma = 86153
    author.is_gold = False
    author.is_mod = False
    author.link_karma = 94
    return author


def Create_Terrycorn():
    author = AuthorStub()
    author.name = u'Terrycorn'
    author.created = 1497247025.0
    author.created_utc = 1497247025.0
    author.is_employee = False
    author.id = u'3tlcoh7'
    author.fullname = 't2_3tlcoh7'
    author.comment_karma = 3949
    author.is_gold = False
    author.is_mod = False
    author.link_karma = 3546
    return author


def Create_luncheonette():
    author = AuthorStub()
    author.name = u'luncheonette'
    author.created = 1515300567.0
    author.created_utc = 1515300567.0
    author.is_employee = False
    author.id = u'r1wli8d'
    author.fullname = 't2_r1wli8d'
    author.comment_karma = 44474
    author.is_gold = False
    author.is_mod = False
    author.link_karma = 1280
    return author


def Create_ToasterHands():
    author = AuthorStub()
    author.name = u'ToasterHands'
    author.created = 1465537050.0
    author.created_utc = 1465537050.0
    author.is_employee = False
    author.id = u'ylpu5'
    author.fullname = 't2_ylpu5'
    author.comment_karma = 30858
    author.is_gold = False
    author.is_mod = False
    author.link_karma = 28290
    return author


def Create_boot20():
    author = AuthorStub()
    author.name = u'boot20'
    author.created = 1235280808.0
    author.created_utc = 1235280808.0
    author.is_employee = False
    author.id = u'3e09f'
    author.fullname = 't2_3e09f'
    author.comment_karma = 303530
    author.is_gold = False
    author.is_mod = True
    author.link_karma = 4083
    return author


def Create_HoomanGuy():
    author = AuthorStub()
    author.name = u'HoomanGuy'
    author.created = 1465557779.0
    author.created_utc = 1465557779.0
    author.is_employee = False
    author.id = u'ylwkt'
    author.fullname = 't2_ylwkt'
    author.comment_karma = 60886
    author.is_gold = False
    author.is_mod = False
    author.link_karma = 27190
    return author


def Submission_Creator(url):
    reddit = praw.Reddit(client_id='lEFw7PrfHU6fCA',
                         client_secret='u08bDqXnbmoazeL4pR4QYaAR3HE',
                         password='fT56jBnm',
                         user_agent='reddite_crawler',
                         username='LinorLipman')
    submission = reddit.submission(url=url)
    submission.comments.replace_more(limit=None)
    i = 0
    comments = ['''StubMockup(id=u'{0}', ups={1}, score={2}, parent_id=u'{3}', stickied={4}, is_submitter={5},
                       distinguished={6}, created={7}, created_utc={8},
                       permalink=u'{9}', body=u\'\'\'{10}\'\'\', author= {11})'''
                    .format(comment.id, comment.ups, comment.score, comment.parent_id, comment.stickied,
                            comment.is_submitter,
                            'u\'{}\''.format(comment.distinguished) if getattr(comment, 'distinguished', None)
                                                                       is not None else None,
                            comment.created, comment.created_utc, comment.permalink,
                            comment.body.encode('utf-8').replace('\n', '\\n'),
                            'Create_{}()'.format(remove_brackets_underlines(comment.author.name))
                            if comment.author is not None else None)
                for comment in submission.comments.list()]
    reddit_authors = list(set([comment.author for comment in submission.comments.list()]))
    authors = ['''def Create_{0}():
    author = AuthorStub()
    author.name = u'{10}'
    author.created = {1}
    author.created_utc = {2}
    author.is_employee = {3}
    author.id = u'{4}'
    author.fullname = '{5}'
    author.comment_karma = {6}
    author.is_gold = {7}
    author.is_mod = {8}
    author.link_karma = {9}
    return author'''.format(remove_brackets_underlines(author.name), author.created, author.created_utc,
                            author.is_employee, author.id,
                            author.fullname, author.comment_karma, author.is_gold, author.is_mod, author.link_karma,
                            author.name)
               for author in reddit_authors if author is not None]
    submission_stub = '''def Create_{0}():
    submission = SubmissionStub()
    submission.comments = StubMockup()
    submission.comments.replace_more = types.MethodType(lambda self, **kargs: None, submission)
    submission.comments.list = types.MethodType(lambda self: [{1}], submission)
    submission.id = u'{2}'
    submission.fullname = u'{3}'
    submission.created = {4}
    submission.created_utc = {5}
    submission.author = Create_{6}()
    submission.score = {7}
    submission.upvote_ratio = {8}
    submission.num_comments = {9}
    submission.stickied = {10}
    submission.title = u'{11}'

    return submission'''.format(remove_brackets_underlines(submission.title.encode('utf-8')), ', '.join(comments),
                                submission.id,
                                submission.fullname,
                                submission.created, submission.created_utc,
                                remove_brackets_underlines(submission.author.name), submission.score,
                                submission.upvote_ratio, submission.num_comments, submission.stickied,
                                submission.title.encode('utf-8'))
    code = '{}\n\n{}'.format(submission_stub, '\n\n'.join(authors))
    return code, submission_stub, comments, authors


def remove_brackets_underlines(s):
    return s.replace(' ', '_').replace('[', '').replace(']', '')


if __name__ == "__main__":
    # code, test_s, test_c, test_a = Submission_Creator('https://redd.it/8a5pf1')
    # print(code)
    unittest.main()
