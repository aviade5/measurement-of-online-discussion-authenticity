from __future__ import print_function
import schedule
from datetime import timedelta, time
from datetime import datetime
import time
from Twitter_API.twitter_api_requester import TwitterApiRequester
from commons.method_executor import Method_Executor
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from win32api import Sleep
import math
import pandas as pd
import random



class PostManager(Method_Executor):
    def __init__(self, db):
        Method_Executor.__init__(self, db)
        self._twitter_api = TwitterApiRequester()
        self._influence_strategy = self._config_parser.eval(self.__class__.__name__, "post_strategy")
        self._source_group = self._config_parser.eval(self.__class__.__name__, "source_group")
        self._target_group = self._config_parser.eval(self.__class__.__name__, "target_group")
        self._team = self._config_parser.eval(self.__class__.__name__, "team")
        self._user_id = self._config_parser.eval(self.__class__.__name__, "user_id")
        self.number_of_posts = self._config_parser.eval(self.__class__.__name__, "number_of_posts")
        self.retweet_precent = self._config_parser.eval(self.__class__.__name__, "retweet_precent")
        self.related_hashtags = self._config_parser.eval(self.__class__.__name__, "related_hashtags")



    def publish_post(self, post,message,media):
            self._twitter_api = TwitterApiRequester()
            statuses = self._twitter_api.api.PostUpdate(message,media)
            activity = self._db.create_activity(self._user_id, post.post_osn_id,statuses.id,'twitter_post', 'twitter', message, datetime.utcnow(),"twitter")
            self._db.addPosts([activity])

    def retweet_post(self, post):
        self._twitter_api = TwitterApiRequester()
        statuses = self._twitter_api.api.PostRetweet(post.post_osn_id, trim_user=False)
        activity = self._db.create_activity(self._user_id, post.post_osn_id, statuses.id, 'twitter_retweet', 'twitter',
                                            post.content, datetime.utcnow(), "twitter")
        self._db.addPosts([activity])




    def publish_comment(self,message,media,status_id):
        statuses = self._twitter_api.api.PostUpdate(message, in_reply_to_status_id=int(status_id), media=media)
        activity = self._db.create_activity(self._user_id, status_id,statuses.id, 'twitter_comment', 'twitter', message,
                                            datetime.utcnow(), "twitter")
        self._db.addPosts([activity])

    def get_posts(self):

        team_guid = self._db.get_author_guid_by_screen_name(self._source_group)
        team_posts = []
        for i in team_guid:
            team_posts.append(self._db.get_posts_by_author_guid(i))
        team_posts = [sublist for item in team_posts for sublist in item]
        team_posts_without_retweet = []
        team_posts_with_retweet = []
        for post in team_posts:
            prefix = str(post.content[0:2])
            if prefix != "RT":
                team_posts_without_retweet.append(post)
            else:
                team_posts_with_retweet.append(post)
        return team_posts_without_retweet,team_posts_with_retweet



    def execute_post_process(self, team_posts_without_retweet,team_posts_with_retweet, coin):

        flag = False
        while flag == False:
            if (coin >= self.retweet_precent):
             post, team_posts_without_retweet,team_posts_with_retweet = self.selecting_post(team_posts_without_retweet,team_posts_with_retweet,"post")
            else:
                post, team_posts_without_retweet, team_posts_with_retweet = self.selecting_post(team_posts_without_retweet, team_posts_with_retweet, "retweet")
            message = post.content
            media = post.media_path

            message = message + '\n' + "@" + self._target_group + '\n' + "#" + self._target_group +" "+ self.related_hashtags
            #message = message + '\n' + "#" + self._target_group + " #" + self._team + '\n' + "@" + self._target_group
            print(message)
            print("test = ", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
            if (coin >= self.retweet_precent):
                try:
                    self.publish_post(post, message, media)
                    flag = True
                except:
                    flag = False
                    continue
            else:
                try:
                    self.retweet_post(post)
                    flag = True
                except:
                    flag = False
                    continue

                flag = True

    def selecting_post(self, team_posts_without_retweet, team_posts_with_retweet, type):

        if type == "post":

            if self._influence_strategy == "last":
                team_posts_without_retweet.sort(key=lambda x: x.date, reverse=True)

            if self._influence_strategy == "popular":
                team_posts_without_retweet.sort(key=lambda x: x.favorite_count, reverse=True)

            post_exist = True

            while post_exist == True:
                ans = team_posts_without_retweet[0]
                message = ans.content
                while "@" + self._target_group in message:
                    if(len(team_posts_without_retweet)>=1):
                        del team_posts_without_retweet[0]
                    else:
                        print("End of tweets")
                        break


                post_exist = self._db.check_if_post_sent(ans, self._user_id)
                if (len(team_posts_without_retweet) >= 1):
                    del team_posts_without_retweet[0]
                else:
                    print("End of tweets")
                    break

        else:

            if self._influence_strategy == "last":
                team_posts_with_retweet.sort(key=lambda x: x.date, reverse=True)

            if self._influence_strategy == "popular":
                team_posts_with_retweet.sort(key=lambda x: x.favorite_count, reverse=True)

            post_exist = True

            while post_exist == True:
                ans = team_posts_with_retweet[0]
                message = ans.content
                while "@" + self._target_group in message:
                    if (len(team_posts_with_retweet) >= 1):
                         del team_posts_with_retweet[0]
                    else:
                        print("End of tweets")
                        break

                post_exist = self._db.check_if_post_sent(ans, self._user_id)
                if (len(team_posts_with_retweet) >= 1):
                    del team_posts_with_retweet[0]
                else:
                        print("End of tweets")
                        break

        return ans, team_posts_without_retweet, team_posts_with_retweet

    def time_schedule(self):


        hours_in_a_day = 24 * 60
        minute_window = float(hours_in_a_day) / self.number_of_posts
        posts_num = 1
        coin = random.uniform(0, 1)
        without_retweet, with_retweet = self.get_posts()
        self.execute_post_process(without_retweet, with_retweet, coin)

        while (self.number_of_posts > posts_num):
            coin = random.uniform(0, 1)
            posts_num = posts_num + 1
            schedule.every(minute_window).minutes.do(self.execute_post_process, without_retweet, with_retweet, coin)

            while True:
                schedule.run_pending()
                time.sleep(1)

    def get_echo_comment(self, post_urls):
        options = webdriver.FirefoxOptions()
        options.set_preference("dom.push.enabled", False)  # Setting firefox options to disable push notifications
        # driver = webdriver.Firefox(executable_path=r'D:\chrome-driver\geckodriver.exe', firefox_options=options)
        driver = webdriver.Firefox(executable_path=r'C:\Python27\geckodriver.exe', firefox_options=options)
        driverWait = WebDriverWait(driver, 4)  # Waiting threshold for loading page is 10 seconds

        # while len(post_urls):
        #     temp_post_urls = post_urls
        for post_url in post_urls:
            driver.get(post_url)
            Sleep(5000)
            comments = driver.find_elements_by_xpath(
                "//div[@class='css-1dbjc4n']/div[@class='css-1dbjc4n r-6337vo']/div/*/*/*/*/div")
            if len(comments) > 2:
                try:
                    comments_page = driver.find_element_by_xpath(
                        "//div[@class='css-1dbjc4n']/div[@class='css-1dbjc4n r-6337vo']/div/*/*/*/*/div[3]/div/*/*/div[2]/div[2]/div[@class='css-901oao r-hkyrab r-1qd0xha r-a023e6 r-16dba41 r-ad9z0x r-bcqeeo r-bnwqim r-qvutc0']")
                except:
                    continue
                comment_text = comments_page.text
                yield (post_url, comment_text)
            # temp_post_urls.remove(post_url)
            # post_urls = temp_post_urls
            # Sleep(10000)
        driver.quit()

    def comment_echo(self):

        while True:

            posts_links = self._db.get_published_posts_from_activity(self._user_id)
            while(len(posts_links)==0):
                posts_links = self._db.get_published_posts_from_activity(self._user_id)

        #posts_links = ["https://twitter.com/yosishahbar/status/1226151866893045760", "https://twitter.com/LFCTV/status/1226147167963930630"]
            for i in self.get_echo_comment(posts_links):
                    id = i[0].split("/")[-1]

                    source  = self._db.get_post_source_from_activity(self._user_id, id)[0]
                    username = self._db.get_username(source)
                    if (self._db.is_comment_exist(self._user_id,source,"@"+username+" " +i[1])) or len(i[1])==0:
                        continue
                    self.publish_comment("@"+username+" " +i[1],None,source)
                    print (i)

        print ("finished")




    def calculate_posts_stat(self):

        author_guid="0927dc1a-8bcb-3488-99ed-7a962aee56e2"
        date = "2020-03-04 03:28:20"

        ids = self._db.source_destination()

        author_posts = self._db.posts_statics_from_date(author_guid, date)
        author_posts_guid = self._db.posts_statics_guids(author_guid, date)

        author_posts_guid = [ids[i] for i in author_posts_guid]
        influencers_posts = self._db.posts_statics_from_date_for_specific_posts(author_posts_guid)

        df1 = pd.DataFrame(author_posts,
                          columns=['author_guid', 'post_count','retweet_sum', 'favorite_sum', 'retweet_avg', 'favorite_avg'])
        df2 = pd.DataFrame(influencers_posts,
                          columns=['author_guid', 'post_count','retweet_sum', 'favorite_sum', 'retweet_avg', 'favorite_avg'])
        frames = [df1, df2]

        result = pd.concat(frames)


        result.to_csv(author_guid+".csv")

    def check_if_post_is_sport_related(self, post):


        i=0

