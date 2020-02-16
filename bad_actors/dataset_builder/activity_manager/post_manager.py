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



class PostManager(Method_Executor):
    def __init__(self, db):
        Method_Executor.__init__(self, db)
        self._twitter_api = TwitterApiRequester()
        self._influence_strategy = self._config_parser.eval(self.__class__.__name__, "post_strategy")
        self._source_group = self._config_parser.eval(self.__class__.__name__, "source_group")
        self._target_group = self._config_parser.eval(self.__class__.__name__, "target_group")
        self._user_id = self._config_parser.eval(self.__class__.__name__, "user_id")


    def publish_post(self, post,message,media):
            statuses = self._twitter_api.api.PostUpdate(message,media)
            activity = self._db.create_activity(self._user_id, post.post_osn_id,statuses.id,'twitter_post', 'twitter', message, datetime.utcnow(),"twitter")
            self._db.addPosts([activity])


    def publish_comment(self,message,media,status_id):
        statuses = self._twitter_api.api.PostUpdate(message, in_reply_to_status_id=int(status_id), media=media)
        activity = self._db.create_activity(self._user_id, status_id,statuses.id, 'twitter_comment', 'twitter', message,
                                            datetime.utcnow(), "twitter")
        self._db.addPosts([activity])

    def get_posts(self):

        team_guid = self._db.get_author_guid_by_screen_name(self._source_group)
        team_posts = self._db.get_posts_by_author_guid(team_guid)
        team_posts_without_retweet = []
        for post in team_posts:
                prefix = str(post.content[0:2])
                if prefix != "RT":
                    team_posts_without_retweet.append(post)

        return team_posts_without_retweet

    def selecting_post(self,team_posts_without_retweet):

        if self._influence_strategy=="last":
            team_posts_without_retweet.sort(key=lambda x: x.date, reverse=True)

        if self._influence_strategy == "popular":
            team_posts_without_retweet.sort(key=lambda x: x.favorite_count, reverse=True)

        post_exist = True
        while post_exist==True:
            ans = team_posts_without_retweet[0]
            post_exist = self._db.check_if_post_sent(ans, self._user_id)
            del team_posts_without_retweet[0]

        return ans,team_posts_without_retweet


    def execute_post_process(self,team_posts_without_retweet):

        post,team_posts_without_retweet = self.selecting_post(team_posts_without_retweet)
        message = post.content
        index = message.index("https://t.co")
        media = post.media_path
        message = message[0:index]

        message = message+'\n' + "#"+self._target_group+'\n'+ "@"+self._target_group
        print(message)
        self.publish_post(post,message,media)

        return team_posts_without_retweet

    def time_schedule(self):

        posts = self.get_posts()
        posts = schedule.every(1).minutes.do(self.execute_post_process,posts)
        while True:
            schedule.run_pending()
            time.sleep(1)



    def get_echo_comment(self,post_urls):
        options = webdriver.FirefoxOptions()
        options.set_preference("dom.push.enabled", False)  # Setting firefox options to disable push notifications
        #driver = webdriver.Firefox(executable_path=r'D:\chrome-driver\geckodriver.exe', firefox_options=options)
        driver = webdriver.Firefox(executable_path=r'C:\Python27\geckodriver.exe', firefox_options=options)
        driverWait = WebDriverWait(driver, 4)  # Waiting threshold for loading page is 10 seconds

        while len(post_urls):
            temp_post_urls = post_urls
            for post_url in post_urls:
                driver.get(post_url)
                Sleep(5000)
                comments = driver.find_elements_by_xpath("//div[@class='css-1dbjc4n']/div[@class='css-1dbjc4n r-6337vo']/div/*/*/*/*/div")
                if len(comments) > 2:
                    comments_page = driver.find_element_by_xpath(
                        "//div[@class='css-1dbjc4n']/div[@class='css-1dbjc4n r-6337vo']/div/*/*/*/*/div[3]/div/*/*/div[2]/div[2]/div[@class='css-901oao r-hkyrab r-1qd0xha r-a023e6 r-16dba41 r-ad9z0x r-bcqeeo r-bnwqim r-qvutc0']")
                    comment_text = comments_page.text
                    yield (post_url, comment_text)
                    temp_post_urls.remove(post_url)
            post_urls = temp_post_urls
            Sleep(10000)
        driver.quit()

    def comment_echo(self):

        posts_links = self._db.get_published_posts_from_activity(self._user_id)

        #posts_links = ["https://twitter.com/yosishahbar/status/1226151866893045760", "https://twitter.com/LFCTV/status/1226147167963930630"]
        for i in self.get_echo_comment(posts_links):
            id = i[0].split("/")[-1]
            source  = self._db.get_post_source_from_activity(self._user_id, id)[0]
            username = self._db.get_username(source)
            self.publish_comment("@"+username+" " +i[1],None,source)
            print (i)

        print ("finished")


