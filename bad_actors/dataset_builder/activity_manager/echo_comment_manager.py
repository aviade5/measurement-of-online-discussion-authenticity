from __future__ import print_function
from datetime import datetime
from commons.commons import *
from Twitter_API.twitter_api_requester import TwitterApiRequester
from commons.method_executor import Method_Executor
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from win32api import Sleep
from twitter_rest_api.twitter_rest_api import Twitter_Rest_Api




class EchoManager(Method_Executor):

    def __init__(self, db):
        Method_Executor.__init__(self, db)
        self._twitter_api = TwitterApiRequester()
        self._social_network_crawler = Twitter_Rest_Api(db)
        self._target_id = self._config_parser.eval(self.__class__.__name__, "target_id")
        self._source_id = self._config_parser.eval(self.__class__.__name__, "source_id")
        self.source_username = self._config_parser.eval(self.__class__.__name__, "source_username")




    def _publish_comment(self,message,media,status_id):
        statuses = self._twitter_api.api.PostUpdate(message, in_reply_to_status_id=int(status_id), media=media)
        activity = self._db.create_activity(self.target_id, status_id,statuses.id, 'twitter_comment', 'twitter', message,
                                            datetime.datetime.utcnow(), "twitter")
        self._db.addPosts([activity])




    def _get_echo_comment(self, post_urls):
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
                    comments_page = driver.find_element_by_xpath("//div[@class='css-901oao r-hkyrab r-1qd0xha r-a023e6 r-16dba41 r-ad9z0x r-bcqeeo r-bnwqim r-qvutc0']")
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
            posts_links = self._db.get_published_posts_from_activity(self._source_id,self.source_username)
            while(len(posts_links)==0):
                posts_links = self._db.get_published_posts_from_activity(self._source_id,self.source_username)

        #posts_links = ["https://twitter.com/yosishahbar/status/1226151866893045760", "https://twitter.com/LFCTV/status/1226147167963930630"]
            for i in self._get_echo_comment(posts_links):
                    id = i[0].split("/")[-1]

                    source  = self._db.get_post_source_from_activity(self._source_id, id)[0]
                    username = self._db.get_username(source)
                    if (self._db.is_comment_exist(self._target_id,source,"@"+username+" " +i[1])) or len(i[1])==0:
                        continue
                    self._publish_comment("@"+username+" " +i[1],None,source)
                    print (i)

        print ("finished")