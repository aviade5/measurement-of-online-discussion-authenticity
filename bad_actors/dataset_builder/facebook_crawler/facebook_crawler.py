## Aviad Imports

from __future__ import print_function
from commons.method_executor import Method_Executor
from commons import commons
## Nitzan Imports
from telnetlib import EC
from datetime import datetime
import time
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from DB.schema_definition import Author, AuthorConnection


__author__ = "Nitzan Maarek"


class FacebookCrawler(Method_Executor):
    def __init__(self, db):
        Method_Executor.__init__(self, db)
        self._account_user_name = self._config_parser.eval(self.__class__.__name__, "account_user_name")
        self._account_user_pw = self._config_parser.eval(self.__class__.__name__, "account_user_pw")
        self._group_id = self._config_parser.eval(self.__class__.__name__, "group_id")
        self._group_name = self._config_parser.eval(self.__class__.__name__, "group_name")
        self._domain = 'Facebook'

        options = webdriver.FirefoxOptions()
        options.set_preference("dom.push.enabled", False)  # Setting firefox options to disable push notifications
        self.driver = webdriver.Firefox(executable_path=r'C:\Python27\geckodriver.exe', firefox_options=options)
        self.driverWait = WebDriverWait(self.driver, 10)   # Waiting threshold for loading page is 10 seconds


    def _facebook_login(self):
        """
        Method logs into facebook in facebook homepage.
        """
        self.driver.get("https://www.facebook.com/")
        self.driver.find_element_by_xpath("//input[@id='email']").send_keys(self._account_user_name)
        self.driver.find_element_by_xpath("//input[@id='pass']").send_keys(self._account_user_pw)
        self.driver.find_element_by_xpath("//input[starts-with(@id, 'u_0_')][@value='Log In']").click()


    def extract_number_of_friends(self):
        """
        Method extract number of friends of all member in the group in config.ini
        Updates/Saves number of friends to facebook_author table.
        """
        #TODO: debug this
        records = self._db.get_authors_by_facebook_group(self._group_id)
        print(records)

    def _get_number_of_friends(self, user_id):
        """
        Method finds the text on a profile page "Friends <num>" on side bar of 'Intro' (not always there),
        'Photos' and 'Friends'.
        :param user_id: User profile page
        :return: The number of friends, or 'User Blocked' if we can't see
        """
        self.driver.get('https://www.facebook.com/' + user_id)
        time.sleep(2)
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        try:
            self.driverWait.until(EC.presence_of_element_located((By.CLASS_NAME, '_39g5')))
        except TimeoutException:
            print("Loading page took too long - timeout exception.")

        # Get list of spans ('Intro', 'Photos', 'Friends')
        friends_span = self.driver.find_elements_by_xpath("//span[@class='_2iel _5kx5']")
        number_of_friends_span = self.driver.find_elements_by_xpath("//a[@class='_39g5']")  # Get number of friends.
        if len(friends_span) == 0:
            print('Page - No Friends')
            return 'Page - No Friends'
        if len(number_of_friends_span) == 0:
            print('User Blocked')
            return 'User Blocked'
        print(number_of_friends_span[0].text)
        return number_of_friends_span[0].text

    def extract_members_from_group(self):
        """
        Method extracts members from given group in config.ini
        Saves all members (Users, pages) of the group and the group itself as Authors.
        Saves connection from group to member as Group-Member.
        """
        user_names, user_ids = self._get_members_from_group()
        authors = self._convert_group_members_to_authors(user_names, user_ids)
        group_as_author = self._convert_group_to_author()
        authors.append(group_as_author)
        self._db.addPosts(authors)  # Add members to Authors table in DB
        connections = self._convert_group_and_members_to_connections(user_ids)
        self._db.addPosts(connections)  # Add group to member connections to AuthorConnection table in DB



    def _convert_group_and_members_to_connections(self, user_id_list):
        """
        Method creates connection objects for group in config.ini with given user_id list.
        Appends all connections created to a list and returns it.
        :return: List of AuthorConnection: src = group_id, dst = member_id
        """
        connection_list = []
        for user_id in user_id_list:
            connection = AuthorConnection()
            group_author_guid = self._get_guid_by_osn_id(self._group_id)
            connection.source_author_guid = group_author_guid
            user_author_osn_id = self._get_guid_by_osn_id(user_id)
            connection.destination_author_guid = user_author_osn_id
            connection.connection_type = 'Group-Member'  #TODO: Check with aviad that this connection type is good.
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            connection.insertion_date = dt_string
            connection_list.append(connection)
        return connection_list

    def _get_guid_by_osn_id(self, osn_id):
        """
        Method return author_guid by author_osn_id and domain = facebook.
        :param osn_id: fbid
        :return: author_guid
        """
        records = self._db.get_author_guid_by_facebook_osn_id(osn_id)
        print(records)
        return records[0].author_guid

    def _convert_group_to_author(self):
        """
        Method takes given group_id from config.ini and creates Author object for it.
        """
        author = Author()
        author.name = self._group_name
        author.author_guid = commons.compute_author_guid_by_author_name(author.name)
        author.author_osn_id = self._group_id
        author.domain = self._domain
        author.author_type = "Group"
        return author

    def _convert_group_members_to_authors(self, user_names_list, user_id_list):
        """
        :return: a list of Author objects ready to be added to DB.
        """
        authors = []
        for i in range(0, len(user_names_list)):
            author = Author()
            author.name = user_names_list[i]
            author.author_guid = commons.compute_author_guid_by_author_name(author.name)
            author.author_osn_id = user_id_list[i]
            author.domain = self._domain
            author.author_type = "User"
            authors.append(author)
        return authors

    def _get_members_from_group(self):
        """
        Method goes to the members page of group.
        Scrolls number of 'iterations' down the page
        Extracts list of user names and user id's (May include pages as users who are members of the group)
        :return: two coordinated lists of user_id and user_name
        """
        self._facebook_login()
        iterations = 1
        self.driver.get('https://www.facebook.com/groups/' + self._group_id + '/members/')
        for i in range(0, iterations):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)  # Seconds between each scroll (think of the page reloading)

        divs_list = self.driver.find_elements_by_xpath("//div[@class='uiProfileBlockContent _61ce']")
        user_names_list = []
        user_id_list = []
        for div in divs_list:
            div_children = div.find_elements_by_xpath(".//*")  # List all children elements
            txt_to_parse = div_children[4].get_attribute("ajaxify")  # ajaxify attr contains member_id and group_id.
            user_id = self._ajaxify_parse_member_id(txt_to_parse)
            username = div.text
            if username.find("\n") > 0:
                username = username[0:username.find("\n")]
            user_names_list.append(username)
            user_id_list.append(user_id)
        print(user_names_list, user_id_list)
        return user_names_list, user_id_list


    def _collect_about_info_from_user(self, user_id):
        """
        Method browses the About page in user profile.
        Collects all details from all tabs.
        :return: dictionary: Keys = the about tabs. Values = dictionary: keys = subjects, values = values.
        example: {education: {COLLEGE: University of..., HIGH SCHOOL: Vernon Hills High School}, living: {...}..}
        """
        about_tabs = {'education': {}, 'living': {}, 'contact-info': {}, 'relationship': {}, 'bio': {}, 'year-overviews': {}}
        for tab in about_tabs:
            self.driver.get('https://www.facebook.com/' + user_id + '/about?section=' + tab)
            info_list = self.driver.find_elements_by_xpath("//div[@class='_4qm1']")
            parsed_info_dic = {}
            for info in info_list:
                txt = info.text
                txt.split('\n')
                parsed_info_dic[txt[0]] = txt[1]    # txt[0] = COLLEGE, txt[1] = University of Illinois at Urbana-Champaign
            about_tabs[tab] = parsed_info_dic
        return about_tabs

    def _ajaxify_parse_member_id(self, txt):
        indx_member_id = txt.find("member_id=")
        indx_ref = txt.find("&ref")
        return txt[indx_member_id + 10: indx_ref]


    def _delete_this_function(self):
        """
        Delete me
        :return:
        """
        return "delete this method now."
