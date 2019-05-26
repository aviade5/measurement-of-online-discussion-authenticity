## Aviad Imports

from __future__ import print_function

from commons import commons
from commons.method_executor import Method_Executor

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


### ******    ******    ******    ******    ******    ******    ******    ******    ******    ******    ******    ******
### ******    ******    ******    ******    ******    ******    ******    ******    ******    ******    ******    ******

# Public methods begin with name "extract" and they include logging into facebook - beginning a logged in session

# Make sure you know that (above) when using this class.

### ******    ******    ******    ******    ******    ******    ******    ******    ******    ******    ******    ******
### ******    ******    ******    ******    ******    ******    ******    ******    ******    ******    ******    ******



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
        self._facebook_login()
        records = self._db.get_authors_by_facebook_group(self._group_id)
        for record in records:
            user_id = record.destination_author_guid
            num_of_friends = self._get_number_of_friends(user_id)
            print("User id is: " + user_id + " ; Number of friends: " + num_of_friends)
            #TODO: Need to check with aviad about the author table - Need to add number of friends to DB.

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
        Method logs in to facebook and extracts members from given group in config.ini
        Saves all members (Users, pages) of the group and the group itself as Authors.
        Saves connection from group to member as Group-Member.
        """
        self._facebook_login()
        users_id_to_name_dict = self._get_members_from_group('https://www.facebook.com/groups/' + self._group_id + '/members/', number_of_scrolls=3)
        authors = self._convert_group_members_to_author(users_id_to_name_dict)
        authors.append(self._convert_group_to_author())
        self._db.addPosts(authors)  # Add members to Authors table in DB
        group_to_members_connections = self._convert_group_and_members_to_connections(users_id_to_name_dict)
        self._db.addPosts(group_to_members_connections)  # Add group to member connections to AuthorConnection table in DB

    def _get_guid_by_osn_id(self, osn_id):
        """
        Method return author_guid by author_osn_id and domain = facebook.
        :param osn_id: fbid
        :return: author_guid
        """
        records = self._db.get_author_guid_by_facebook_osn_id(osn_id)
        first_record = records[0]
        # print(records)
        return first_record.author_guid

    def _convert_group_and_members_to_connections(self, users_id_to_name_dict):
        """
        Method creates connection objects for group in config.ini with given user_id to user_name dictionary.
        Appends all connections created to a list and returns it.
        :return: List of AuthorConnection: src = group_id, dst = member_id
        """

        connections = []
        for user_id in users_id_to_name_dict:
            connection = AuthorConnection()
            group_author_guid = self._get_guid_by_osn_id(self._group_id)
            connection.source_author_guid = group_author_guid
            user_author_osn_id = self._get_guid_by_osn_id(user_id)
            connection.destination_author_guid = user_author_osn_id
            connection.connection_type = 'Group-Member'
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            connection.insertion_date = dt_string
            connections.append(connection)
        return connections


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

    def _convert_group_members_to_author(self, users_id_to_name_dict):
        """
        :return: a list of Author objects ready to be added to DB.
        """
        authors = []
        for user_id in users_id_to_name_dict:
            author = Author()
            author.name = users_id_to_name_dict[user_id]
            author.author_guid = commons.compute_author_guid_by_author_name(author.name)
            author.author_osn_id = user_id
            author.domain = self._domain
            author.author_type = "User"
            authors.append(author)
        return authors


    def extract_group_admins(self):
        """
        Method logs in to facebook and extracts admin members from the group_id in config.ini
        :return: dictionary (key = user_id, value = user_name)
        """
        self._facebook_login()
        users_id_to_name_dict = self._get_members_from_group('https://www.facebook.com/groups/'
                                                                     + self._group_id + '/admins/')
        return users_id_to_name_dict

    def _get_group_number_of_members(self):
        """
        Method gets from span tag in class "_grt _50f8" the text = number of members
        :return: string number of members
        """
        self.driver.get('https://www.facebook.com/groups/' + self._group_id + '/members/')
        num_of_members = self.driver.find_elements_by_xpath("//span[@class='_grt _50f8']")[0].text
        return num_of_members

    def _get_members_from_group(self, link, number_of_scrolls=3):
        """
        Method goes to the members page of group.
        Scrolls number of 'number of scrolls' down the page
        Extracts dictionary user_id to user_name(May include pages as users who are members of the group)
        :return: dictionary: key = user_id, value = user_name
        """
        self.driver.get(link)
        for i in range(0, number_of_scrolls):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)  # Seconds between each scroll (think of the page reloading)

        divs = self.driver.find_elements_by_xpath("//div[@class='uiProfileBlockContent _61ce']")
        users_id_to_name_dict = {}

        for div in divs:
            div_children = div.find_elements_by_xpath(".//*")  # List all children elements
            txt_to_parse = div_children[4].get_attribute("ajaxify")  # ajaxify attr contains member_id and group_id.
            user_id = self.parse_member_id(txt_to_parse)
            user_name = div.text
            if user_name.find("\n") > 0:
                user_name = user_name[0:user_name.find("\n")]
            users_id_to_name_dict[user_id] = user_name

        return users_id_to_name_dict

    def extract_about_info_from_user(self):
        self._facebook_login()
        records = self._db.get_authors_by_facebook_group(self._group_id)
        about_info = None
        for record in records:
            user_id = record.destination_author_guid
            about_info = self._collect_about_info_from_user(user_id)
            #TODO: Need to save about info in DB here.
            print(about_info)

        #TODO: Need to fix the authors table so that we can save about data.

    def _collect_about_info_from_user(self, user_id):
        """
        Method browses the About page in user profile.
        Collects all details from all tabs.
        :return: dictionary: Keys = the about tabs. Values = dictionary: keys = subjects, values = values.
        example: {education: {COLLEGE: University of..., HIGH SCHOOL: Vernon Hills High School}, living: {...}..}
        """
        #TODO: Need to fix "Life Events"
        about_tabs = {'education': {}, 'living': {}, 'contact-info': {}, 'relationship': {}, 'bio': {}, 'year-overviews': {}}
        for tab in about_tabs:
            self.driver.get('https://www.facebook.com/' + user_id + '/about?section=' + tab)
            user_about_info = self.driver.find_elements_by_xpath("//div[@class='_4qm1']")
            parsed_info_dic = {}
            for info in user_about_info:
                txt = info.text
                txt = txt.split('\n')
                parsed_info_dic[txt[0]] = txt[1]    # txt[0] = COLLEGE, txt[1] = University of Illinois at Urbana-Champaign
            about_tabs[tab] = parsed_info_dic
        return about_tabs

    def parse_member_id(self, txt):
        """
        Method parses member id from ajaxify tag
        """
        indx_member_id = txt.find("member_id=")
        indx_ref = txt.find("&ref")
        return txt[indx_member_id + 10: indx_ref]


