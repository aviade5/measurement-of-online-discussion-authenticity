## Aviad Imports

from __future__ import print_function

from trace import pickle

from commons import commons
from commons.method_executor import Method_Executor
import os
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

# notes

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
        self.osn_ids = self._config_parser.eval(self.__class__.__name__, "osn_ids")
        self._domain = 'Facebook'
        self._author_guid_author_dict = {}

        options = webdriver.FirefoxOptions()
        options.set_preference("dom.push.enabled", False)  # Setting firefox options to disable push notifications
        self.driver = webdriver.Firefox(executable_path=r'C:\Python27\geckodriver.exe', firefox_options=options)
        self.driverWait = WebDriverWait(self.driver, 10)   # Waiting threshold for loading page is 10 seconds

    def __del__(self):
        self.driver.quit()
        print("FacebookCrawler destroyed.")

    def _facebook_login(self):
        """
        Method logs into facebook in facebook homepage.
        """
        if self.driver.current_url == 'about:blank':
            cookie_loaded = self._load_cookies()
            time.sleep(2)
            if cookie_loaded == 'Failed':
                self.driver.get("https://www.facebook.com/")
                self.driver.find_element_by_xpath("//input[@id='email']").send_keys(self._account_user_name)
                self.driver.find_element_by_xpath("//input[@id='pass']").send_keys(self._account_user_pw)
                self.driver.find_element_by_xpath("//input[starts-with(@id, 'u_0_')][@value='Log In']").click()
                self._save_cookies()

    def get_number_of_friends_from_group_members(self):
        """
        Method extract number of friends of all member in the group in config.ini
        Updates/Saves number of friends to facebook_author table.
        """
        self._facebook_login()
        records = self._db.get_authors_by_facebook_group(self._group_id)
        for record in records:
            user_id = record.destination_author_guid
            num_of_friends = self.get_number_of_friends_from_user(user_id)
            print("User id is: " + user_id + " ; Number of friends: " + num_of_friends)
            #TODO: Need to check with aviad about the author table - Need to add number of friends to DB.

    def get_number_of_friends_from_user(self, user_id):
        """
        Method finds the text on a profile page "Friends <num>" on side bar of 'Intro' (not always there),
        'Photos' and 'Friends'.
        :param user_id: User profile page
        :return: The number of friends, or 'User Blocked' if we can't see
        """
        self._facebook_login()
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

    def get_group_members(self):
        """
        Method logs in to facebook and extracts members from given group in config.ini
        Extract from elements under the 'div' with id = 'groupsMemberSection_recently_joined'
        Saves all members (Users, pages) of the group and the group itself as Authors.
        Saves connection from group to member as Group-Member.
        """
        self._facebook_login()
        users_id_to_name_dict = self._get_users_from_group_link('https://www.facebook.com/groups/' + self._group_id + '/members/', number_of_scrolls=0, id='groupsMemberSection_recently_joined')
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
        author.author_osn_id = self._group_id
        author.author_guid = commons.compute_author_guid_by_osn_id(author.author_osn_id)
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
            author.author_osn_id = user_id
            author.author_guid = commons.compute_author_guid_by_osn_id(author.author_osn_id)
            author.domain = self._domain
            author.author_type = "User"
            authors.append(author)
        return authors

    def get_group_admins(self):
        """
        Method logs in to facebook and extracts admin members from the group_id in config.ini
        Extract from elements under the 'div' with id = 'groupsMemberSection_admins_moderators'
        :return: dictionary (key = user_id, value = user_name)
        """
        self._facebook_login()
        users_id_to_name_dict = self._get_users_from_group_link('https://www.facebook.com/groups/'
                                                                + self._group_id + '/admins/', id='groupsMemberSection_admins_moderators')
        return users_id_to_name_dict

    def get_group_number_of_members(self):
        """
        Method gets from span tag in class "_grt _50f8" the text = number of members
        :return: string number of members
        """
        self._facebook_login()
        self.driver.get('https://www.facebook.com/groups/' + self._group_id + '/members/')
        num_of_members = self.driver.find_elements_by_xpath("//span[@class='_grt _50f8']")[0].text
        print(num_of_members)

    def _get_users_from_group_link(self, link, number_of_scrolls=1, id='groupsMemberSection_recently_joined'):
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
        divs = self.driver.find_elements_by_xpath("//div[@id='" + id + "']")
        divs = divs[0].find_elements_by_xpath(".//div[@class='uiProfileBlockContent _61ce']")
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

    def get_about_info_from_group_members(self):
        self._facebook_login()
        group_author_guid = self._db.get_author_guid_by_facebook_osn_id(self._group_id)[0].author_guid
        group_member_connections = self._db.get_authors_by_facebook_group(group_author_guid)

        group_member_guids = [group_member_connection.destination_author_guid for group_member_connection in group_member_connections]
        facebook_authors = self._db.get_authors_by_domain(self._domain)

        for facebook_author in facebook_authors:
            author_guid = facebook_author.author_guid
            if author_guid in group_member_guids:
                self._author_guid_author_dict[author_guid] = facebook_author

        # author_guids = self._author_guid_author_dict.keys()
        authors = self._author_guid_author_dict.values()

        # authors = [self._author_guid_author_dict[author_guid] for author_guid in author_guids]
        self._get_about_info_for_authors(authors)
        self._db.addPosts(authors)

    def get_about_info_from_users(self):
        self._facebook_login()
        authors = []
        for author_osn_id in self.osn_ids:
            author = Author()
            author.domain = self._domain
            author.author_osn_id = author_osn_id
            author.author_type = 'User'
            author.education = 'User Blocked'
            author.professional_skills = 'User Blocked'
            author.past_residence = 'User Blocked'
            author.birth_day = 'User Blocked'
            author.gender = 'User Blocked'
            author.gender = 'User Blocked'
            author.email = 'User Blocked'  # Need to add the rest of the features with User Blocked as default.
            author.work = 'User Blocked'
            self.driver.get('https://www.facebook.com/' + author_osn_id)
            name = self.driver.find_element_by_xpath("//a[@class='_2nlw _2nlv']").text  # Extracting name
            author.name = name
            author.author_guid = commons.compute_author_guid_by_osn_id(author_osn_id)
            authors.append(author)
        self._get_about_info_for_authors(authors)
        self._db.addPosts(authors)

    def _get_about_info_for_authors(self, authors):
        """
        Method browses the About page in user profile.
        Collects all details from all tabs.
        :return: Author with all 'about' info in it
        example: {education: {COLLEGE: University of..., HIGH SCHOOL: Vernon Hills High School}, living: {...}..}
        """
        about_tabs = {'education': {}, 'living': {}, 'contact-info': {}, 'relationship': {}, 'bio': {}}
        for author in authors:
            time.sleep(10)
            for tab in about_tabs:
                time.sleep(5)  # Sleeping between tabs.
                author_osn_id = author.author_osn_id
                self.driver.get('https://www.facebook.com/' + author_osn_id + '/about?section=' + tab)
                user_about_info = self.driver.find_elements_by_xpath("//div[starts-with(@class, '_4qm1')]")
                parsed_info_dic = {}
                for info in user_about_info:
                    txt = info.text
                    txt = txt.split('\n')
                    parsed_info_dic[txt[0]] = txt[1:]    # txt[0] = COLLEGE, txt[1] = University of Illinois at Urbana-Champaign
                about_tabs[tab] = parsed_info_dic
            print("User ID:" + author.author_osn_id + " :: collected:") #TODO remove prints
            print(about_tabs)
            self._update_author_about_info(author, about_tabs)
        # return authors

    def _update_author_about_info(self, author, about_info_dict):
        """
        Method adds about information to given author object.
        :return: Author
        """

        if 'WORK' in about_info_dict['education']:
            work_info = about_info_dict['education']['WORK'][0:]
            author.work = ' '.join(work_info)

        if 'EDUCATION' in about_info_dict['education']:
            education_info = about_info_dict['education']['EDUCATION'][0:]
            author.education = ' '.join(education_info)

        if 'PROFESSIONAL SKILLS' in about_info_dict['education']:
            author.professional_skills = about_info_dict['education']['PROFESSIONAL SKILLS'][0]

        if 'CURRENT CITY AND HOMETOWN' in about_info_dict['living']:
            current_residence_details = about_info_dict['living']['CURRENT CITY AND HOMETOWN'][0:]
            author.current_residence = ' '.join(current_residence_details)

        if 'OTHER PLACES LIVED' in about_info_dict['living']:
            author.past_residence = about_info_dict['living']['OTHER PLACES LIVED'][0]

        if 'BASIC INFORMATION' in about_info_dict['contact-info']:
            if 'Birth Date' in about_info_dict['contact-info']['BASIC INFORMATION'] or 'Birth Year' in about_info_dict['contact-info']['BASIC INFORMATION']:
                birth_date_index = about_info_dict['contact-info']['BASIC INFORMATION'].index('Birth Date')
                birth_year_index = about_info_dict['contact-info']['BASIC INFORMATION'].index('Birth Year')
                birth_day = ""
                if birth_date_index > -1:
                    birth_day = about_info_dict['contact-info']['BASIC INFORMATION'][birth_date_index + 1]
                if birth_year_index > -1:
                    birth_day += about_info_dict['contact-info']['BASIC INFORMATION'][birth_date_index + 1]
                author.birth_day = birth_day

            if 'Gender' in about_info_dict['contact-info']['BASIC INFORMATION']:
                gender_index = about_info_dict['contact-info']['BASIC INFORMATION'].index('Gender')
                author.gender = about_info_dict['contact-info']['BASIC INFORMATION'][gender_index + 1]

        if 'CONTACT INFORMATION' in about_info_dict['contact-info']:
            if 'Email' in about_info_dict['contact-info']['CONTACT INFORMATION']:
                email_index = about_info_dict['contact-info']['CONTACT INFORMATION'].index('Email')
                author.email = about_info_dict['contact-info']['CONTACT INFORMATION'][email_index + 1]

        if 'RELATIONSHIP' in about_info_dict['relationship']:
            author.relationship_status = about_info_dict['relationship']['RELATIONSHIP'][0]

    def parse_member_id(self, txt):
        """
        Method parses member id from ajaxify tag
        """
        indx_member_id = txt.find("member_id=")
        indx_ref = txt.find("&ref")
        return txt[indx_member_id + 10: indx_ref]

    def _save_cookies(self):
        try:
            fp_name = '{}.ProfileName'.format(self._account_user_name.replace('@', ''))
            self.open_if_not_exists(fp_name)
            self.driver.get('http://www.facebook.com')
            pickle.dump(self.driver.get_cookies(), open("{}/Cookies.pkl".format(fp_name), "wb"))
            return 'Success'
        except:
            return 'Failed'

    def _load_cookies(self):
        try:
            fp_name = '{}.ProfileName'.format(self._account_user_name.replace('@', ''))
            self.driver.get('http://www.facebook.com')
            for cookie in pickle.load(open("{}/Cookies.pkl".format(fp_name), "rb")):
                self.driver.add_cookie(cookie)
            return 'Success'
        except:
            print('Failed to load cookies')
            return 'Failed'

    def open_if_not_exists(self, directory):
        # if not os.path.exists(file_path):
        #     with open(file_path, 'w'): pass
        if not os.path.exists(directory):
            os.makedirs(directory)