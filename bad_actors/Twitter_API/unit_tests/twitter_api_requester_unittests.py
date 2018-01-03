# Created by Aviad on 29-Apr-16 12:55 PM.
import unittest
from Twitter_API.twitter_api_requester import TwitterApiRequester

class TestTwitterApiRequester(unittest.TestCase):

    def testCredentialsAreValid(self):
        twitter_api_requester = TwitterApiRequester()

        authenticated_user = twitter_api_requester.verify_credentials()
        self.assertIsNotNone(authenticated_user)
        self.assertIsNotNone(authenticated_user.followers_count)
        self.assertIsNotNone(authenticated_user.friends_count)