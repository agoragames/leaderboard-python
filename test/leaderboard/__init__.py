import unittest
from leaderboard_test import LeaderboardTest

def all_tests():
  suite = unittest.TestSuite()
  suite.addTest(unittest.makeSuite(LeaderboardTest))
  return suite
