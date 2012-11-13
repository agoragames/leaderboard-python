from redis import Redis, ConnectionPool
from leaderboard import Leaderboard
import unittest
import sure

class LeaderboardTest(unittest.TestCase):
  def setUp(self):
    self.leaderboard = Leaderboard('name')

  def tearDown(self):
    self.leaderboard.redis_connection.flushdb()

  def test_version(self):
    "2.0.0".should.equal(Leaderboard.VERSION)