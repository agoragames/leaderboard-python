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

  def test_init_with_defaults(self):
    'name'.should.equal(self.leaderboard.leaderboard_name)
    (1).should.equal(len(self.leaderboard.options))
    self.leaderboard.options['connection_pool'].should.be.a(ConnectionPool)
    self.leaderboard.redis_connection.should.be.a(Redis)
    self.leaderboard.DEFAULT_PAGE_SIZE.should.equal(self.leaderboard.page_size)

  def test_init_sets_page_size_to_default_if_set_to_invalid_value(self):
    self.leaderboard = Leaderboard('name', page_size=0)
    self.assertEquals(self.leaderboard.DEFAULT_PAGE_SIZE, self.leaderboard.page_size)

  def test_init_uses_connection_pooling(self):
    lb0 = Leaderboard('lb0', db = 0)
    lb1 = Leaderboard('lb1', db = 0)
    lb2 = Leaderboard('lb2', db = 1)

    lb0.redis_connection.connection_pool.should.equal(lb1.redis_connection.connection_pool)
    lb0.redis_connection.connection_pool.should_not.equal(lb2.redis_connection.connection_pool)