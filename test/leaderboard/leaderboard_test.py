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
    Leaderboard.VERSION.should.equal('2.0.0')

  def test_init_with_defaults(self):
    'name'.should.equal(self.leaderboard.leaderboard_name)
    len(self.leaderboard.options).should.be(1)
    self.leaderboard.options['connection_pool'].should.be.a(ConnectionPool)
    self.leaderboard.redis_connection.should.be.a(Redis)
    self.leaderboard.DEFAULT_PAGE_SIZE.should.equal(self.leaderboard.page_size)

  def test_init_sets_page_size_to_default_if_set_to_invalid_value(self):
    self.leaderboard = Leaderboard('name', page_size = 0)
    self.leaderboard.page_size.should.equal(Leaderboard.DEFAULT_PAGE_SIZE)

  def test_init_uses_connection_pooling(self):
    lb0 = Leaderboard('lb0', db = 0)
    lb1 = Leaderboard('lb1', db = 0)
    lb2 = Leaderboard('lb2', db = 1)

    lb0.redis_connection.connection_pool.should.equal(lb1.redis_connection.connection_pool)
    lb0.redis_connection.connection_pool.should_not.equal(lb2.redis_connection.connection_pool)

  def test_delete_leaderboard(self):
    self.__rank_members_in_leaderboard()
    self.leaderboard.redis_connection.exists('name').should.be.true
    self.leaderboard.delete_leaderboard()
    self.leaderboard.redis_connection.exists('name').should.be.false

  def test_total_members(self):
    self.__rank_members_in_leaderboard()
    self.leaderboard.total_members().should.be(5)

  def test_remove_member(self):
    self.__rank_members_in_leaderboard()
    self.leaderboard.total_members().should.be(5)
    self.leaderboard.remove_member('member_1')
    self.leaderboard.total_members().should.be(4)

  def test_total_pages(self):
    self.__rank_members_in_leaderboard(26)
    self.leaderboard.total_members().should.be(26)
    self.leaderboard.total_pages().should.be(2)



  def __rank_members_in_leaderboard(self, members_to_add = 5):
    for index in range(1, members_to_add):
      self.leaderboard.rank_member('member_%s' % index, index, { 'member_name': 'Leaderboard member %s' % index })
