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

  def test_total_members_in_score_range(self):
    self.__rank_members_in_leaderboard()
    self.leaderboard.total_members_in_score_range(2, 4).should.be(3)

  def test_score_for(self):
    self.__rank_members_in_leaderboard()
    self.leaderboard.score_for('member_5').should.be(5.0)

  def test_check_member(self):
    self.__rank_members_in_leaderboard()
    self.leaderboard.check_member('member_3').should.be.true
    self.leaderboard.check_member('member_6').should.be.false

  def test_rank_for(self):
    self.__rank_members_in_leaderboard()
    self.leaderboard.rank_for('member_5').should.be(1)

  def test_change_score_for(self):
    self.__rank_members_in_leaderboard()
    self.leaderboard.change_score_for('member_1', 99)
    self.leaderboard.rank_for('member_1').should.be(1)
    self.leaderboard.score_for('member_1').should.be(99.0)

  def test_score_and_rank_for(self):
    self.__rank_members_in_leaderboard()
    score_and_rank = self.leaderboard.score_and_rank_for('member_3')
    score_and_rank['member'].should.be('member_3')
    score_and_rank['score'].should.be(3.0)
    score_and_rank['rank'].should.be(3)

  def test_remove_members_in_score_range(self):
    self.__rank_members_in_leaderboard()
    self.leaderboard.total_members().should.be(5)
    self.leaderboard.remove_members_in_score_range(2, 4)
    self.leaderboard.total_members().should.be(2)

  def test_leaders(self):
    self.__rank_members_in_leaderboard(27)
    leaders = self.leaderboard.leaders(1)
    len(leaders).should.be(25)
    leaders[0]['member'].should.be('member_26')
    leaders[0]['rank'].should.be(1)
    leaders[24]['member'].should.be('member_2')

    leaders = self.leaderboard.leaders(2)
    len(leaders).should.be(1)
    leaders[0]['member'].should.be('member_1')
    leaders[0]['rank'].should.be(26)

    leaders = self.leaderboard.leaders(1, page_size = 5)
    len(leaders).should.be(5)

  def test_ranked_in_list(self):
    self.__rank_members_in_leaderboard(27)
    leaders = self.leaderboard.ranked_in_list(['member_1', 'member_15', 'member_25'])
    len(leaders).should.be(3)
    leaders[0]['member'].should.be('member_1')
    leaders[1]['member'].should.be('member_15')
    leaders[2]['member'].should.be('member_25')

  def test_all_leaders(self):
    self.__rank_members_in_leaderboard(26)
    leaders = self.leaderboard.all_leaders()
    len(leaders).should.be(25)
    leaders[0]['member'].should.be('member_25')

  def test_around_me(self):
    self.__rank_members_in_leaderboard(Leaderboard.DEFAULT_PAGE_SIZE * 3 + 1)

    self.leaderboard.total_members().should.be(Leaderboard.DEFAULT_PAGE_SIZE * 3 + 1)

    leaders_around_me = self.leaderboard.around_me('member_30')
    (len(leaders_around_me) / 2).should.be(self.leaderboard.page_size / 2)

    leaders_around_me = self.leaderboard.around_me('member_1')
    len(leaders_around_me).should.be(self.leaderboard.page_size / 2 + 1)

    leaders_around_me = self.leaderboard.around_me('member_76')
    (len(leaders_around_me) / 2).should.be(self.leaderboard.page_size / 2)


  def __rank_members_in_leaderboard(self, members_to_add = 6):
    for index in range(1, members_to_add):
      self.leaderboard.rank_member('member_%s' % index, index, { 'member_name': 'Leaderboard member %s' % index })
