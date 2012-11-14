from redis import Redis, ConnectionPool
from leaderboard import Leaderboard
import unittest
import time
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

  def test_member_data_for(self):
    self.__rank_members_in_leaderboard()
    self.leaderboard.member_data_for('member_1').should.eql(str({'member_name': 'Leaderboard member 1'}))

  def test_update_member_data(self):
    self.__rank_members_in_leaderboard()
    self.leaderboard.update_member_data('member_1', {'member_name': 'Updated Leaderboard member 1'})
    self.leaderboard.member_data_for('member_1').should.eql(str({'member_name': 'Updated Leaderboard member 1'}))

  def test_remove_member_data(self):
    self.__rank_members_in_leaderboard()
    self.leaderboard.remove_member_data('member_1')
    self.leaderboard.member_data_for('member_1').should.be(None)

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

  def test_page_for(self):
    self.leaderboard.page_for('jones').should.be(0)

    self.__rank_members_in_leaderboard(20)

    self.leaderboard.page_for('member_17').should.be(1)
    self.leaderboard.page_for('member_11').should.be(1)
    self.leaderboard.page_for('member_10').should.be(1)
    self.leaderboard.page_for('member_1').should.be(1)

    self.leaderboard.page_for('member_17', 10).should.be(1)
    self.leaderboard.page_for('member_11', 10).should.be(1)
    self.leaderboard.page_for('member_10', 10).should.be(2)
    self.leaderboard.page_for('member_1', 10).should.be(2)

  def test_percentile_for(self):
    self.__rank_members_in_leaderboard(13)

    self.leaderboard.percentile_for('member_1').should.eql(0)
    self.leaderboard.percentile_for('member_2').should.eql(9)
    self.leaderboard.percentile_for('member_3').should.eql(17)
    self.leaderboard.percentile_for('member_4').should.eql(25)
    self.leaderboard.percentile_for('member_12').should.eql(92)

  def test_expire_leaderboard(self):
    self.__rank_members_in_leaderboard()
    self.leaderboard.expire_leaderboard(3)
    ttl = self.leaderboard.redis_connection.ttl(self.leaderboard.leaderboard_name)
    ttl.should.be.greater_than(1)

  def test_expire_leaderboard_at(self):
    self.__rank_members_in_leaderboard()
    self.leaderboard.expire_leaderboard_at(int(time.time() + 10))
    ttl = self.leaderboard.redis_connection.ttl(self.leaderboard.leaderboard_name)
    ttl.should.be.lower_than(11)

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

  def test_leaders_with_optional_member_data(self):
    self.__rank_members_in_leaderboard()
    leaders = self.leaderboard.leaders(1, with_member_data = True)
    len(leaders).should.be(5)
    leaders[0]['member'].should.be('member_1')
    leaders[0]['member_data'].should.be(str({'member_name': 'Leaderboard member 1'}))

  def test_ranked_in_list_with_sort_by(self):
    self.__rank_members_in_leaderboard(26)
    leaders = self.leaderboard.ranked_in_list(['member_25', 'member_1', 'member_15'], sort_by = 'score')
    len(leaders).should.be(3)
    leaders[0]['member'].should.be('member_1')
    leaders[1]['member'].should.be('member_15')
    leaders[2]['member'].should.be('member_25')

    leaders = self.leaderboard.ranked_in_list(['member_25', 'member_1', 'member_15'], sort_by = 'rank')
    len(leaders).should.be(3)
    leaders[0]['member'].should.be('member_1')
    leaders[1]['member'].should.be('member_15')
    leaders[2]['member'].should.be('member_25')

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

  def test_members_from_score_range(self):
    self.__rank_members_in_leaderboard(26)

    members = self.leaderboard.members_from_score_range(10, 15)

    member_15 = {
      'member': 'member_15', 
      'score': 15.0,
      'rank': 11
    }
    members[0].should.eql(member_15)

    member_10 = {
      'member': 'member_10', 
      'score': 10.0,
      'rank': 16
    }
    members[5].should.eql(member_10)

  def test_members_from_rank_range(self):
    self.__rank_members_in_leaderboard(26)

    members = self.leaderboard.members_from_rank_range(5, 9)

    len(members).should.be(5)
    members[0]['member'].should.eql('member_21')
    members[0]['score'].should.be(21.0)    
    members[4]['member'].should.eql('member_17')

    members = self.leaderboard.members_from_rank_range(1, 1)
    len(members).should.be(1)
    members[0]['member'].should.eql('member_25')

    members = self.leaderboard.members_from_rank_range(1, 26)
    len(members).should.be(25)
    members[0]['member'].should.eql('member_25')
    members[0]['score'].should.be(25)
    members[24]['member'].should.eql('member_1')

  def test_member_at(self):
    self.__rank_members_in_leaderboard(51)
    self.leaderboard.member_at(1)['rank'].should.be(1)
    self.leaderboard.member_at(1)['score'].should.be(50.0)
    self.leaderboard.member_at(26)['rank'].should.be(26)
    self.leaderboard.member_at(50)['rank'].should.be(50)
    self.leaderboard.member_at(51).should.be(None)
    self.leaderboard.member_at(1, with_member_data = True)['member_data'].should.be(str({'member_name': 'Leaderboard member 1'}))
  
  def test_around_me(self):
    self.__rank_members_in_leaderboard(Leaderboard.DEFAULT_PAGE_SIZE * 3 + 1)

    self.leaderboard.total_members().should.be(Leaderboard.DEFAULT_PAGE_SIZE * 3 + 1)

    leaders_around_me = self.leaderboard.around_me('member_30')
    (len(leaders_around_me) / 2).should.be(self.leaderboard.page_size / 2)

    leaders_around_me = self.leaderboard.around_me('member_1')
    len(leaders_around_me).should.be(self.leaderboard.page_size / 2 + 1)

    leaders_around_me = self.leaderboard.around_me('member_76')
    (len(leaders_around_me) / 2).should.be(self.leaderboard.page_size / 2)

  def test_merge_leaderboards(self):
    foo_leaderboard = Leaderboard('foo')
    bar_leaderboard = Leaderboard('bar')

    foo_leaderboard.rank_member('foo_1', 1)
    foo_leaderboard.rank_member('foo_2', 2)
    bar_leaderboard.rank_member('bar_1', 1)
    bar_leaderboard.rank_member('bar_2', 2)
    bar_leaderboard.rank_member('bar_3', 5)

    foo_leaderboard.merge_leaderboards('foobar', ['bar'], aggregate = 'SUM')

    foobar_leaderboard = Leaderboard('foobar')
    foobar_leaderboard.total_members().should.be(5)

    foobar_leaderboard.leaders(1)[0]['member'].should.be('bar_3')

  def test_intersect_leaderboards(self):
    foo_leaderboard = Leaderboard('foo')
    bar_leaderboard = Leaderboard('bar')

    foo_leaderboard.rank_member('foo_1', 1)
    foo_leaderboard.rank_member('foo_2', 2)
    foo_leaderboard.rank_member('bar_3', 6)
    bar_leaderboard.rank_member('bar_1', 3)
    bar_leaderboard.rank_member('foo_1', 4)
    bar_leaderboard.rank_member('bar_3', 5)

    foo_leaderboard.intersect_leaderboards('foobar', ['bar'], aggregate = 'SUM')

    foobar_leaderboard = Leaderboard('foobar')
    foobar_leaderboard.total_members().should.be(2)

    foobar_leaderboard.leaders(1)[0]['member'].should.be('bar_3')
  
  def __rank_members_in_leaderboard(self, members_to_add = 6):
    for index in range(1, members_to_add):
      self.leaderboard.rank_member('member_%s' % index, index, { 'member_name': 'Leaderboard member %s' % index })
