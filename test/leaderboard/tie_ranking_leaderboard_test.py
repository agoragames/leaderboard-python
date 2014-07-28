from redis import Redis, StrictRedis, ConnectionPool
from leaderboard.tie_ranking_leaderboard import TieRankingLeaderboard
import unittest
import time
import sure


class TieRankingLeaderboardTest(unittest.TestCase):

    def setUp(self):
        self.leaderboard = TieRankingLeaderboard('ties')

    def tearDown(self):
        self.leaderboard.redis_connection.flushdb()

    def test_delete_the_ties_ranking_internal_leaderboard_when_you_delete_a_leaderboard_configured_for_ties(self):
        self.leaderboard.rank_member('member_1', 50)
        self.leaderboard.redis_connection.exists('ties:ties').should.be.true
        self.leaderboard.delete_leaderboard()
        self.leaderboard.redis_connection.exists('ties:ties').should.be.false

    def test_leaders(self):
        self.leaderboard.rank_member('member_1', 50)
        self.leaderboard.rank_member('member_2', 50)
        self.leaderboard.rank_member('member_3', 30)
        self.leaderboard.rank_member('member_4', 30)
        self.leaderboard.rank_member('member_5', 10)

        leaders = self.leaderboard.leaders(1)
        leaders[0]['rank'].should.equal(1)
        leaders[1]['rank'].should.equal(1)
        leaders[2]['rank'].should.equal(2)
        leaders[3]['rank'].should.equal(2)
        leaders[4]['rank'].should.equal(3)

    def test_correct_rankings_for_leaders_with_different_page_sizes(self):
        self.leaderboard.rank_member('member_1', 50)
        self.leaderboard.rank_member('member_2', 50)
        self.leaderboard.rank_member('member_3', 30)
        self.leaderboard.rank_member('member_4', 30)
        self.leaderboard.rank_member('member_5', 10)
        self.leaderboard.rank_member('member_6', 50)
        self.leaderboard.rank_member('member_7', 50)
        self.leaderboard.rank_member('member_8', 30)
        self.leaderboard.rank_member('member_9', 30)
        self.leaderboard.rank_member('member_10', 10)

        leaders = self.leaderboard.leaders(1, page_size=3)
        leaders[0]['rank'].should.equal(1)
        leaders[1]['rank'].should.equal(1)
        leaders[2]['rank'].should.equal(1)

        leaders = self.leaderboard.leaders(2, page_size=3)
        leaders[0]['rank'].should.equal(1)
        leaders[1]['rank'].should.equal(2)
        leaders[2]['rank'].should.equal(2)

    def test_correct_rankings_for_around_me(self):
        self.leaderboard.rank_member('member_1', 50)
        self.leaderboard.rank_member('member_2', 50)
        self.leaderboard.rank_member('member_3', 30)
        self.leaderboard.rank_member('member_4', 30)
        self.leaderboard.rank_member('member_5', 10)
        self.leaderboard.rank_member('member_6', 50)
        self.leaderboard.rank_member('member_7', 50)
        self.leaderboard.rank_member('member_8', 30)
        self.leaderboard.rank_member('member_9', 30)
        self.leaderboard.rank_member('member_10', 10)

        leaders = self.leaderboard.around_me('member_3', page_size=3)
        leaders[0]['rank'].should.equal(2)
        leaders[1]['rank'].should.equal(2)
        leaders[2]['rank'].should.equal(3)

    def test_removing_a_single_member_will_also_remove_their_score_from_the_tie_scores_leaderboard_when_appropriate(self):
        self.leaderboard.rank_member('member_1', 50)
        self.leaderboard.rank_member('member_2', 50)
        self.leaderboard.rank_member('member_3', 30)

        self.leaderboard.remove_member('member_1')
        self.leaderboard.total_members_in('ties:ties').should.equal(2)
        self.leaderboard.remove_member('member_2')
        self.leaderboard.total_members_in('ties:ties').should.equal(1)
        self.leaderboard.remove_member('member_3')
        self.leaderboard.total_members_in('ties:ties').should.equal(0)

    def test_retrieve_the_rank_of_a_single_member_using_rank_for(self):
        self.leaderboard.rank_member('member_1', 50)
        self.leaderboard.rank_member('member_2', 50)
        self.leaderboard.rank_member('member_3', 30)

        self.leaderboard.rank_for('member_1').should.equal(1)
        self.leaderboard.rank_for('member_2').should.equal(1)
        self.leaderboard.rank_for('member_3').should.equal(2)

    def test_retrieve_the_score_and_rank_of_a_single_member_using_score_and_rank_for(self):
        self.leaderboard.rank_member('member_1', 50)
        self.leaderboard.rank_member('member_2', 50)
        self.leaderboard.rank_member('member_3', 30)

        self.leaderboard.score_and_rank_for('member_1')['rank'].should.equal(1)
        self.leaderboard.score_and_rank_for('member_2')['rank'].should.equal(1)
        self.leaderboard.score_and_rank_for('member_3')['rank'].should.equal(2)

    def test_remove_members_in_a_given_score_range_using_remove_members_in_score_range(self):
        self.__rank_members_in_leaderboard()

        self.leaderboard.total_members().should.equal(5)

        self.leaderboard.rank_member('cheater_1', 100)
        self.leaderboard.rank_member('cheater_2', 101)
        self.leaderboard.rank_member('cheater_3', 102)

        self.leaderboard.total_members().should.equal(8)
        self.leaderboard.total_members_in('ties:ties').should.equal(8)

        self.leaderboard.remove_members_in_score_range(100, 102)

        self.leaderboard.total_members().should.equal(5)
        self.leaderboard.total_members_in('ties:ties').should.equal(5)

    def test_expire_the_ties_leaderboard_in_a_given_number_of_seconds(self):
        self.__rank_members_in_leaderboard()

        self.leaderboard.expire_leaderboard(3)
        ttl = self.leaderboard.redis_connection.ttl('ties')
        ttl.should.be.greater_than(1)
        ttl = self.leaderboard.redis_connection.ttl('ties:ties')
        ttl.should.be.greater_than(1)
        ttl = self.leaderboard.redis_connection.ttl('ties:member_data')
        ttl.should.be.greater_than(1)

    def test_expire_the_ties_leaderboard_at_a_specific_timestamp(self):
        self.__rank_members_in_leaderboard()
        self.leaderboard.expire_leaderboard_at(int(time.time() + 10))
        ttl = self.leaderboard.redis_connection.ttl(
            self.leaderboard.leaderboard_name)
        ttl.should.be.lower_than(11)
        ttl = self.leaderboard.redis_connection.ttl(
            '%s:ties' %
            self.leaderboard.leaderboard_name)
        ttl.should.be.lower_than(11)
        ttl = self.leaderboard.redis_connection.ttl(
            '%s:member_data' %
            self.leaderboard.leaderboard_name)
        ttl.should.be.lower_than(11)

    def __rank_members_in_leaderboard(self, members_to_add=6):
        for index in range(1, members_to_add):
            self.leaderboard.rank_member(
                'member_%s' %
                index, index, {
                    'member_name': 'Leaderboard member %s' %
                    index})
