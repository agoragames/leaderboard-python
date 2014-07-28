from redis import Redis, StrictRedis, ConnectionPool
from leaderboard.leaderboard import Leaderboard
from leaderboard.competition_ranking_leaderboard import CompetitionRankingLeaderboard
import unittest
import time
import sure


class ReverseCompetitionRankingLeaderboardTest(unittest.TestCase):

    def setUp(self):
        self.leaderboard = CompetitionRankingLeaderboard(
            'ties', order=Leaderboard.ASC)

    def tearDown(self):
        self.leaderboard.redis_connection.flushdb()

    def test_leaders(self):
        self.leaderboard.rank_member('member_1', 50)
        self.leaderboard.rank_member('member_2', 50)
        self.leaderboard.rank_member('member_3', 30)
        self.leaderboard.rank_member('member_4', 30)
        self.leaderboard.rank_member('member_5', 10)

        leaders = self.leaderboard.leaders(1)
        leaders[0]['rank'].should.equal(1)
        leaders[1]['rank'].should.equal(2)
        leaders[2]['rank'].should.equal(2)
        leaders[3]['rank'].should.equal(4)
        leaders[4]['rank'].should.equal(4)

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
        leaders[2]['rank'].should.equal(3)

        leaders = self.leaderboard.leaders(2, page_size=3)
        leaders[0]['rank'].should.equal(3)
        leaders[1]['rank'].should.equal(3)
        leaders[2]['rank'].should.equal(3)

        leaders = self.leaderboard.leaders(3, page_size=3)
        leaders[0]['rank'].should.equal(7)
        leaders[1]['rank'].should.equal(7)
        leaders[2]['rank'].should.equal(7)

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

        leaders = self.leaderboard.around_me('member_4')
        leaders[0]['rank'].should.equal(1)
        leaders[4]['rank'].should.equal(3)
        leaders[9]['rank'].should.equal(7)

    def test_retrieve_the_rank_of_a_single_member_using_rank_for(self):
        self.leaderboard.rank_member('member_1', 50)
        self.leaderboard.rank_member('member_2', 50)
        self.leaderboard.rank_member('member_3', 30)

        self.leaderboard.rank_for('member_3').should.equal(1)
        self.leaderboard.rank_for('member_1').should.equal(2)
        self.leaderboard.rank_for('member_2').should.equal(2)

    def test_retrieve_the_score_and_rank_for_a_single_member_using_score_and_rank_for(self):
        self.leaderboard.rank_member('member_1', 50)
        self.leaderboard.rank_member('member_2', 50)
        self.leaderboard.rank_member('member_3', 30)

        self.leaderboard.score_and_rank_for('member_3')['rank'].should.equal(1)
        self.leaderboard.score_and_rank_for('member_1')['rank'].should.equal(2)
        self.leaderboard.score_and_rank_for('member_2')['rank'].should.equal(2)
