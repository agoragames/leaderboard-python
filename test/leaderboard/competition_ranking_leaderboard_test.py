from leaderboard.competition_ranking_leaderboard import CompetitionRankingLeaderboard
import unittest
import sure


class CompetitionRankingLeaderboardTest(unittest.TestCase):

    def setUp(self):
        self.leaderboard = CompetitionRankingLeaderboard('ties', decode_responses=True)

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
        leaders[1]['rank'].should.equal(1)
        leaders[2]['rank'].should.equal(3)
        leaders[3]['rank'].should.equal(3)
        leaders[4]['rank'].should.equal(5)

    def test_leaders_with_optional_member_data(self):
        self.leaderboard.rank_member('member_1', 50)
        self.leaderboard.rank_member('member_2', 50)
        self.leaderboard.rank_member('member_3', 30)
        self.leaderboard.rank_member('member_4', 30)
        self.leaderboard.rank_member('member_5', 10, 'member_5')

        leaders = self.leaderboard.leaders(1, with_member_data=True)
        leaders[0]['rank'].should.equal(1)
        leaders[0]['member_data'].should.equal(None)
        leaders[1]['rank'].should.equal(1)
        leaders[1]['member_data'].should.equal(None)
        leaders[2]['rank'].should.equal(3)
        leaders[2]['member_data'].should.equal(None)
        leaders[3]['rank'].should.equal(3)
        leaders[3]['member_data'].should.equal(None)
        leaders[4]['rank'].should.equal(5)
        leaders[4]['member_data'].should.equal('member_5')

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
        leaders[1]['rank'].should.equal(5)
        leaders[2]['rank'].should.equal(5)

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
        leaders[4]['rank'].should.equal(5)
        leaders[9]['rank'].should.equal(9)

    def test_retrieve_the_rank_of_a_single_member_using_rank_for(self):
        self.leaderboard.rank_member('member_1', 50)
        self.leaderboard.rank_member('member_2', 50)
        self.leaderboard.rank_member('member_3', 30)

        self.leaderboard.rank_for('member_1').should.equal(1)
        self.leaderboard.rank_for('member_2').should.equal(1)
        self.leaderboard.rank_for('member_3').should.equal(3)

    def test_retrieve_the_score_and_rank_for_a_single_member_using_score_and_rank_for(self):
        self.leaderboard.rank_member('member_1', 50)
        self.leaderboard.rank_member('member_2', 50)
        self.leaderboard.rank_member('member_3', 30)

        self.leaderboard.score_and_rank_for('member_1')['rank'].should.equal(1)
        self.leaderboard.score_and_rank_for('member_2')['rank'].should.equal(1)
        self.leaderboard.score_and_rank_for('member_3')['rank'].should.equal(3)

    def test_correct_rankings_and_scores_when_using_change_score_for(self):
        self.leaderboard.rank_member('member_1', 50)
        self.leaderboard.rank_member('member_2', 50)
        self.leaderboard.rank_member('member_3', 30)
        self.leaderboard.rank_member('member_4', 30)
        self.leaderboard.rank_member('member_5', 10)
        self.leaderboard.change_score_for('member_3', 10)

        self.leaderboard.rank_for('member_3').should.equal(3)
        self.leaderboard.rank_for('member_4').should.equal(4)
        self.leaderboard.score_for('member_3').should.equal(40.0)

    def test_retrieve_a_given_set_of_members_from_the_leaderboard_in_a_range_from_1_to_the_number_given(self):
        self.__rank_members_in_leaderboard(26)

        members = self.leaderboard.top(5)
        len(members).should.equal(5)
        members[0]['member'].should.equal('member_25')
        members[0]['score'].should.equal(25.0)
        members[4]['member'].should.equal('member_21')

        members = self.leaderboard.top(1)
        len(members).should.equal(1)
        members[0]['member'].should.equal('member_25')

        members = self.leaderboard.top(26)
        len(members).should.equal(25)
        members[0]['member'].should.equal('member_25')
        members[0]['score'].should.equal(25.0)
        members[24]['member'].should.equal('member_1')

    def test_allow_you_to_include_or_exclude_missing_members_using_the_include_missing_option(self):
        self.__rank_members_in_leaderboard(26)

        leaders = self.leaderboard.ranked_in_list(
            ['member_1', 'member_15', 'member_25', 'member_200'])
        len(leaders).should.equal(4)
        leaders[0]['member'].should.equal('member_1')
        leaders[1]['member'].should.equal('member_15')
        leaders[2]['member'].should.equal('member_25')
        leaders[3]['member'].should.equal('member_200')

        leaders = self.leaderboard.ranked_in_list(
            ['member_1', 'member_15', 'member_25', 'member_200'], include_missing=False)
        len(leaders).should.equal(3)
        leaders[0]['member'].should.equal('member_1')
        leaders[1]['member'].should.equal('member_15')
        leaders[2]['member'].should.equal('member_25')

    def __rank_members_in_leaderboard(self, members_to_add=6):
        for index in range(1, members_to_add):
            self.leaderboard.rank_member(
                'member_%s' % index,
                index,
                str({'member_name': 'Leaderboard member %s' % index})
            )
