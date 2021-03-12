from leaderboard.tie_ranking_leaderboard import TieRankingLeaderboard
import unittest
import time
import sure


class TieRankingLeaderboardTest(unittest.TestCase):

    def setUp(self):
        self.leaderboard = TieRankingLeaderboard('ties', decode_responses=True)

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

    def test_correct_rankings_and_scores_when_using_change_score_for(self):
        self.leaderboard.rank_member('member_1', 50)
        self.leaderboard.rank_member('member_2', 50)
        self.leaderboard.rank_member('member_3', 30)
        self.leaderboard.rank_member('member_4', 30)
        self.leaderboard.rank_member('member_5', 10)
        self.leaderboard.change_score_for('member_3', 10)

        self.leaderboard.rank_for('member_3').should.equal(2)
        self.leaderboard.rank_for('member_4').should.equal(3)
        self.leaderboard.score_for('member_3').should.equal(40.0)

    def test_correct_rankings_and_scores_when_using_change_score_for_with_varying_scores(self):
        self.leaderboard.rank_member('member_1', 5)
        self.leaderboard.rank_member('member_2', 4)
        self.leaderboard.rank_member('member_3', 3)
        self.leaderboard.rank_member('member_4', 2)
        self.leaderboard.rank_member('member_5', 1)
        self.leaderboard.change_score_for('member_3', 0.5)

        self.leaderboard.rank_for('member_3').should.equal(3)
        self.leaderboard.rank_for('member_4').should.equal(4)
        self.leaderboard.score_for('member_3').should.equal(3.5)

    def test_change_score_and_member_data_for_a_member(self):
        self.leaderboard.change_score_for('member_1', 10, 'optional-data')
        self.leaderboard.rank_for('member_1').should.equal(1)
        self.leaderboard.member_data_for('member_1').should.equal('optional-data')

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

        leaders = self.leaderboard.ranked_in_list(
            ['member_200'], include_missing=False, with_member_data=True)
        len(leaders).should.be(0)

    def test_it_should_output_the_correct_rank_when_initial_score_is_0_and_then_later_scores_are_ties(self):
        self.leaderboard.rank_members(['member_1', 0, 'member_2', 0])
        self.leaderboard.rank_for('member_1').should.equal(1)
        self.leaderboard.rank_for('member_2').should.equal(1)
        self.leaderboard.rank_members(['member_1', 0, 'member_2', 0])
        self.leaderboard.rank_for('member_1').should.equal(1)
        self.leaderboard.rank_for('member_2').should.equal(1)
        self.leaderboard.rank_members(['member_1', 1, 'member_2', 1])
        self.leaderboard.rank_for('member_1').should.equal(1)
        self.leaderboard.rank_for('member_2').should.equal(1)
        self.leaderboard.rank_members(['member_1', 1, 'member_2', 1, 'member_3', 4])
        self.leaderboard.rank_for('member_3').should.equal(1)
        self.leaderboard.rank_for('member_1').should.equal(2)
        self.leaderboard.rank_for('member_2').should.equal(2)

    def test_rank_member_across(self):
        self.leaderboard.rank_member_across(
            ['highscores', 'more_highscores'], 'david', 50000, {'member_name': 'david'})
        len(self.leaderboard.leaders_in('highscores', 1)).should.equal(1)
        len(self.leaderboard.leaders_in('more_highscores', 1)).should.equal(1)

    def test_it_should_correctly_pop_ties_namespace_from_options(self):
        self.leaderboard = TieRankingLeaderboard('ties', ties_namespace='ties_namespace')
        self.__rank_members_in_leaderboard(26)

    def __rank_members_in_leaderboard(self, members_to_add=6):
        for index in range(1, members_to_add):
            self.leaderboard.rank_member(
                'member_%s' % index,
                index,
                str({'member_name': 'Leaderboard member %s' % index})
            )
