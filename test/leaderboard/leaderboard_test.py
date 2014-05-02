from redis import Redis, StrictRedis, ConnectionPool
from leaderboard.leaderboard import Leaderboard
import unittest
import time
import sure


class LeaderboardTest(unittest.TestCase):

    def setUp(self):
        self.leaderboard = Leaderboard('name')

    def tearDown(self):
        self.leaderboard.redis_connection.flushdb()
        Leaderboard.MEMBER_KEY = 'member'
        Leaderboard.SCORE_KEY = 'score'
        Leaderboard.RANK_KEY = 'rank'
        Leaderboard.MEMBER_DATA_KEY = 'member_data'

    def test_version(self):
        Leaderboard.VERSION.should.equal('2.8.0')

    def test_init_with_defaults(self):
        'name'.should.equal(self.leaderboard.leaderboard_name)
        len(self.leaderboard.options).should.be(1)
        self.leaderboard.options['connection_pool'].should.be.a(ConnectionPool)
        self.leaderboard.redis_connection.should.be.a(Redis)
        self.leaderboard.DEFAULT_PAGE_SIZE.should.equal(
            self.leaderboard.page_size)

    def test_init_sets_page_size_to_default_if_set_to_invalid_value(self):
        self.leaderboard = Leaderboard('name', page_size=0)
        self.leaderboard.page_size.should.equal(Leaderboard.DEFAULT_PAGE_SIZE)

    def test_init_uses_connection_pooling(self):
        lb0 = Leaderboard('lb0', db=0)
        lb1 = Leaderboard('lb1', db=0)
        lb2 = Leaderboard('lb2', db=1)

        lb0.redis_connection.connection_pool.should.equal(
            lb1.redis_connection.connection_pool)
        lb0.redis_connection.connection_pool.should_not.equal(
            lb2.redis_connection.connection_pool)

    def test_init_uses_connection(self):
        lb = Leaderboard('lb0', connection=Redis(db=1))
        lb.redis_connection.connection_pool.connection_kwargs[
            'db'].should.equal(1)
        lb = Leaderboard('lb1', connection=StrictRedis(db=1))
        lb.redis_connection.connection_pool.connection_kwargs[
            'db'].should.equal(1)

    def test_delete_leaderboard(self):
        self.__rank_members_in_leaderboard()
        self.leaderboard.redis_connection.exists('name').should.be.true
        self.leaderboard.delete_leaderboard()
        self.leaderboard.redis_connection.exists('name').should.be.false

    def test_member_data_for(self):
        self.__rank_members_in_leaderboard()
        self.leaderboard.member_data_for('member_1').should.eql(
            str({'member_name': 'Leaderboard member 1'}))

    def test_update_member_data(self):
        self.__rank_members_in_leaderboard()
        self.leaderboard.update_member_data(
            'member_1', {
                'member_name': 'Updated Leaderboard member 1'})
        self.leaderboard.member_data_for('member_1').should.eql(
            str({'member_name': 'Updated Leaderboard member 1'}))

    def test_remove_member_data(self):
        self.__rank_members_in_leaderboard()
        self.leaderboard.remove_member_data('member_1')
        self.leaderboard.member_data_for('member_1').should.be(None)

    def test_total_members(self):
        self.__rank_members_in_leaderboard()
        self.leaderboard.total_members().should.equal(5)

    def test_remove_member(self):
        self.__rank_members_in_leaderboard()
        self.leaderboard.total_members().should.equal(5)
        self.leaderboard.remove_member('member_1')
        self.leaderboard.total_members().should.equal(4)

    def test_remove_member_also_removes_member_data(self):
        self.__rank_members_in_leaderboard()
        self.leaderboard.redis_connection.exists(
            "name:member_data").should.be.true
        len(self.leaderboard.redis_connection.hgetall(
            "name:member_data")).should.equal(5)
        self.leaderboard.total_members().should.equal(5)
        self.leaderboard.remove_member('member_1')
        self.leaderboard.redis_connection.exists(
            "name:member_data").should.be.true
        len(self.leaderboard.redis_connection.hgetall(
            "name:member_data")).should.equal(4)
        self.leaderboard.total_members().should.equal(4)

    def test_total_pages(self):
        self.__rank_members_in_leaderboard(27)
        self.leaderboard.total_members().should.equal(26)
        self.leaderboard.total_pages().should.equal(2)

    def test_total_members_in_score_range(self):
        self.__rank_members_in_leaderboard()
        self.leaderboard.total_members_in_score_range(2, 4).should.equal(3)

    def test_score_for(self):
        self.__rank_members_in_leaderboard()
        self.leaderboard.score_for('member_5').should.equal(5.0)
        self.leaderboard.score_for('jones').should.be(None)

    def test_check_member(self):
        self.__rank_members_in_leaderboard()
        self.leaderboard.check_member('member_3').should.be.true
        self.leaderboard.check_member('member_6').should.be.false

    def test_rank_for(self):
        self.__rank_members_in_leaderboard()
        self.leaderboard.rank_for('member_5').should.equal(1)

    def test_change_score_for(self):
        self.__rank_members_in_leaderboard()
        self.leaderboard.change_score_for('member_1', 99)
        self.leaderboard.rank_for('member_1').should.equal(1)
        self.leaderboard.score_for('member_1').should.equal(100.0)

    def test_score_and_rank_for(self):
        self.__rank_members_in_leaderboard()
        score_and_rank = self.leaderboard.score_and_rank_for('member_3')
        score_and_rank['member'].should.equal('member_3')
        score_and_rank['score'].should.equal(3.0)
        score_and_rank['rank'].should.equal(3)

        score_and_rank = self.leaderboard.score_and_rank_for('jones')
        score_and_rank['member'].should.equal('jones')
        score_and_rank['score'].should.be(None)
        score_and_rank['rank'].should.be(None)

    def test_remove_members_in_score_range(self):
        self.__rank_members_in_leaderboard()
        self.leaderboard.total_members().should.equal(5)
        self.leaderboard.remove_members_in_score_range(2, 4)
        self.leaderboard.total_members().should.equal(2)

    def test_remove_members_outside_rank(self):
        self.__rank_members_in_leaderboard()

        self.leaderboard.total_members().should.equal(5)
        self.leaderboard.remove_members_outside_rank(3).should.equal(2)

        leaders = self.leaderboard.leaders(1)
        len(leaders).should.equal(3)
        leaders[0]['member'].should.equal('member_5')
        leaders[2]['member'].should.equal('member_3')

        self.leaderboard.order = Leaderboard.ASC
        self.__rank_members_in_leaderboard()

        self.leaderboard.total_members().should.equal(5)
        self.leaderboard.remove_members_outside_rank(3).should.equal(2)

        leaders = self.leaderboard.leaders(1)
        len(leaders).should.equal(3)
        leaders[0]['member'].should.equal('member_1')
        leaders[2]['member'].should.equal('member_3')

    def test_page_for(self):
        self.leaderboard.page_for('jones').should.equal(0)

        self.__rank_members_in_leaderboard(21)

        self.leaderboard.page_for('member_17').should.equal(1)
        self.leaderboard.page_for('member_11').should.equal(1)
        self.leaderboard.page_for('member_10').should.equal(1)
        self.leaderboard.page_for('member_1').should.equal(1)

        self.leaderboard.page_for('member_17', 10).should.equal(1)
        self.leaderboard.page_for('member_11', 10).should.equal(1)
        self.leaderboard.page_for('member_10', 10).should.equal(2)
        self.leaderboard.page_for('member_1', 10).should.equal(2)

    def test_page_for_with_sort_option_ASC(self):
        self.leaderboard.order = Leaderboard.ASC
        self.leaderboard.page_for('jones').should.equal(0)

        self.__rank_members_in_leaderboard(21)

        self.leaderboard.page_for('member_10', 10).should.equal(1)
        self.leaderboard.page_for('member_1', 10).should.equal(1)
        self.leaderboard.page_for('member_17', 10).should.equal(2)
        self.leaderboard.page_for('member_11', 10).should.equal(2)

    def test_percentile_for(self):
        self.__rank_members_in_leaderboard(13)

        self.leaderboard.percentile_for('member_1').should.eql(0)
        self.leaderboard.percentile_for('member_2').should.eql(9)
        self.leaderboard.percentile_for('member_3').should.eql(17)
        self.leaderboard.percentile_for('member_4').should.eql(25)
        self.leaderboard.percentile_for('member_12').should.eql(92)

    def test_score_for_percentile(self):
        self.__rank_members_in_leaderboard(6)

        self.leaderboard.score_for_percentile(0).should.eql(1.0)
        self.leaderboard.score_for_percentile(75).should.eql(4.0)
        self.leaderboard.score_for_percentile(87.5).should.eql(4.5)
        self.leaderboard.score_for_percentile(93.75).should.eql(4.75)
        self.leaderboard.score_for_percentile(100).should.eql(5.0)

    def test_score_for_percentile_with_sort_option_ASC(self):
        self.leaderboard.order = Leaderboard.ASC
        self.__rank_members_in_leaderboard(6)

        self.leaderboard.score_for_percentile(0).should.eql(5.0)
        self.leaderboard.score_for_percentile(75).should.eql(2.0)
        self.leaderboard.score_for_percentile(87.5).should.eql(1.5)
        self.leaderboard.score_for_percentile(93.75).should.eql(1.25)
        self.leaderboard.score_for_percentile(100).should.eql(1.0)

    def test_expire_leaderboard(self):
        self.__rank_members_in_leaderboard()
        self.leaderboard.expire_leaderboard(3)
        ttl = self.leaderboard.redis_connection.ttl(
            self.leaderboard.leaderboard_name)
        ttl.should.be.greater_than(1)
        ttl = self.leaderboard.redis_connection.ttl(
            '%s:member_data' %
            self.leaderboard.leaderboard_name)
        ttl.should.be.greater_than(1)

    def test_expire_leaderboard_at(self):
        self.__rank_members_in_leaderboard()
        self.leaderboard.expire_leaderboard_at(int(time.time() + 10))
        ttl = self.leaderboard.redis_connection.ttl(
            self.leaderboard.leaderboard_name)
        ttl.should.be.lower_than(11)
        ttl = self.leaderboard.redis_connection.ttl(
            '%s:member_data' %
            self.leaderboard.leaderboard_name)
        ttl.should.be.lower_than(11)

    def test_leaders(self):
        self.__rank_members_in_leaderboard(27)
        leaders = self.leaderboard.leaders(1)
        len(leaders).should.equal(25)
        leaders[0]['member'].should.equal('member_26')
        leaders[0]['rank'].should.equal(1)
        leaders[24]['member'].should.equal('member_2')

        leaders = self.leaderboard.leaders(2)
        len(leaders).should.equal(1)
        leaders[0]['member'].should.equal('member_1')
        leaders[0]['rank'].should.equal(26)

        leaders = self.leaderboard.leaders(1, page_size=5)
        len(leaders).should.equal(5)

    def test_leaders_with_optional_member_data(self):
        self.__rank_members_in_leaderboard()
        leaders = self.leaderboard.leaders(1, with_member_data=True)
        len(leaders).should.equal(5)
        leaders[0]['member'].should.equal('member_5')
        leaders[0]['member_data'].should.be(
            str({'member_name': 'Leaderboard member 5'}))

    def test_leaders_return_type(self):
        leaders = self.leaderboard.leaders(1)
        type(leaders).should.equal(type([]))
        leaders.should.equal([])

    def test_ranked_in_list_with_sort_by(self):
        self.__rank_members_in_leaderboard(26)
        leaders = self.leaderboard.ranked_in_list(
            ['member_25', 'member_1', 'member_15'], sort_by='score')
        len(leaders).should.equal(3)
        leaders[0]['member'].should.equal('member_1')
        leaders[1]['member'].should.equal('member_15')
        leaders[2]['member'].should.equal('member_25')

        leaders = self.leaderboard.ranked_in_list(
            ['member_25', 'member_1', 'member_15'], sort_by='rank')
        len(leaders).should.be(3)
        leaders[0]['member'].should.equal('member_25')
        leaders[1]['member'].should.equal('member_15')
        leaders[2]['member'].should.equal('member_1')

    def test_ranked_in_list(self):
        self.__rank_members_in_leaderboard(27)
        leaders = self.leaderboard.ranked_in_list(
            ['member_1', 'member_15', 'member_25'])
        len(leaders).should.be(3)
        leaders[0]['member'].should.equal('member_1')
        leaders[1]['member'].should.equal('member_15')
        leaders[2]['member'].should.equal('member_25')

    def test_ranked_in_list_with_unknown_member(self):
        self.__rank_members_in_leaderboard(27)
        leaders = self.leaderboard.ranked_in_list(['jones'])
        len(leaders).should.be(1)
        leaders[0]['member'].should.equal('jones')
        leaders[0]['score'].should.be(None)
        leaders[0]['rank'].should.be(None)

    def test_all_leaders(self):
        self.__rank_members_in_leaderboard(26)
        leaders = self.leaderboard.all_leaders()
        len(leaders).should.be(25)
        leaders[0]['member'].should.equal('member_25')

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
        members[0]['score'].should.equal(21.0)
        members[4]['member'].should.eql('member_17')

        members = self.leaderboard.members_from_rank_range(1, 1)
        len(members).should.equal(1)
        members[0]['member'].should.eql('member_25')

        members = self.leaderboard.members_from_rank_range(1, 26)
        len(members).should.equal(25)
        members[0]['member'].should.eql('member_25')
        members[0]['score'].should.equal(25)
        members[24]['member'].should.eql('member_1')

    def test_member_at(self):
        self.__rank_members_in_leaderboard(51)
        self.leaderboard.member_at(1)['rank'].should.equal(1)
        self.leaderboard.member_at(1)['score'].should.equal(50.0)
        self.leaderboard.member_at(26)['rank'].should.equal(26)
        self.leaderboard.member_at(50)['rank'].should.equal(50)
        self.leaderboard.member_at(51).should.equal(None)
        self.leaderboard.member_at(1, with_member_data=True)['member_data'].should.eql(
            str({'member_name': 'Leaderboard member 50'}))

    def test_around_me(self):
        self.__rank_members_in_leaderboard(
            Leaderboard.DEFAULT_PAGE_SIZE *
            3 +
            2)

        self.leaderboard.total_members().should.be(
            Leaderboard.DEFAULT_PAGE_SIZE *
            3 +
            1)

        leaders_around_me = self.leaderboard.around_me('member_30')
        (len(leaders_around_me) /
         2).should.equal(self.leaderboard.page_size /
                         2)

        leaders_around_me = self.leaderboard.around_me('member_1')
        len(leaders_around_me).should.equal(self.leaderboard.page_size / 2 + 1)

        leaders_around_me = self.leaderboard.around_me('member_76')
        (len(leaders_around_me) /
         2).should.equal(self.leaderboard.page_size /
                         2)

    def test_members_only(self):
        exp = [{'member': 'member_%d' % x} for x in reversed(range(1, 27))]

        self.__rank_members_in_leaderboard(27)
        leaders = self.leaderboard.leaders(1, members_only=True)
        leaders.should.equal(exp[0:25])

        leaders = self.leaderboard.leaders(2, members_only=True)
        leaders.should.equal(exp[25:26])

        members = self.leaderboard.all_leaders(members_only=True)
        members.should.equal(exp)

        members = self.leaderboard.members_from_score_range(
            10,
            15,
            members_only=True)
        members.should.equal(exp[11:17])

        members = self.leaderboard.members_from_rank_range(
            5,
            9,
            members_only=True)
        members.should.equal(exp[4:9])

        leaders_around_me = self.leaderboard.around_me(
            'member_25',
            page_size=3,
            members_only=True)
        leaders_around_me.should.equal(exp[0:3])

    def test_merge_leaderboards(self):
        foo_leaderboard = Leaderboard('foo')
        bar_leaderboard = Leaderboard('bar')

        foo_leaderboard.rank_member('foo_1', 1)
        foo_leaderboard.rank_member('foo_2', 2)
        bar_leaderboard.rank_member('bar_1', 1)
        bar_leaderboard.rank_member('bar_2', 2)
        bar_leaderboard.rank_member('bar_3', 5)

        foo_leaderboard.merge_leaderboards('foobar', ['bar'], aggregate='SUM')

        foobar_leaderboard = Leaderboard('foobar')
        foobar_leaderboard.total_members().should.equal(5)

        foobar_leaderboard.leaders(1)[0]['member'].should.equal('bar_3')

    def test_intersect_leaderboards(self):
        foo_leaderboard = Leaderboard('foo')
        bar_leaderboard = Leaderboard('bar')

        foo_leaderboard.rank_member('foo_1', 1)
        foo_leaderboard.rank_member('foo_2', 2)
        foo_leaderboard.rank_member('bar_3', 6)
        bar_leaderboard.rank_member('bar_1', 3)
        bar_leaderboard.rank_member('foo_1', 4)
        bar_leaderboard.rank_member('bar_3', 5)

        foo_leaderboard.intersect_leaderboards(
            'foobar',
            ['bar'],
            aggregate='SUM')

        foobar_leaderboard = Leaderboard('foobar')
        foobar_leaderboard.total_members().should.equal(2)

        foobar_leaderboard.leaders(1)[0]['member'].should.equal('bar_3')

    def test_rank_member_if(self):
        def highscore_check(
                self,
                member,
                current_score,
                score,
                member_data,
                leaderboard_options):
            if (current_score is None):
                return True
            if (score > current_score):
                return True
            return False

        self.leaderboard.total_members().should.equal(0)
        self.leaderboard.rank_member_if(highscore_check, 'david', 1337)
        self.leaderboard.total_members().should.equal(1)
        self.leaderboard.score_for('david').should.equal(1337.0)
        self.leaderboard.rank_member_if(highscore_check, 'david', 1336)
        self.leaderboard.score_for('david').should.equal(1337.0)
        self.leaderboard.rank_member_if(highscore_check, 'david', 1338)
        self.leaderboard.score_for('david').should.equal(1338.0)

    def test_rank_members(self):
        self.leaderboard.total_members().should.equal(0)
        self.leaderboard.rank_members(['member_1', 1000, 'member_2', 3000])
        self.leaderboard.total_members().should.equal(2)

    def test_rank_member_across(self):
        self.leaderboard.rank_member_across(
            ['highscores', 'more_highscores'], 'david', 50000, {'member_name': 'david'})
        len(self.leaderboard.leaders_in('highscores', 1)).should.equal(1)
        len(self.leaderboard.leaders_in('more_highscores', 1)).should.equal(1)

    def test_custom_keys_for_member_score_rank_and_member_data(self):
        Leaderboard.MEMBER_KEY = 'member_custom'
        Leaderboard.SCORE_KEY = 'score_custom'
        Leaderboard.RANK_KEY = 'rank_custom'
        Leaderboard.MEMBER_DATA_KEY = 'member_data_custom'

        self.__rank_members_in_leaderboard(26)
        leaders = self.leaderboard.leaders(1, with_member_data=True)
        len(leaders).should.equal(25)
        leaders[0]['member_custom'].should.equal('member_25')
        leaders[0]['score_custom'].should.equal(25)
        leaders[0]['rank_custom'].should.equal(1)
        leaders[0]['member_data_custom'].should.equal(
            "{'member_name': 'Leaderboard member 25'}")

    def test_can_use_StrictRedis_class_for_connection(self):
        lb = Leaderboard('lb1', connection=StrictRedis(db=0))
        lb.rank_member('david', 50.1)
        lb.score_for('david').should.equal(50.1)
        lb.rank_for('david').should.equal(1)
        len(lb.leaders(1)).should.equal(1)

    def test_can_set_member_data_namespace_option(self):
        self.leaderboard = Leaderboard('name', member_data_namespace='md')
        self.__rank_members_in_leaderboard()

        self.leaderboard.redis_connection.exists(
            "name:member_data").should.be.false
        self.leaderboard.redis_connection.exists("name:md").should.be.true

    def __rank_members_in_leaderboard(self, members_to_add=6):
        for index in range(1, members_to_add):
            self.leaderboard.rank_member(
                'member_%s' %
                index, index, {
                    'member_name': 'Leaderboard member %s' %
                    index})
