from .leaderboard import Leaderboard
from .leaderboard import grouper
from redis import StrictRedis, Redis, ConnectionPool
import math


class TieRankingLeaderboard(Leaderboard):
    DEFAULT_TIES_NAMESPACE = 'ties'

    def __init__(self, leaderboard_name, **options):
        '''
        Initialize a connection to a specific leaderboard. By default, will use a
        redis connection pool for any unique host:port:db pairing.

        The options and their default values (if any) are:

        host : the host to connect to if creating a new handle ('localhost')
        port : the port to connect to if creating a new handle (6379)
        db : the redis database to connect to if creating a new handle (0)
        page_size : the default number of items to return in each page (25)
        connection : an existing redis handle if re-using for this leaderboard
        connection_pool : redis connection pool to use if creating a new handle
        '''
        super(TieRankingLeaderboard, self).__init__(
            leaderboard_name, **options)

        self.leaderboard_name = leaderboard_name
        self.options = options

        self.ties_namespace = self.options.pop(
            'ties_namespace',
            self.DEFAULT_TIES_NAMESPACE)

    def delete_leaderboard_named(self, leaderboard_name):
        '''
        Delete the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        '''
        pipeline = self.redis_connection.pipeline()
        pipeline.delete(leaderboard_name)
        pipeline.delete(self._member_data_key(leaderboard_name))
        pipeline.delete(self._ties_leaderboard_key(leaderboard_name))
        pipeline.execute()

    def change_score_for_member_in(self, leaderboard_name, member, delta, member_data=None):
        '''
        Change the score for a member in the named leaderboard by a delta which can be positive or negative.

        @param leaderboard_name [String] Name of the leaderboard.
        @param member [String] Member name.
        @param delta [float] Score change.
        @param member_data [String] Optional member data.
        '''
        previous_score = self.score_for(member)
        new_score = (previous_score or 0) + delta

        total_members_at_previous_score = []
        if previous_score is not None:
            total_members_at_previous_score = self.redis_connection.zrevrangebyscore(leaderboard_name, previous_score, previous_score)

        pipeline = self.redis_connection.pipeline()
        if isinstance(self.redis_connection, Redis):
            pipeline.zadd(leaderboard_name, member, new_score)
            pipeline.zadd(self._ties_leaderboard_key(leaderboard_name), str(float(new_score)), new_score)
        else:
            pipeline.zadd(leaderboard_name, new_score, member)
            pipeline.zadd(self._ties_leaderboard_key(leaderboard_name), new_score, str(float(new_score)))
        if member_data:
            pipeline.hset(
                self._member_data_key(leaderboard_name),
                member,
                member_data)
        pipeline.execute()

        if len(total_members_at_previous_score) == 1:
            self.redis_connection.zrem(self._ties_leaderboard_key(leaderboard_name), str(float(previous_score)))

    def rank_member_in(
            self, leaderboard_name, member, score, member_data=None):
        '''
        Rank a member in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param member [String] Member name.
        @param score [float] Member score.
        @param member_data [String] Optional member data.
        '''
        member_score = None or self.redis_connection.zscore(leaderboard_name, member)
        can_delete_score = member_score is not None and\
            (len(self.members_from_score_range_in(leaderboard_name, member_score, member_score)) == 1) and\
            member_score != score

        pipeline = self.redis_connection.pipeline()
        if isinstance(self.redis_connection, Redis):
            pipeline.zadd(leaderboard_name, member, score)
            pipeline.zadd(self._ties_leaderboard_key(leaderboard_name),
                          str(float(score)), score)
        else:
            pipeline.zadd(leaderboard_name, score, member)
            pipeline.zadd(self._ties_leaderboard_key(leaderboard_name),
                          score, str(float(score)))
        if can_delete_score:
            pipeline.zrem(self._ties_leaderboard_key(leaderboard_name),
                          str(float(member_score)))
        if member_data:
            pipeline.hset(
                self._member_data_key(leaderboard_name),
                member,
                member_data)
        pipeline.execute()

    def rank_member_across(
            self, leaderboards, member, score, member_data=None):
        '''
        Rank a member across multiple leaderboards.

        @param leaderboards [Array] Leaderboard names.
        @param member [String] Member name.
        @param score [float] Member score.
        @param member_data [String] Optional member data.
        '''
        for leaderboard_name in leaderboards:
            self.rank_member_in(leaderboard, member, score, member_data)

    def rank_members_in(self, leaderboard_name, members_and_scores):
        '''
        Rank an array of members in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param members_and_scores [Array] Variable list of members and scores.
        '''
        for member, score in grouper(2, members_and_scores):
            self.rank_member_in(leaderboard_name, member, score)

    def remove_member_from(self, leaderboard_name, member):
        '''
        Remove the optional member data for a given member in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param member [String] Member name.
        '''
        member_score = None or self.redis_connection.zscore(
            leaderboard_name, member)
        can_delete_score = member_score and len(
            self.members_from_score_range_in(leaderboard_name, member_score, member_score)) == 1

        pipeline = self.redis_connection.pipeline()
        pipeline.zrem(leaderboard_name, member)
        if can_delete_score:
            pipeline.zrem(self._ties_leaderboard_key(leaderboard_name),
                          str(float(member_score)))
        pipeline.hdel(self._member_data_key(leaderboard_name), member)
        pipeline.execute()

    def rank_for_in(self, leaderboard_name, member):
        '''
        Retrieve the rank for a member in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param member [String] Member name.
        @return the rank for a member in the leaderboard.
        '''
        member_score = self.score_for_in(leaderboard_name, member)
        if self.order == self.ASC:
            try:
                return self.redis_connection.zrank(
                    self._ties_leaderboard_key(leaderboard_name), str(float(member_score))) + 1
            except:
                return None
        else:
            try:
                return self.redis_connection.zrevrank(
                    self._ties_leaderboard_key(leaderboard_name), str(float(member_score))) + 1
            except:
                return None

    def remove_members_in_score_range_in(
            self, leaderboard_name, min_score, max_score):
        '''
        Remove members from the named leaderboard in a given score range.

        @param leaderboard_name [String] Name of the leaderboard.
        @param min_score [float] Minimum score.
        @param max_score [float] Maximum score.
        '''
        pipeline = self.redis_connection.pipeline()
        pipeline.zremrangebyscore(
            leaderboard_name,
            min_score,
            max_score)
        pipeline.zremrangebyscore(
            self._ties_leaderboard_key(leaderboard_name),
            min_score,
            max_score)
        pipeline.execute()

    def expire_leaderboard_for(self, leaderboard_name, seconds):
        '''
        Expire the given leaderboard in a set number of seconds. Do not use this with
        leaderboards that utilize member data as there is no facility to cascade the
        expiration out to the keys for the member data.

        @param leaderboard_name [String] Name of the leaderboard.
        @param seconds [int] Number of seconds after which the leaderboard will be expired.
        '''
        pipeline = self.redis_connection.pipeline()
        pipeline.expire(leaderboard_name, seconds)
        pipeline.expire(self._ties_leaderboard_key(leaderboard_name), seconds)
        pipeline.expire(self._member_data_key(leaderboard_name), seconds)
        pipeline.execute()

    def expire_leaderboard_at_for(self, leaderboard_name, timestamp):
        '''
        Expire the given leaderboard at a specific UNIX timestamp. Do not use this with
        leaderboards that utilize member data as there is no facility to cascade the
        expiration out to the keys for the member data.

        @param leaderboard_name [String] Name of the leaderboard.
        @param timestamp [int] UNIX timestamp at which the leaderboard will be expired.
        '''
        pipeline = self.redis_connection.pipeline()
        pipeline.expireat(leaderboard_name, timestamp)
        pipeline.expireat(
            self._ties_leaderboard_key(leaderboard_name), timestamp)
        pipeline.expireat(self._member_data_key(leaderboard_name), timestamp)
        pipeline.execute()

    def ranked_in_list_in(self, leaderboard_name, members, **options):
        '''
        Retrieve a page of leaders from the named leaderboard for a given list of members.

        @param leaderboard_name [String] Name of the leaderboard.
        @param members [Array] Member names.
        @param options [Hash] Options to be used when retrieving the page from the named leaderboard.
        @return a page of leaders from the named leaderboard for a given list of members.
        '''
        ranks_for_members = []

        pipeline = self.redis_connection.pipeline()

        for member in members:
            if self.order == self.ASC:
                pipeline.zrank(leaderboard_name, member)
            else:
                pipeline.zrevrank(leaderboard_name, member)

            pipeline.zscore(leaderboard_name, member)

        responses = pipeline.execute()

        for index, member in enumerate(members):
            data = {}
            data[self.MEMBER_KEY] = member

            score = responses[index * 2 + 1]
            if score is not None:
                score = float(score)
            data[self.SCORE_KEY] = score

            if self.order == self.ASC:
                data[self.RANK_KEY] = self.redis_connection.zrank(
                    self._ties_leaderboard_key(leaderboard_name), str(data[self.SCORE_KEY]))
            else:
                data[self.RANK_KEY] = self.redis_connection.zrevrank(
                    self._ties_leaderboard_key(leaderboard_name), str(data[self.SCORE_KEY]))
            if data[self.RANK_KEY] is not None:
                data[self.RANK_KEY] += 1
            else:
                if not options.get('include_missing', True):
                    continue

            ranks_for_members.append(data)

        if ('with_member_data' in options) and (True == options['with_member_data']):
            for index, member_data in enumerate(self.members_data_for_in(leaderboard_name, members)):
                ranks_for_members[index][self.MEMBER_DATA_KEY] = member_data

        if 'sort_by' in options:
            if self.RANK_KEY == options['sort_by']:
                ranks_for_members = sorted(
                    ranks_for_members,
                    key=lambda member: member[
                        self.RANK_KEY])
            elif self.SCORE_KEY == options['sort_by']:
                ranks_for_members = sorted(
                    ranks_for_members,
                    key=lambda member: member[
                        self.SCORE_KEY])

        return ranks_for_members

    def _ties_leaderboard_key(self, leaderboard_name):
        '''
        Key for ties leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @return a key in the form of +leaderboard_name:ties_namespace+
        '''
        return '%s:%s' % (leaderboard_name, self.ties_namespace)
