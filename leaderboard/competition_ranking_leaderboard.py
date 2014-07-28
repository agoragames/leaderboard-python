from .leaderboard import Leaderboard
from redis import StrictRedis, Redis, ConnectionPool
import math
from itertools import izip_longest


class CompetitionRankingLeaderboard(Leaderboard):

    def rank_for_in(self, leaderboard_name, member):
        '''
        Retrieve the rank for a member in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param member [String] Member name.
        @return the rank for a member in the leaderboard.
        '''
        member_score = str(float(self.score_for_in(leaderboard_name, member)))
        if self.order == self.ASC:
            try:
                return self.redis_connection.zcount(
                    leaderboard_name, '-inf', '(%s' % member_score) + 1
            except:
                return None
        else:
            try:
                return self.redis_connection.zcount(
                    leaderboard_name, '(%s' % member_score, '+inf') + 1
            except:
                return None

    def score_and_rank_for_in(self, leaderboard_name, member):
        '''
        Retrieve the score and rank for a member in the named leaderboard.

        @param leaderboard_name [String]Name of the leaderboard.
        @param member [String] Member name.
        @return the score and rank for a member in the named leaderboard as a Hash.
        '''
        pipeline = self.redis_connection.pipeline()
        pipeline.zscore(leaderboard_name, member)
        if self.order == self.ASC:
            pipeline.zrank(leaderboard_name, member)
        else:
            pipeline.zrevrank(leaderboard_name, member)
        responses = pipeline.execute()

        if responses[0] is not None:
            responses[0] = float(responses[0])

        if self.order == self.ASC:
            try:
                responses[1] = self.redis_connection.zcount(
                    leaderboard_name, '-inf', "(%s" % str(float(responses[0]))) + 1
            except:
                responses[1] = None
        else:
            try:
                responses[1] = self.redis_connection.zcount(
                    leaderboard_name, "(%s" % str(float(responses[0])), '+inf') + 1
            except:
                responses[1] = None

        return {
            self.MEMBER_KEY: member,
            self.SCORE_KEY: responses[0],
            self.RANK_KEY: responses[1]
        }

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
                data[self.RANK_KEY] = self.redis_connection.zcount(
                    leaderboard_name, '-inf', "(%s" % str(float(data[self.SCORE_KEY])))
            else:
                data[self.RANK_KEY] = self.redis_connection.zcount(
                    leaderboard_name, "(%s" % str(float(data[self.SCORE_KEY])), '+inf')
            if data[self.RANK_KEY] is not None:
                data[self.RANK_KEY] += 1

            if ('with_member_data' in options) and (True == options['with_member_data']):
                data[
                    self.MEMBER_DATA_KEY] = self.member_data_for_in(
                    leaderboard_name,
                    member)

            ranks_for_members.append(data)

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
