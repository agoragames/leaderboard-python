from .leaderboard import Leaderboard


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
        scores = []

        pipeline = self.redis_connection.pipeline()
        for member in members:
            pipeline.zscore(leaderboard_name, member)
        responses = pipeline.execute()

        for index, member in enumerate(members):
            data = {}
            data[self.MEMBER_KEY] = member

            score = responses[index]
            if score is not None:
                score = float(score)
            else:
                if not options.get('include_missing', True):
                    continue

            data[self.SCORE_KEY] = score

            ranks_for_members.append(data)
            scores.append(data[self.SCORE_KEY])

        for index, rank in enumerate(self.__rankings_for_members_having_scores_in(leaderboard_name, members, scores)):
            ranks_for_members[index][self.RANK_KEY] = rank

        if options.get('with_member_data', False):
            self._with_member_data(leaderboard_name, members, ranks_for_members)

        if 'sort_by' in options:
            self._sort_by(ranks_for_members, options['sort_by'])

        return ranks_for_members

    def __up_rank(self, rank):
        if rank is not None:
            return rank + 1
        else:
            return None

    def __rankings_for_members_having_scores_in(self, leaderboard_name, members, scores):
        pipeline = self.redis_connection.pipeline()

        for index, member in enumerate(members):
            if self.order == self.ASC:
                try:
                    pipeline.zcount(leaderboard_name, '-inf', "(%s" % str(float(scores[index])))
                except:
                    None
            else:
                try:
                    pipeline.zcount(leaderboard_name, "(%s" % str(float(scores[index])), '+inf')
                except:
                    None

        responses = pipeline.execute()

        return [self.__up_rank(response) for response in responses]

    def _members_from_rank_range_internal(
            self, leaderboard_name, start_rank, end_rank, members_only=False, **options):
        '''
        Format ordered members with score as efficiently as possible.
        '''
        response = self._range_method(
            self.redis_connection,
            leaderboard_name,
            start_rank,
            end_rank,
            withscores=not members_only)

        if members_only or not response:
            return [{self.MEMBER_KEY: member} for member in response]

        # Find out where the current rank started using the first two ranks
        current_rank = None
        current_score = None
        current_rank_start = 0
        for index, (member, score) in enumerate(response):
            if current_score is None:
                current_rank = self.rank_for_in(leaderboard_name, member)
                current_score = score
            elif score != current_score:
                next_rank = self.rank_for_in(leaderboard_name, member)
                current_rank_start = current_rank - next_rank + index
                break

        members = []
        ranks_for_members = []
        for index, (member, score) in enumerate(response):
            members.append(member)
            if score != current_score:
                current_rank += (index - current_rank_start)
                current_rank_start = index
                current_score = score

            member_entry = {
                self.MEMBER_KEY: member,
                self.RANK_KEY: current_rank,
                self.SCORE_KEY: score,
            }
            ranks_for_members.append(member_entry)

        if options.get('with_member_data', False):
            self._with_member_data(leaderboard_name, members, ranks_for_members)

        if 'sort_by' in options:
            self._sort_by(ranks_for_members, options['sort_by'])

        return ranks_for_members
