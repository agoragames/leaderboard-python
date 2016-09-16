from redis import StrictRedis, Redis, ConnectionPool
import math
import sys
if sys.version_info.major == 3:
    from itertools import zip_longest
else:
    from itertools import izip_longest as zip_longest


def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)


class Leaderboard(object):
    VERSION = '3.5.0'
    DEFAULT_PAGE_SIZE = 25
    DEFAULT_REDIS_HOST = 'localhost'
    DEFAULT_REDIS_PORT = 6379
    DEFAULT_REDIS_DB = 0
    DEFAULT_MEMBER_DATA_NAMESPACE = 'member_data'
    DEFAULT_GLOBAL_MEMBER_DATA = False
    DEFAULT_POOLS = {}
    ASC = 'asc'
    DESC = 'desc'
    MEMBER_KEY = 'member'
    MEMBER_DATA_KEY = 'member_data'
    SCORE_KEY = 'score'
    RANK_KEY = 'rank'

    @classmethod
    def pool(self, host, port, db, pools={}, **options):
        '''
        Fetch a redis conenction pool for the unique combination of host
        and port. Will create a new one if there isn't one already.
        '''
        key = (host, port, db)
        rval = pools.get(key)
        if not isinstance(rval, ConnectionPool):
            rval = ConnectionPool(host=host, port=port, db=db, **options)
            pools[key] = rval

        return rval

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
        self.leaderboard_name = leaderboard_name
        self.options = options

        self.page_size = self.options.pop('page_size', self.DEFAULT_PAGE_SIZE)
        if self.page_size < 1:
            self.page_size = self.DEFAULT_PAGE_SIZE

        self.member_data_namespace = self.options.pop(
            'member_data_namespace',
            self.DEFAULT_MEMBER_DATA_NAMESPACE)
        self.global_member_data = self.options.pop(
            'global_member_data',
            self.DEFAULT_GLOBAL_MEMBER_DATA)

        self.order = self.options.pop('order', self.DESC).lower()
        if not self.order in [self.ASC, self.DESC]:
            raise ValueError(
                "%s is not one of [%s]" % (self.order, ",".join([self.ASC, self.DESC])))

        redis_connection = self.options.pop('redis_connection', None)
        if redis_connection:
            # allow the developer to pass a raw redis connection and
            # we will use it directly instead of creating a new one
            self.redis_connection = redis_connection
        else:
            connection = self.options.pop('connection', None)
            if isinstance(connection, (StrictRedis, Redis)):
                self.options['connection_pool'] = connection.connection_pool
            if 'connection_pool' not in self.options:
                self.options['connection_pool'] = self.pool(
                    self.options.pop('host', self.DEFAULT_REDIS_HOST),
                    self.options.pop('port', self.DEFAULT_REDIS_PORT),
                    self.options.pop('db', self.DEFAULT_REDIS_DB),
                    self.options.pop('pools', self.DEFAULT_POOLS),
                    **self.options
                )
            self.redis_connection = Redis(**self.options)

    def delete_leaderboard(self):
        '''
        Delete the current leaderboard.
        '''
        self.delete_leaderboard_named(self.leaderboard_name)

    def delete_leaderboard_named(self, leaderboard_name):
        '''
        Delete the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        '''
        pipeline = self.redis_connection.pipeline()
        pipeline.delete(leaderboard_name)
        pipeline.delete(self._member_data_key(leaderboard_name))
        pipeline.execute()

    def rank_member(self, member, score, member_data=None):
        '''
        Rank a member in the leaderboard.

        @param member [String] Member name.
        @param score [float] Member score.
        @param member_data [String] Optional member data.
        '''
        self.rank_member_in(self.leaderboard_name, member, score, member_data)

    def rank_member_in(
            self, leaderboard_name, member, score, member_data=None):
        '''
        Rank a member in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param member [String] Member name.
        @param score [float] Member score.
        @param member_data [String] Optional member data.
        '''
        pipeline = self.redis_connection.pipeline()
        if isinstance(self.redis_connection, Redis):
            pipeline.zadd(leaderboard_name, member, score)
        else:
            pipeline.zadd(leaderboard_name, score, member)
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
        pipeline = self.redis_connection.pipeline()
        for leaderboard_name in leaderboards:
            if isinstance(self.redis_connection, Redis):
                pipeline.zadd(leaderboard_name, member, score)
            else:
                pipeline.zadd(leaderboard_name, score, member)
            if member_data:
                pipeline.hset(
                    self._member_data_key(leaderboard_name),
                    member,
                    member_data)
        pipeline.execute()

    def rank_member_if(
            self, rank_conditional, member, score, member_data=None):
        '''
        Rank a member in the leaderboard based on execution of the +rank_conditional+.

        The +rank_conditional+ is passed the following parameters:
          member: Member name.
          current_score: Current score for the member in the leaderboard.
          score: Member score.
          member_data: Optional member data.
          leaderboard_options: Leaderboard options, e.g. 'reverse': Value of reverse option

        @param rank_conditional [function] Function which must return +True+ or +False+ that controls whether or not the member is ranked in the leaderboard.
        @param member [String] Member name.
        @param score [float] Member score.
        @param member_data [String] Optional member_data.
        '''
        self.rank_member_if_in(
            self.leaderboard_name,
            rank_conditional,
            member,
            score,
            member_data)

    def rank_member_if_in(
            self,
            leaderboard_name,
            rank_conditional,
            member,
            score,
            member_data=None):
        '''
        Rank a member in the named leaderboard based on execution of the +rank_conditional+.

        The +rank_conditional+ is passed the following parameters:
          member: Member name.
          current_score: Current score for the member in the leaderboard.
          score: Member score.
          member_data: Optional member data.
          leaderboard_options: Leaderboard options, e.g. 'reverse': Value of reverse option

        @param leaderboard_name [String] Name of the leaderboard.
        @param rank_conditional [function] Function which must return +True+ or +False+ that controls whether or not the member is ranked in the leaderboard.
        @param member [String] Member name.
        @param score [float] Member score.
        @param member_data [String] Optional member_data.
        '''
        current_score = self.redis_connection.zscore(leaderboard_name, member)
        if current_score is not None:
            current_score = float(current_score)

        if rank_conditional(self, member, current_score, score, member_data, {'reverse': self.order}):
            self.rank_member_in(leaderboard_name, member, score, member_data)

    def rank_members(self, members_and_scores):
        '''
        Rank an array of members in the leaderboard.

        @param members_and_scores [Array] Variable list of members and scores.
        '''
        self.rank_members_in(self.leaderboard_name, members_and_scores)

    def rank_members_in(self, leaderboard_name, members_and_scores):
        '''
        Rank an array of members in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param members_and_scores [Array] Variable list of members and scores.
        '''
        pipeline = self.redis_connection.pipeline()
        for member, score in grouper(2, members_and_scores):
            if isinstance(self.redis_connection, Redis):
                pipeline.zadd(leaderboard_name, member, score)
            else:
                pipeline.zadd(leaderboard_name, score, member)
        pipeline.execute()

    def member_data_for(self, member):
        '''
        Retrieve the optional member data for a given member in the leaderboard.

        @param member [String] Member name.
        @return String of optional member data.
        '''
        return self.member_data_for_in(self.leaderboard_name, member)

    def member_data_for_in(self, leaderboard_name, member):
        '''
        Retrieve the optional member data for a given member in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param member [String] Member name.
        @return String of optional member data.
        '''
        return self.redis_connection.hget(
            self._member_data_key(leaderboard_name), member)

    def members_data_for(self, members):
        '''
        Retrieve the optional member data for a given list of members in the leaderboard.

        @param members [Array] Member names.
        @return Array of strings of optional member data.
        '''
        return self.members_data_for_in(self.leaderboard_name, members)

    def members_data_for_in(self, leaderboard_name, members):
        '''
        Retrieve the optional member data for a given list of members in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param members [Array] Member names.
        @return Array of strings of optional member data.
        '''
        return self.redis_connection.hmget(
            self._member_data_key(leaderboard_name), members)

    def update_member_data(self, member, member_data):
        '''
        Update the optional member data for a given member in the leaderboard.

        @param member [String] Member name.
        @param member_data [String] Optional member data.
        '''
        self.update_member_data_in(self.leaderboard_name, member, member_data)

    def update_member_data_in(self, leaderboard_name, member, member_data):
        '''
        Update the optional member data for a given member in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param member [String] Member name.
        @param member_data [String] Optional member data.
        '''
        self.redis_connection.hset(
            self._member_data_key(leaderboard_name),
            member,
            member_data)

    def remove_member_data(self, member):
        '''
        Remove the optional member data for a given member in the leaderboard.

        @param member [String] Member name.
        '''
        self.remove_member_data_in(self.leaderboard_name, member)

    def remove_member_data_in(self, leaderboard_name, member):
        '''
        Remove the optional member data for a given member in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param member [String] Member name.
        '''
        self.redis_connection.hdel(
            self._member_data_key(leaderboard_name),
            member)

    def total_members(self):
        '''
        Retrieve the total number of members in the leaderboard.

        @return total number of members in the leaderboard.
        '''
        return self.total_members_in(self.leaderboard_name)

    def total_members_in(self, leaderboard_name):
        '''
        Retrieve the total number of members in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @return the total number of members in the named leaderboard.
        '''
        return self.redis_connection.zcard(leaderboard_name)

    def remove_member(self, member):
        '''
        Remove a member from the leaderboard.

        @param member [String] Member name.
        '''
        self.remove_member_from(self.leaderboard_name, member)

    def remove_member_from(self, leaderboard_name, member):
        '''
        Remove the optional member data for a given member in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param member [String] Member name.
        '''
        pipeline = self.redis_connection.pipeline()
        pipeline.zrem(leaderboard_name, member)
        pipeline.hdel(self._member_data_key(leaderboard_name), member)
        pipeline.execute()

    def total_pages(self, page_size=None):
        '''
        Retrieve the total number of pages in the leaderboard.

        @param page_size [int, nil] Page size to be used when calculating the total number of pages.
        @return the total number of pages in the leaderboard.
        '''
        return self.total_pages_in(self.leaderboard_name, page_size)

    def total_pages_in(self, leaderboard_name, page_size=None):
        '''
        Retrieve the total number of pages in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param page_size [int, nil] Page size to be used when calculating the total number of pages.
        @return the total number of pages in the named leaderboard.
        '''
        if page_size is None:
            page_size = self.page_size

        return int(
            math.ceil(
                self.total_members_in(leaderboard_name) /
                float(page_size)))

    def total_members_in_score_range(self, min_score, max_score):
        '''
        Retrieve the total members in a given score range from the leaderboard.

        @param min_score [float] Minimum score.
        @param max_score [float] Maximum score.
        @return the total members in a given score range from the leaderboard.
        '''
        return self.total_members_in_score_range_in(
            self.leaderboard_name, min_score, max_score)

    def total_members_in_score_range_in(
            self, leaderboard_name, min_score, max_score):
        '''
        Retrieve the total members in a given score range from the named leaderboard.

        @param leaderboard_name Name of the leaderboard.
        @param min_score [float] Minimum score.
        @param max_score [float] Maximum score.
        @return the total members in a given score range from the named leaderboard.
        '''
        return self.redis_connection.zcount(
            leaderboard_name, min_score, max_score)

    def check_member(self, member):
        '''
        Check to see if a member exists in the leaderboard.

        @param member [String] Member name.
        @return +true+ if the member exists in the leaderboard, +false+ otherwise.
        '''
        return self.check_member_in(self.leaderboard_name, member)

    def check_member_in(self, leaderboard_name, member):
        '''
        Check to see if a member exists in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param member [String] Member name.
        @return +true+ if the member exists in the named leaderboard, +false+ otherwise.
        '''
        return self.redis_connection.zscore(
            leaderboard_name, member) is not None

    def rank_for(self, member):
        '''
        Retrieve the rank for a member in the leaderboard.

        @param member [String] Member name.
        @return the rank for a member in the leaderboard.
        '''
        return self.rank_for_in(self.leaderboard_name, member)

    def rank_for_in(self, leaderboard_name, member):
        '''
        Retrieve the rank for a member in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param member [String] Member name.
        @return the rank for a member in the leaderboard.
        '''
        if self.order == self.ASC:
            try:
                return self.redis_connection.zrank(
                    leaderboard_name, member) + 1
            except:
                return None
        else:
            try:
                return self.redis_connection.zrevrank(
                    leaderboard_name, member) + 1
            except:
                return None

    def score_for(self, member):
        '''
        Retrieve the score for a member in the leaderboard.

        @param member Member name.
        @return the score for a member in the leaderboard or +None+ if the member is not in the leaderboard.
        '''
        return self.score_for_in(self.leaderboard_name, member)

    def score_for_in(self, leaderboard_name, member):
        '''
        Retrieve the score for a member in the named leaderboard.

        @param leaderboard_name Name of the leaderboard.
        @param member [String] Member name.
        @return the score for a member in the leaderboard or +None+ if the member is not in the leaderboard.
        '''
        score = self.redis_connection.zscore(leaderboard_name, member)
        if score is not None:
            score = float(score)

        return score

    def score_and_rank_for(self, member):
        '''
        Retrieve the score and rank for a member in the leaderboard.

        @param member [String] Member name.
        @return the score and rank for a member in the leaderboard as a Hash.
        '''
        return self.score_and_rank_for_in(self.leaderboard_name, member)

    def score_and_rank_for_in(self, leaderboard_name, member):
        '''
        Retrieve the score and rank for a member in the named leaderboard.

        @param leaderboard_name [String]Name of the leaderboard.
        @param member [String] Member name.
        @return the score and rank for a member in the named leaderboard as a Hash.
        '''
        return {
            self.MEMBER_KEY: member,
            self.SCORE_KEY: self.score_for_in(leaderboard_name, member),
            self.RANK_KEY: self.rank_for_in(leaderboard_name, member)
        }

    def change_score_for(self, member, delta, member_data=None):
        '''
        Change the score for a member in the leaderboard by a score delta which can be positive or negative.

        @param member [String] Member name.
        @param delta [float] Score change.
        @param member_data [String] Optional member data.
        '''
        self.change_score_for_member_in(self.leaderboard_name, member, delta, member_data)

    def change_score_for_member_in(self, leaderboard_name, member, delta, member_data=None):
        '''
        Change the score for a member in the named leaderboard by a delta which can be positive or negative.

        @param leaderboard_name [String] Name of the leaderboard.
        @param member [String] Member name.
        @param delta [float] Score change.
        @param member_data [String] Optional member data.
        '''
        pipeline = self.redis_connection.pipeline()
        pipeline.zincrby(leaderboard_name, member, delta)
        if member_data:
            pipeline.hset(
                self._member_data_key(leaderboard_name),
                member,
                member_data)
        pipeline.execute()

    def remove_members_in_score_range(self, min_score, max_score):
        '''
        Remove members from the leaderboard in a given score range.

        @param min_score [float] Minimum score.
        @param max_score [float] Maximum score.
        '''
        self.remove_members_in_score_range_in(
            self.leaderboard_name,
            min_score,
            max_score)

    def remove_members_in_score_range_in(
            self, leaderboard_name, min_score, max_score):
        '''
        Remove members from the named leaderboard in a given score range.

        @param leaderboard_name [String] Name of the leaderboard.
        @param min_score [float] Minimum score.
        @param max_score [float] Maximum score.
        '''
        self.redis_connection.zremrangebyscore(
            leaderboard_name,
            min_score,
            max_score)

    def remove_members_outside_rank(self, rank):
        '''
        Remove members from the leaderboard in a given rank range.

        @param rank [int] the rank (inclusive) which we should keep.
        @return the total member count which was removed.
        '''
        return self.remove_members_outside_rank_in(self.leaderboard_name, rank)

    def remove_members_outside_rank_in(self, leaderboard_name, rank):
        '''
        Remove members from the named leaderboard in a given rank range.

        @param leaderboard_name [String] Name of the leaderboard.
        @param rank [int] the rank (inclusive) which we should keep.
        @return the total member count which was removed.
        '''
        if self.order == self.DESC:
            rank = -(rank) - 1
            return self.redis_connection.zremrangebyrank(
                leaderboard_name, 0, rank)
        else:
            return self.redis_connection.zremrangebyrank(
                leaderboard_name, rank, -1)

    def page_for(self, member, page_size=DEFAULT_PAGE_SIZE):
        '''
        Determine the page where a member falls in the leaderboard.

        @param member [String] Member name.
        @param page_size [int] Page size to be used in determining page location.
        @return the page where a member falls in the leaderboard.
        '''
        return self.page_for_in(self.leaderboard_name, member, page_size)

    def page_for_in(self, leaderboard_name, member,
                    page_size=DEFAULT_PAGE_SIZE):
        '''
        Determine the page where a member falls in the named leaderboard.

        @param leaderboard [String] Name of the leaderboard.
        @param member [String] Member name.
        @param page_size [int] Page size to be used in determining page location.
        @return the page where a member falls in the leaderboard.
        '''
        rank_for_member = None
        if self.order == self.ASC:
            rank_for_member = self.redis_connection.zrank(
                leaderboard_name,
                member)
        else:
            rank_for_member = self.redis_connection.zrevrank(
                leaderboard_name,
                member)

        if rank_for_member is None:
            rank_for_member = 0
        else:
            rank_for_member += 1

        return int(math.ceil(float(rank_for_member) / float(page_size)))

    def percentile_for(self, member):
        '''
        Retrieve the percentile for a member in the leaderboard.

        @param member [String] Member name.
        @return the percentile for a member in the leaderboard. Return +nil+ for a non-existent member.
        '''
        return self.percentile_for_in(self.leaderboard_name, member)

    def percentile_for_in(self, leaderboard_name, member):
        '''
        Retrieve the percentile for a member in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param member [String] Member name.
        @return the percentile for a member in the named leaderboard.
        '''
        if not self.check_member_in(leaderboard_name, member):
            return None

        responses = self.redis_connection.pipeline().zcard(
            leaderboard_name).zrevrank(leaderboard_name, member).execute()

        percentile = math.ceil(
            (float(
                (responses[0] -
                 responses[1] -
                 1)) /
                float(
                    responses[0]) *
                100))

        if self.order == self.ASC:
            return 100 - percentile
        else:
            return percentile

    def score_for_percentile(self, percentile):
        '''
        Calculate the score for a given percentile value in the leaderboard.

        @param percentile [float] Percentile value (0.0 to 100.0 inclusive).
        @return the score corresponding to the percentile argument. Return +None+ for arguments outside 0-100 inclusive and for leaderboards with no members.
        '''
        return self.score_for_percentile_in(self.leaderboard_name, percentile)

    def score_for_percentile_in(self, leaderboard_name, percentile):
        '''
        Calculate the score for a given percentile value in the leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param percentile [float] Percentile value (0.0 to 100.0 inclusive).
        @return the score corresponding to the percentile argument. Return +None+ for arguments outside 0-100 inclusive and for leaderboards with no members.
        '''
        if not 0 <= percentile <= 100:
            return None

        total_members = self.total_members_in(leaderboard_name)
        if total_members < 1:
            return None

        if self.order == self.ASC:
            percentile = 100 - percentile

        index = (total_members - 1) * (percentile / 100.0)

        scores = [
            pair[1] for pair in self.redis_connection.zrange(
                leaderboard_name, int(
                    math.floor(index)), int(
                    math.ceil(index)), withscores=True)]

        if index == math.floor(index):
            return scores[0]
        else:
            interpolate_fraction = index - math.floor(index)
            return scores[0] + interpolate_fraction * (scores[1] - scores[0])

    def expire_leaderboard(self, seconds):
        '''
        Expire the current leaderboard in a set number of seconds. Do not use this with
        leaderboards that utilize member data as there is no facility to cascade the
        expiration out to the keys for the member data.

        @param seconds [int] Number of seconds after which the leaderboard will be expired.
        '''
        self.expire_leaderboard_for(self.leaderboard_name, seconds)

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
        pipeline.expire(self._member_data_key(leaderboard_name), seconds)
        pipeline.execute()

    def expire_leaderboard_at(self, timestamp):
        '''
        Expire the current leaderboard at a specific UNIX timestamp. Do not use this with
        leaderboards that utilize member data as there is no facility to cascade the
        expiration out to the keys for the member data.

        @param timestamp [int] UNIX timestamp at which the leaderboard will be expired.
        '''
        self.expire_leaderboard_at_for(self.leaderboard_name, timestamp)

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
        pipeline.expireat(self._member_data_key(leaderboard_name), timestamp)
        pipeline.execute()

    def leaders(self, current_page, **options):
        '''
        Retrieve a page of leaders from the leaderboard.

        @param current_page [int] Page to retrieve from the leaderboard.
        @param options [Hash] Options to be used when retrieving the page from the leaderboard.
        @return a page of leaders from the leaderboard.
        '''
        return self.leaders_in(self.leaderboard_name, current_page, **options)

    def leaders_in(self, leaderboard_name, current_page, **options):
        '''
        Retrieve a page of leaders from the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param current_page [int] Page to retrieve from the named leaderboard.
        @param options [Hash] Options to be used when retrieving the page from the named leaderboard.
        @return a page of leaders from the named leaderboard.
        '''
        if current_page < 1:
            current_page = 1

        page_size = options.get('page_size', self.page_size)

        index_for_redis = current_page - 1

        starting_offset = (index_for_redis * page_size)
        if starting_offset < 0:
            starting_offset = 0

        ending_offset = (starting_offset + page_size) - 1

        raw_leader_data = self._range_method(
            self.redis_connection,
            leaderboard_name,
            int(starting_offset),
            int(ending_offset),
            withscores=False)
        return self._parse_raw_members(
            leaderboard_name, raw_leader_data, **options)

    def all_leaders(self, **options):
        '''
        Retrieve all leaders from the leaderboard.

        @param options [Hash] Options to be used when retrieving the leaders from the leaderboard.
        @return the leaders from the leaderboard.
        '''
        return self.all_leaders_from(self.leaderboard_name, **options)

    def all_leaders_from(self, leaderboard_name, **options):
        '''
        Retrieves all leaders from the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param options [Hash] Options to be used when retrieving the leaders from the named leaderboard.
        @return the named leaderboard.
        '''
        raw_leader_data = self._range_method(
            self.redis_connection, leaderboard_name, 0, -1, withscores=False)
        return self._parse_raw_members(
            leaderboard_name, raw_leader_data, **options)

    def members_from_score_range(
            self, minimum_score, maximum_score, **options):
        '''
        Retrieve members from the leaderboard within a given score range.

        @param minimum_score [float] Minimum score (inclusive).
        @param maximum_score [float] Maximum score (inclusive).
        @param options [Hash] Options to be used when retrieving the data from the leaderboard.
        @return members from the leaderboard that fall within the given score range.
        '''
        return self.members_from_score_range_in(
            self.leaderboard_name, minimum_score, maximum_score, **options)

    def members_from_score_range_in(
            self, leaderboard_name, minimum_score, maximum_score, **options):
        '''
        Retrieve members from the named leaderboard within a given score range.

        @param leaderboard_name [String] Name of the leaderboard.
        @param minimum_score [float] Minimum score (inclusive).
        @param maximum_score [float] Maximum score (inclusive).
        @param options [Hash] Options to be used when retrieving the data from the leaderboard.
        @return members from the leaderboard that fall within the given score range.
        '''
        raw_leader_data = []
        if self.order == self.DESC:
            raw_leader_data = self.redis_connection.zrevrangebyscore(
                leaderboard_name,
                maximum_score,
                minimum_score)
        else:
            raw_leader_data = self.redis_connection.zrangebyscore(
                leaderboard_name,
                minimum_score,
                maximum_score)
        return self._parse_raw_members(
            leaderboard_name, raw_leader_data, **options)

    def members_from_rank_range(self, starting_rank, ending_rank, **options):
        '''
        Retrieve members from the leaderboard within a given rank range.

        @param starting_rank [int] Starting rank (inclusive).
        @param ending_rank [int] Ending rank (inclusive).
        @param options [Hash] Options to be used when retrieving the data from the leaderboard.
        @return members from the leaderboard that fall within the given rank range.
        '''
        return self.members_from_rank_range_in(
            self.leaderboard_name, starting_rank, ending_rank, **options)

    def members_from_rank_range_in(
            self, leaderboard_name, starting_rank, ending_rank, **options):
        '''
        Retrieve members from the named leaderboard within a given rank range.

        @param leaderboard_name [String] Name of the leaderboard.
        @param starting_rank [int] Starting rank (inclusive).
        @param ending_rank [int] Ending rank (inclusive).
        @param options [Hash] Options to be used when retrieving the data from the leaderboard.
        @return members from the leaderboard that fall within the given rank range.
        '''
        starting_rank -= 1
        if starting_rank < 0:
            starting_rank = 0

        ending_rank -= 1
        if ending_rank > self.total_members_in(leaderboard_name):
            ending_rank = self.total_members_in(leaderboard_name) - 1

        raw_leader_data = []
        if self.order == self.DESC:
            raw_leader_data = self.redis_connection.zrevrange(
                leaderboard_name,
                starting_rank,
                ending_rank,
                withscores=False)
        else:
            raw_leader_data = self.redis_connection.zrange(
                leaderboard_name,
                starting_rank,
                ending_rank,
                withscores=False)

        return self._parse_raw_members(
            leaderboard_name, raw_leader_data, **options)

    def top(self, number, **options):
        '''
        Retrieve members from the leaderboard within a range from 1 to the number given.

        @param ending_rank [int] Ending rank (inclusive).
        @param options [Hash] Options to be used when retrieving the data from the leaderboard.

        @return number from the leaderboard that fall within the given rank range.
        '''
        return self.top_in(self.leaderboard_name, number, **options)

    def top_in(self, leaderboard_name, number, **options):
        '''
        Retrieve members from the named leaderboard within a range from 1 to the number given.

        @param leaderboard_name [String] Name of the leaderboard.
        @param starting_rank [int] Starting rank (inclusive).
        @param ending_rank [int] Ending rank (inclusive).
        @param options [Hash] Options to be used when retrieving the data from the leaderboard.

        @return members from the leaderboard that fall within the given rank range.
        '''
        return self.members_from_rank_range_in(leaderboard_name, 1, number, **options)

    def member_at(self, position, **options):
        '''
        Retrieve a member at the specified index from the leaderboard.

        @param position [int] Position in leaderboard.
        @param options [Hash] Options to be used when retrieving the member from the leaderboard.
        @return a member from the leaderboard.
        '''
        return self.member_at_in(self.leaderboard_name, position, **options)

    def member_at_in(self, leaderboard_name, position, **options):
        '''
        Retrieve a member at the specified index from the leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param position [int] Position in named leaderboard.
        @param options [Hash] Options to be used when retrieving the member from the named leaderboard.
        @return a page of leaders from the named leaderboard.
        '''
        if position > 0 and position <= self.total_members_in(leaderboard_name):
            page_size = options.get('page_size', self.page_size)
            current_page = math.ceil(float(position) / float(page_size))
            offset = (position - 1) % page_size

            leaders = self.leaders_in(
                leaderboard_name,
                current_page,
                **options)
            if leaders:
                return leaders[offset]

    def around_me(self, member, **options):
        '''
        Retrieve a page of leaders from the leaderboard around a given member.

        @param member [String] Member name.
        @param options [Hash] Options to be used when retrieving the page from the leaderboard.
        @return a page of leaders from the leaderboard around a given member.
        '''
        return self.around_me_in(self.leaderboard_name, member, **options)

    def around_me_in(self, leaderboard_name, member, **options):
        '''
        Retrieve a page of leaders from the named leaderboard around a given member.

        @param leaderboard_name [String] Name of the leaderboard.
        @param member [String] Member name.
        @param options [Hash] Options to be used when retrieving the page from the named leaderboard.
        @return a page of leaders from the named leaderboard around a given member. Returns an empty array for a non-existent member.
        '''
        reverse_rank_for_member = None
        if self.order == self.DESC:
            reverse_rank_for_member = self.redis_connection.zrevrank(
                leaderboard_name,
                member)
        else:
            reverse_rank_for_member = self.redis_connection.zrank(
                leaderboard_name,
                member)

        if reverse_rank_for_member is None:
            return []

        page_size = options.get('page_size', self.page_size)

        starting_offset = reverse_rank_for_member - (page_size / 2)
        if starting_offset < 0:
            starting_offset = 0

        ending_offset = (starting_offset + page_size) - 1

        raw_leader_data = self._range_method(
            self.redis_connection,
            leaderboard_name,
            int(starting_offset),
            int(ending_offset),
            withscores=False)
        return self._parse_raw_members(
            leaderboard_name, raw_leader_data, **options)

    def ranked_in_list(self, members, **options):
        '''
        Retrieve a page of leaders from the leaderboard for a given list of members.

        @param members [Array] Member names.
        @param options [Hash] Options to be used when retrieving the page from the leaderboard.
        @return a page of leaders from the leaderboard for a given list of members.
        '''
        return self.ranked_in_list_in(
            self.leaderboard_name, members, **options)

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
            rank = responses[index * 2]
            if rank is not None:
                rank += 1
            else:
                if not options.get('include_missing', True):
                    continue
            data[self.RANK_KEY] = rank
            score = responses[index * 2 + 1]
            if score is not None:
                score = float(score)
            data[self.SCORE_KEY] = score

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

    def merge_leaderboards(self, destination, keys, aggregate='SUM'):
        '''
        Merge leaderboards given by keys with this leaderboard into a named destination leaderboard.

        @param destination [String] Destination leaderboard name.
        @param keys [Array] Leaderboards to be merged with the current leaderboard.
        @param options [Hash] Options for merging the leaderboards.
        '''
        keys.insert(0, self.leaderboard_name)
        self.redis_connection.zunionstore(destination, keys, aggregate)

    def intersect_leaderboards(self, destination, keys, aggregate='SUM'):
        '''
        Intersect leaderboards given by keys with this leaderboard into a named destination leaderboard.

        @param destination [String] Destination leaderboard name.
        @param keys [Array] Leaderboards to be merged with the current leaderboard.
        @param options [Hash] Options for intersecting the leaderboards.
        '''
        keys.insert(0, self.leaderboard_name)
        self.redis_connection.zinterstore(destination, keys, aggregate)

    def _range_method(self, connection, *args, **kwargs):
        if self.order == self.DESC:
            return connection.zrevrange(*args, **kwargs)
        else:
            return connection.zrange(*args, **kwargs)

    def _member_data_key(self, leaderboard_name):
        '''
        Key for retrieving optional member data.

        @param leaderboard_name [String] Name of the leaderboard.
        @return a key in the form of +leaderboard_name:member_data+
        '''
        if self.global_member_data is False:
            return '%s:%s' % (leaderboard_name, self.member_data_namespace)
        else:
            return self.member_data_namespace

    def _parse_raw_members(
            self, leaderboard_name, members, members_only=False, **options):
        '''
        Parse the raw leaders data as returned from a given leader board query. Do associative
        lookups with the member to rank, score and potentially sort the results.

        @param leaderboard_name [String] Name of the leaderboard.
        @param members [List] A list of members as returned from a sorted set range query
        @param members_only [bool] Set True to return the members as is, Default is False.
        @param options [Hash] Options to be used when retrieving the page from the named leaderboard.
        @return a list of members.
        '''
        if members_only:
            return [{self.MEMBER_KEY: m} for m in members]

        if members:
            return self.ranked_in_list_in(leaderboard_name, members, **options)
        else:
            return []
