from redis import Redis, ConnectionPool
from copy import deepcopy
import math

class Leaderboard(object):
  VERSION = "2.0.0"
  DEFAULT_PAGE_SIZE = 25
  DEFAULT_REDIS_HOST = 'localhost'
  DEFAULT_REDIS_PORT = 6379
  DEFAULT_REDIS_DB = 0
  ASC = 'asc'
  DESC = 'desc'

  @classmethod
  def pool(self, host, port, db, pools={}):
    '''
    Fetch a redis conenction pool for the unique combination of host
    and port. Will create a new one if there isn't one already.
    '''
    key = (host,port,db)
    rval = pools.get( key )
    if not isinstance(rval,ConnectionPool):
      rval = ConnectionPool(host=host, port=port, db=db)
      pools[ key ] = rval

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
    self.options = deepcopy(options)

    self.page_size = self.options.pop('page_size', self.DEFAULT_PAGE_SIZE)
    if self.page_size < 1:
      self.page_size = self.DEFAULT_PAGE_SIZE

    self.order = self.options.pop('order', self.DESC).lower()
    if not self.order in [self.ASC, self.DESC]:
      raise ValueError("%s is not one of [%s]" % (self.order,  ",".join([self.ASC, self.DESC])))

    self.redis_connection = self.options.pop('connection', None)
    if not isinstance(self.redis_connection, Redis):
      if 'connection_pool' not in self.options:
        self.options['connection_pool'] = self.pool(
          self.options.pop('host', self.DEFAULT_REDIS_HOST),
          self.options.pop('port', self.DEFAULT_REDIS_PORT),
          self.options.pop('db', self.DEFAULT_REDIS_DB)
        )
      self.redis_connection = Redis(**self.options)

  def delete_leaderboard(self):
    self.delete_leaderboard_named(self.leaderboard_name)

  def delete_leaderboard_named(self, leaderboard_name):
    pipeline = self.redis_connection.pipeline()
    pipeline.delete(leaderboard_name)
    pipeline.delete(self._member_data_key(leaderboard_name))
    pipeline.execute()

  def rank_member(self, member, score, member_data = None):
    self.rank_member_in(self.leaderboard_name, member, score, member_data)

  def rank_member_in(self, leaderboard_name, member, score, member_data = None):
    pipeline = self.redis_connection.pipeline()
    pipeline.zadd(leaderboard_name, member, score)
    if member_data:
      pipeline.hset(self._member_data_key(leaderboard_name), member, member_data)
    pipeline.execute()

  def member_data_for(self, member):
    return self.member_data_for_in(self.leaderboard_name, member)

  def member_data_for_in(self, leaderboard_name, member):
    return self.redis_connection.hget(self._member_data_key(leaderboard_name), member)

  def update_member_data(self, member, member_data):
    self.update_member_data_in(self.leaderboard_name, member, member_data)

  def update_member_data_in(self, leaderboard_name, member, member_data):
    self.redis_connection.hset(self._member_data_key(leaderboard_name), member, member_data)

  def remove_member_data(self, member):
    self.remove_member_data_in(self.leaderboard_name, member)

  def remove_member_data_in(self, leaderboard_name, member):
    self.redis_connection.hdel(self._member_data_key(leaderboard_name), member)

  def total_members(self):
    return self.total_members_in(self.leaderboard_name)

  def total_members_in(self, leaderboard_name):
    return self.redis_connection.zcard(leaderboard_name)

  def remove_member(self, member):
    self.remove_member_from(self.leaderboard_name, member)

  def remove_member_from(self, leaderboard_name, member):
    pipeline = self.redis_connection.pipeline()
    pipeline.zrem(leaderboard_name, member)
    pipeline.execute()

  def total_pages(self, page_size = None):
    return self.total_pages_in(self.leaderboard_name, page_size)

  def total_pages_in(self, leaderboard_name, page_size = None):
    if page_size is None:
      page_size = self.page_size

    return math.ceil(self.total_members_in(leaderboard_name) / float(page_size))

  def total_members_in_score_range(self, min_score, max_score):
    return self.total_members_in_score_range_in(self.leaderboard_name, min_score, max_score)

  def total_members_in_score_range_in(self, leaderboard_name, min_score, max_score):
    return self.redis_connection.zcount(leaderboard_name, min_score, max_score)

  def check_member(self, member):
    return self.check_member_in(self.leaderboard_name, member)

  def check_member_in(self, leaderboard_name, member):
    return self.redis_connection.zscore(leaderboard_name, member) is not None

  def rank_for(self, member):
    return self.rank_for_in(self.leaderboard_name, member)

  def rank_for_in(self, leaderboard_name, member):
    if self.order == self.ASC:
      return self.redis_connection.zrank(leaderboard_name, member) + 1
    else:
      return self.redis_connection.zrevrank(leaderboard_name, member) + 1

  def score_for(self, member):
    self.score_for_in(self.leaderboard_name, member)

  def score_for_in(self, leaderboard_name, member):
    return float(self.redis_connection.zscore(leaderboard_name, member))

  def score_and_rank_for(self, member, use_zero_index_for_rank = False, **options):
    return self.score_and_rank_for_in(self.leaderboard_name, member)

  def score_and_rank_for_in(self, leaderboard_name, member):
    return {
      'member' : member,
      'score' : self.score_for_in(leaderboard_name, member),
      'rank' : self.rank_for_in(leaderboard_name, member)
    }

  def change_score_for(self, member, delta):
    self.change_score_for_member_in(self.leaderboard_name, member, delta)

  def change_score_for_member_in(self, leaderboard_name, member, delta):
    self.redis_connection.zincrby(leaderboard_name, member, delta)

  def remove_members_in_score_range(self, min_score, max_score):
    self.remove_members_in_score_range_in(self.leaderboard_name, min_score, max_score)

  def remove_members_in_score_range_in(self, leaderboard_name, min_score, max_score):
    self.redis_connection.zremrangebyscore(leaderboard_name, min_score, max_score)

  def page_for(self, member, page_size = DEFAULT_PAGE_SIZE):
    return self.page_for_in(self.leaderboard_name, member, page_size)
  
  def page_for_in(self, leaderboard_name, member, page_size = DEFAULT_PAGE_SIZE):
    rank_for_member = None
    if self.order == self.DESC:
      rank_for_member = self.redis_connection.zrank(leaderboard_name, member)
    else:
      rank_for_memebr = self.redis_connection.zrevrank(leaderboard_name, member)

    if rank_for_member == None:
      rank_for_member = 0
    else:
      rank_for_member += 1

    return math.ceil(float(rank_for_member) / float(page_size))

  def percentile_for(self, member):
    return self.percentile_for_in(self.leaderboard_name, member)

  def percentile_for_in(self, leaderboard_name, member):
    if not self.check_member_in(leaderboard_name, member):
      return None

    responses = self.redis_connection.pipeline().zcard(leaderboard_name).zrevrank(leaderboard_name, member).execute()

    percentile = math.ceil((float((responses[0] - responses[1] - 1)) / float(responses[0]) * 100))

    if self.order == self.ASC:      
      return 100 - percentile
    else:
      return percentile

  def expire_leaderboard(self, seconds):
    self.expire_leaderboard_for(self.leaderboard_name, seconds)

  def expire_leaderboard_for(self, leaderboard_name, seconds):
    self.redis_connection.expire(leaderboard_name, seconds)

  def expire_leaderboard_at(self, timestamp):
    self.expire_leaderboard_at_for(self.leaderboard_name, timestamp)

  def expire_leaderboard_at_for(self, leaderboard_name, timestamp):
    self.redis_connection.expireat(leaderboard_name, timestamp)

  def leaders(self, current_page, **options):
    return self.leaders_in(self.leaderboard_name, current_page, **options)

  def leaders_in(self, leaderboard_name, current_page, **options):
    if current_page < 1:
      current_page = 1

    page_size = options.get('page_size', self.page_size)
    total_pages = self.total_pages(page_size = page_size)

    index_for_redis = current_page - 1

    starting_offset = (index_for_redis * page_size)
    if starting_offset < 0:
      starting_offset = 0

    ending_offset = (starting_offset + page_size) - 1

    raw_leader_data = self._range_method(self.redis_connection, self.leaderboard_name, int(starting_offset), int(ending_offset), withscores = False)
    if raw_leader_data:
      return self.ranked_in_list_in(self.leaderboard_name, raw_leader_data, **options)
    else:
      return None

  def all_leaders(self, **options):
    return self.all_leaders_from(self.leaderboard_name, **options)

  def all_leaders_from(self, leaderboard_name, **options):
    raw_leader_data = self._range_method(self.redis_connection, leaderboard_name, 0, -1, withscores = False)

    if raw_leader_data:
      return self.ranked_in_list_in(leaderboard_name, raw_leader_data, **options)
    else:
      return []

  def members_from_score_range(self, minimum_score, maximum_score, **options):
    return self.members_from_score_range_in(self.leaderboard_name, minimum_score, maximum_score, **options)

  def members_from_score_range_in(self, leaderboard_name, minimum_score, maximum_score, **options):
    raw_leader_data = []
    if self.order == self.DESC:
      raw_leader_data = self.redis_connection.zrevrangebyscore(leaderboard_name, maximum_score, minimum_score)
    else:
      raw_leader_data = self.redis_connection.zrangebyscore(leaderboard_name, minimum_score, maximum_score)

    if raw_leader_data:
      return self.ranked_in_list_in(leaderboard_name, raw_leader_data, **options)
    else:
      return []

  def members_from_rank_range(self, starting_rank, ending_rank, **options):
    return self.members_from_rank_range_in(self.leaderboard_name, starting_rank, ending_rank, **options)

  def members_from_rank_range_in(self, leaderboard_name, starting_rank, ending_rank, **options):
    starting_rank -= 1
    if starting_rank < 0:
      starting_rank = 0

    ending_rank -= 1
    if ending_rank > self.total_members_in(leaderboard_name):
      ending_rank = self.total_members_in(leaderboard_name) - 1

    raw_leader_data = []
    if self.order == self.DESC:
      raw_leader_data = self.redis_connection.zrevrange(leaderboard_name, starting_rank, ending_rank, withscores = False)
    else:
      raw_leader_data = self.redis_connection.zrange(leaderboard_name, starting_rank, ending_rank, withscores = False)

    if raw_leader_data:
      return self. ranked_in_list_in(leaderboard_name, raw_leader_data, **options)
    else:
      return []

  def around_me(self, member, **options):
    return self.around_me_in(self.leaderboard_name, member, **options)
  
  def around_me_in(self, leaderboard_name, member, **options):
    reverse_rank_for_member = None
    if self.order == self.DESC:
      reverse_rank_for_member = self.redis_connection.zrevrank(leaderboard_name, member)
    else:
      reverse_rank_for_member = self.redis_connection.zrank(leaderboard_name, member)

    if reverse_rank_for_member == None:
      return []

    page_size = options.get('page_size', self.page_size)

    starting_offset = reverse_rank_for_member - (page_size / 2)
    if starting_offset < 0:
      starting_offset = 0

    ending_offset = (starting_offset + page_size) - 1

    raw_leader_data = self._range_method(self.redis_connection, self.leaderboard_name, int(starting_offset), int(ending_offset), withscores = False)

    if raw_leader_data:
      return self.ranked_in_list_in(leaderboard_name, raw_leader_data, **options)
    else:
      return []

  def ranked_in_list(self, members, **options):
    return self.ranked_in_list_in(self.leaderboard_name, members, **options)

  def ranked_in_list_in(self, leaderboard_name, members, **options):
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
      data['member'] = member
      data['rank'] = responses[index * 2] + 1
      data['score'] = float(responses[index * 2 + 1])

      if ('with_member_data' in options) and (True == options['with_member_data']):
        data['member_data'] = self.member_data_for_in(leaderboard_name, member)

      ranks_for_members.append(data)

    # support for sort_by in options

    return ranks_for_members

  def _range_method(self, connection, *args, **kwargs):
    if self.order == self.DESC:
      return connection.zrevrange(*args, **kwargs)
    else:
      return connection.zrange(*args, **kwargs)

  def _member_data_key(self, leaderboard_name):
    return '%s:member_data' % leaderboard_name
