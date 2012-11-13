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
    pipeline.execute()

  def rank_member(self, member, score, member_data = None):
    self.rank_member_in(self.leaderboard_name, member, score, member_data)

  def rank_member_in(self, leaderboard_name, member, score, member_data):
    pipeline = self.redis_connection.pipeline()
    pipeline.zadd(leaderboard_name, member, score)
    pipeline.execute()

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

  def change_score_for(self, member, delta):
    self.change_score_for_member_in(self.leaderboard_name, member, delta)

  def change_score_for_member_in(self, leaderboard_name, member, delta):
    self.redis_connection.zincrby(leaderboard_name, member, delta)
