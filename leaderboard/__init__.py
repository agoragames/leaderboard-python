from redis import Redis, ConnectionPool
from copy import deepcopy
from math import ceil

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
