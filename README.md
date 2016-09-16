# leaderboard

Leaderboards backed by [Redis](http://redis.io) in Python.

Builds off ideas proposed in http://www.agoragames.com/blog/2011/01/01/creating-high-score-tables-leaderboards-using-redis/.

## Installation

`pip install leaderboard`

Make sure your redis server is running! Redis configuration is outside the scope of this README, but
check out the [Redis documentation](http://redis.io/documentation).

## Usage

### Creating a leaderboard

Be sure to require the leaderboard library:

```python
from leaderboard.leaderboard import Leaderboard
```

Create a new leaderboard or attach to an existing leaderboard named 'highscores':

```python
highscore_lb = Leaderboard('highscores')
```

### Defining leaderboard options

The default options are as follows:

```python
    DEFAULT_PAGE_SIZE = 25
    DEFAULT_REDIS_HOST = 'localhost'
    DEFAULT_REDIS_PORT = 6379
    DEFAULT_REDIS_DB = 0
    DEFAULT_MEMBER_DATA_NAMESPACE = 'member_data'
    DEFAULT_GLOBAL_MEMBER_DATA = False
    ASC = 'asc'
    DESC = 'desc'
    MEMBER_KEY = 'member'
    MEMBER_DATA_KEY = 'member_data'
    SCORE_KEY = 'score'
    RANK_KEY = 'rank'
```

You would use the option, `order=Leaderboard.ASC`, if you wanted a leaderboard sorted from lowest-to-highest score. You may also set the `order` option on a leaderboard after you have created a new instance of a leaderboard. The various `..._KEY` options above control what data is returned in the hash of leaderboard data from calls such as `leaders` or `around_me`. Finally, the `global_member_data` option allows you to control whether optional member data is per-leaderboard (`False`) or global (`True`).

### Ranking members in the leaderboard

Add members to your leaderboard using `rank_member`:

```python
for index in range(1, 11):
  highscore_lb.rank_member('member_%s' % index, index)
```

You can call `rank_member` with the same member and the leaderboard will be updated automatically.

Get some information about your leaderboard:

```python
highscore_lb.total_members()
10

highscore_lb.total_pages()
1
```

Get some information about a specific member(s) in the leaderboard:

```python
highscore_lb.score_for('member_4')
4.0

highscore_lb.rank_for('member_4')
7

highscore_lb.rank_for('member_10')
1
```

### Retrieving members from the leaderboard

Get page 1 in the leaderboard:

```python
highscore_lb.leaders(1)

[{'member': 'member_10', 'score': 10.0, 'rank': 1}, {'member': 'member_9', 'score': 9.0, 'rank': 2}, {'member': 'member_8', 'score': 8.0, 'rank': 3}, {'member': 'member_7', 'score': 7.0, 'rank': 4}, {'member': 'member_6', 'score': 6.0, 'rank': 5}, {'member': 'member_5', 'score': 5.0, 'rank': 6}, {'member': 'member_4', 'score': 4.0, 'rank': 7}, {'member': 'member_3', 'score': 3.0, 'rank': 8}, {'member': 'member_2', 'score': 2.0, 'rank': 9}, {'member': 'member_1', 'score': 1.0, 'rank': 10}]
```

Add more members to your leaderboard:

```python
for index in range(50, 96):
  highscore_lb.rank_member('member_%s' % index, index)

highscore_lb.total_pages()
3
```

Get an "Around Me" leaderboard page for a given member, which pulls members above and below the given member:

```python
highscore_lb.around_me('member_53')

[{'member': 'member_65', 'score': 65.0, 'rank': 31}, {'member': 'member_64', 'score': 64.0, 'rank': 32}, {'member': 'member_63', 'score': 63.0, 'rank': 33}, {'member': 'member_62', 'score': 62.0, 'rank': 34}, {'member': 'member_61', 'score': 61.0, 'rank': 35}, {'member': 'member_60', 'score': 60.0, 'rank': 36}, {'member': 'member_59', 'score': 59.0, 'rank': 37}, {'member': 'member_58', 'score': 58.0, 'rank': 38}, {'member': 'member_57', 'score': 57.0, 'rank': 39}, {'member': 'member_56', 'score': 56.0, 'rank': 40}, {'member': 'member_55', 'score': 55.0, 'rank': 41}, {'member': 'member_54', 'score': 54.0, 'rank': 42}, {'member': 'member_53', 'score': 53.0, 'rank': 43}, {'member': 'member_52', 'score': 52.0, 'rank': 44}, {'member': 'member_51', 'score': 51.0, 'rank': 45}, {'member': 'member_50', 'score': 50.0, 'rank': 46}, {'member': 'member_10', 'score': 10.0, 'rank': 47}, {'member': 'member_9', 'score': 9.0, 'rank': 48}, {'member': 'member_8', 'score': 8.0, 'rank': 49}, {'member': 'member_7', 'score': 7.0, 'rank': 50}, {'member': 'member_6', 'score': 6.0, 'rank': 51}, {'member': 'member_5', 'score': 5.0, 'rank': 52}, {'member': 'member_4', 'score': 4.0, 'rank': 53}, {'member': 'member_3', 'score': 3.0, 'rank': 54}, {'member': 'member_2', 'score': 2.0, 'rank': 55}]
```

Get rank and score for an arbitrary list of members (e.g. friends) from the leaderboard:

```python
highscore_lb.ranked_in_list(['member_1', 'member_62', 'member_67'])

[{'member': 'member_1', 'score': 1.0, 'rank': 56}, {'member': 'member_62', 'score': 62.0, 'rank': 34}, {'member': 'member_67', 'score': 67.0, 'rank': 29}]
```

Retrieve members from the leaderboard in a given score range:

```python
highscore_lb.members_from_score_range(4, 19)

[{'member': 'member_10', 'score': 10.0, 'rank': 47}, {'member': 'member_9', 'score': 9.0, 'rank': 48}, {'member': 'member_8', 'score': 8.0, 'rank': 49}, {'member': 'member_7', 'score': 7.0, 'rank': 50}, {'member': 'member_6', 'score': 6.0, 'rank': 51}, {'member': 'member_5', 'score': 5.0, 'rank': 52}, {'member': 'member_4', 'score': 4.0, 'rank': 53}]
```

Retrieve a single member from the leaderboard at a given position:

```python
highscore_lb.member_at(4)

{'member': 'member_92', 'score': 92.0, 'rank': 4}
```

Retrieve a range of members from the leaderboard within a given rank range:

```python
highscore_lb.members_from_rank_range(1, 5)

[{'member': 'member_95', 'score': 95.0, 'rank': 1}, {'member': 'member_94', 'score': 94.0, 'rank': 2}, {'member': 'member_93', 'score': 93.0, 'rank': 3}, {'member': 'member_92', 'score': 92.0, 'rank': 4}, {'member': 'member_91', 'score': 91.0, 'rank': 5}]
```

#### Optional member data notes

If you use optional member data, the use of the `remove_members_in_score_range` or `remove_members_outside_rank` methods
will leave data around in the member data hash. This is because the internal Redis method, `zremrangebyscore`,
only returns the number of items removed. It does not return the members that it removed.

#### Leaderboard request options

You can pass various options to the calls `leaders`, `all_leaders`, `around_me`, `members_from_score_range`, `members_from_rank_range` and `ranked_in_list`. Valid options are:

* `with_member_data` - `true` or `false` to return the optional member data.
* `page_size` - An integer value to change the page size for that call.
* `members_only` - `true` or `false` to return only the members without their score and rank.
* `sort_by` - Valid values for `sort_by` are `score` and `rank`.

### Conditionally rank a member in the leaderboard

You can pass a function to the `rank_member_if` method to conditionally rank a member in the leaderboard. The function is passed the following 5 parameters:

* `member`: Member name.
* `current_score`: Current score for the member in the leaderboard. May be `nil` if the member is not currently ranked in the leaderboard.
* `score`: Member score.
* `member_data`: Optional member data.
* `leaderboard_options`: Leaderboard options, e.g. 'reverse': Value of reverse option

```python
def highscore_check(self, member, current_score, score, member_data, leaderboard_options):
  if (current_score is None):
    return True
  if (score > current_score):
    return True
  return False

highscore_lb.rank_member_if(highscore_check, 'david', 1337)
highscore_lb.score_for('david')

1337.0

highscore_lb.rank_member_if(highscore_check, 'david', 1336)
highscore_lb.score_for('david')

1337.0

highscore_lb.rank_member_if(highscore_check, 'david', 1338)
highscore_lb.score_for('david')

1338.0
```

### Ranking a member across multiple leaderboards

```python
highscore_lb.rank_member_across(['highscores', 'more_highscores'], 'david', 50000, { 'member_name': 'david' })
```

### Alternate leaderboard types

The leaderboard library offers 3 styles of ranking. This is only an issue for members with the same score in a leaderboard.

Default: The `Leaderboard` class uses the default Redis sorted set ordering, whereby different members having the same score are ordered lexicographically. As per the Redis documentation on Redis sorted sets, "The lexicographic ordering used is binary, it compares strings as array of bytes."

Tie ranking: The `TieRankingLeaderboard` subclass of `Leaderboard` allows you to define a leaderboard where members with the same score are given the same rank. For example, members in a leaderboard with the associated scores would have the ranks of:

```
| member     | score | rank |
-----------------------------
| member_1   | 50    | 1    |
| member_2   | 50    | 1    |
| member_3   | 30    | 2    |
| member_4   | 30    | 2    |
| member_5   | 10    | 3    |
```

The `TieRankingLeaderboard` accepts one additional option, `ties_namespace` (default: ties), when initializing a new instance of this class. Please note that in its current implementation, the `TieRankingLeaderboard` class uses an additional sorted set to rank the scores, so please keep this in mind when you are doing any capacity planning for Redis with respect to memory usage.

Competition ranking: The `CompetitionRankingLeaderboard` subclass of `Leaderboard` allows you to define a leaderboard where members with the same score will have the same rank, and then a gap is left in the ranking numbers. For example, members in a leaderboard with the associated scores would have the ranks of:

```
| member     | score | rank |
-----------------------------
| member_1   | 50    | 1    |
| member_2   | 50    | 1    |
| member_3   | 30    | 3    |
| member_4   | 30    | 3    |
| member_5   | 10    | 5    |
```

## Performance Metrics

You can view [performance metrics](https://github.com/agoragames/leaderboard#performance-metrics) for the
leaderboard library at the original Ruby library's page.

## Ports

The following ports have been made of the [leaderboard gem](https://github.com/agoragames/leaderboard).

Officially supported:

* CoffeeScript: https://github.com/agoragames/leaderboard-coffeescript
* Python: https://github.com/agoragames/leaderboard-python
* Ruby: https://github.com/agoragames/leaderboard

Unofficially supported (they need some feature parity love):

* Java: https://github.com/agoragames/java-leaderboard
* PHP: https://github.com/agoragames/php-leaderboard
* Scala: https://github.com/agoragames/scala-leaderboard

## Contributing to leaderboard

* Check out the latest master to make sure the feature hasn't been implemented or the bug hasn't been fixed yet
* Check out the issue tracker to make sure someone already hasn't requested it and/or contributed it
* Fork the project
* Start a feature/bugfix branch
* Commit and push until you are happy with your contribution
* Make sure to add tests for it. This is important so I don't break it in a future version unintentionally.
* Please try not to mess with the version or history. If you want to have your own version, or is otherwise necessary, that is fine, but please isolate to its own commit so I can cherry-pick around it.

## Copyright

Copyright (c) 2011-2016 Ola Mork, David Czarnecki. See LICENSE.txt for further details.

