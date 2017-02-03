# CHANGELOG

## 3.7.0 (2017-02-03)

* Fixed an error with the `ranked_in_list` method when using certain options [#46](https://github.com/agoragames/leaderboard-python/pull/46)
* Added `total_scores` method [#45](https://github.com/agoragames/leaderboard-python/pull/45)

## 3.6.1 (2016-10-18)

* Remove `izip_longest` import where unneeded. [#44](https://github.com/agoragames/leaderboard-python/pull/44)

## 3.6.0 (2016-09-16)

* More pipelining as per Ruby library, [#58](https://github.com/agoragames/leaderboard/pull/58).
  Also adds `members_data_for(...) and `members_data_for_in(...)` methods.
* Guard against an out of bounds index in `member_at`

## 3.5.0 (2015-12-11)

* Allow options to be passed down to the connection pool.

## 3.4.0 (2015-06-16)

* Allow a Redis connection to be passed in the Leaderboard initializer using the `redis_connection` option.

## 3.3.0 (2015-03-20)

* Similar fix to one found in the [Ruby leaderboard](https://github.com/agoragames/leaderboard) library. Fixes TieRankingLeaderboard doesn't rank if the score is 0.
* Allow member data to be set in the `change_score_for(...)` method.
* Add `include_missing` option in leaderboard request options to change
  whether or not to include missing members in the result.

## 3.2.0 (2015-02-15)

* Add `global_member_data` option that allows multiple leaderboards to share the same set of member_data. [#51](https://github.com/agoragames/leaderboard/pull/51) for original pull request from the Ruby leaderboard library.
* Add `top` helper method. [#50](https://github.com/agoragames/leaderboard/pull/50) for the original pull request from the Ruby leaderboard library.

## 3.1.0 (2014-11-07)

* Add support for `change_score_for(...)` in the `TieRankingLeaderboard` class.

## 3.0.1 (2014-09-04)

* Add Python3 version check to accommodate 2to3 change of itertools.izip_longest [#30](https://github.com/agoragames/leaderboard-python/pull/30)

## 3.0.0 (2014-07-28)

* Add support for tie handling in leaderboards [#28](https://github.com/agoragames/leaderboard-python/pull/28)
* Re-organized the structure of the leaderboard library so that all the base leaderboard code is no longer in the init.py.

## 2.8.0 (2014-02-15)

* Allow for customization of member_data namespace [#19](https://github.com/agoragames/leaderboard-python/pull/19)

## 2.7.0 (2014-01-24)

* Allow for custom keys to be set for customizing the data returned from calls like `leaders` or `around_me` [#18](https://github.com/agoragames/leaderboard-python/pull/18). Thanks @seaders.

## 2.6.1 (2014-01-22)

* Identify client class and send appropriate zadd command. Resolves [#17](https://github.com/agoragames/leaderboard-python/issues/17).

## 2.6.0 (2013-11-12)

* Added `score_for_percentile` method to be able to calculate the score for a given percentile value in the leaderboard.

## 2.5.0 (2013-07-17)

* Added `rank_member_across` method to be able to rank a member across multiple leaderboards at once.
* Fixed bugs in `leaders_in` and `around_me_in` methods that would not correctly use the `leaderboard_name` argument.

## 2.4.0 (2013-05-31)

* Added `remove_members_outside_rank` method to remove members from the leaderboard outside a given rank.

## 2.3.0 (2013-05-15)

* Added `members_only` option for various leaderboard requests - HT: [Simon Zimmerman](https://github.com/simonz05)
* `leaders` call should return `[]` in case of an empty result set - HT: [Simon Zimmerman](https://github.com/simonz05)
* Initializer no longer deep copies options - HT: [Simon Zimmerman](https://github.com/simonz05)

## 2.2.2 (2013-02-22)

* Fixed a data leak in `expire_leaderboard` and `expire_leaderboard_at` to also set expiration on the member data hash.

## 2.2.1 (2012-12-19)

* Updated `remove_member` to also remove the optional member data for the member being removed.

## 2.2.0 (2012-12-03)

* Added `rank_member_if` and `rank_member_if_in` methods that allow you to rank a member in the leaderboard based on execution of a function.
* Added `rank_members` and `rank_members_in` methods that allow you to rank multiple members in a leaderboard at once by passing in an array of members and scores.

## 2.1 (2012-11-27)

* No longer cast scores to a floating point automatically. If requesting a score for an unknown member in the leaderboard, return `None`. Under the old behavior, a `None` score gets returned as 0.0. This is misleading as 0.0 is a valid score.
* Fixes a bug in `ranked_in_list` when requesting a list with an unknown member.

## 2.0.1 (2012-11-15)

* Remove unnecessary options from `score_and_rank_for` method

## 2.0.0 (2012-11-14)

* Port missing functionality from Ruby [leaderboard](https://github.com/agoragames/leaderboard) project to Python.
* Feature and data parity should exist between these two implementations.