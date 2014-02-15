# CHANGELOG

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