# CHANGELOG

## 2.1 (2012-11-27)

* No longer cast scores to a floating point automatically. If requesting a score for an unknown member in the leaderboard, return `None`. Under the old behavior, a `None` score gets returned as 0.0. This is misleading as 0.0 is a valid score.
* Fixes a bug in `ranked_in_list` when requesting a list with an unknown member.

## 2.0.1 (2012-11-15)

* Remove unnecessary options from `score_and_rank_for` method

## 2.0.0 (2012-11-14)

* Port missing functionality from Ruby [leaderboard](https://github.com/agoragames/leaderboard) project to Python.
* Feature and data parity should exist between these two implementations.