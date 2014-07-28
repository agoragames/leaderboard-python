import unittest
from leaderboard_test import LeaderboardTest
from tie_ranking_leaderboard_test import TieRankingLeaderboardTest
from competition_ranking_leaderboard_test import CompetitionRankingLeaderboardTest
from reverse_tie_ranking_leaderboard_test import ReverseTieRankingLeaderboardTest
from reverse_competition_ranking_leaderboard_test import ReverseCompetitionRankingLeaderboardTest


def all_tests():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(LeaderboardTest))
    suite.addTest(unittest.makeSuite(TieRankingLeaderboardTest))
    suite.addTest(unittest.makeSuite(ReverseTieRankingLeaderboardTest))
    suite.addTest(unittest.makeSuite(CompetitionRankingLeaderboardTest))
    suite.addTest(unittest.makeSuite(ReverseCompetitionRankingLeaderboardTest))
    return suite
