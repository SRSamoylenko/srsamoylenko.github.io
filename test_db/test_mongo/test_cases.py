import time
from typing import Protocol

from pymongo.database import Database


class TestCase(Protocol):
    @classmethod
    def measure_exec_time(
        cls,
        db: Database,
        movie_id: str,
        user_id: str,
    ) -> float:
        pass


class TestGetUserFavorites(TestCase):
    @staticmethod
    def measure_exec_time(db: Database, movie_id: str, user_id: str) -> float:
        collection = db.estimations
        result = collection.find({"user_id": user_id, "estimation": {"$gt": 7}})
        exec_time_ms = result.explain()["executionStats"]["executionTimeMillis"]
        return exec_time_ms / 1000


class TestCountMovieLikes(TestCase):
    @staticmethod
    def measure_exec_time(db: Database, movie_id: str, user_id: str) -> float:
        start = time.time()
        collection = db.estimations
        collection.count_documents({"movie_id": movie_id, "estimation": {"$gt": 7}})
        return time.time() - start


class TestGetUserPostponed(TestCase):
    @staticmethod
    def measure_exec_time(db: Database, movie_id: str, user_id: str):
        collection = db.postponed
        result = collection.find({"user_id": user_id})
        exec_time_ms = result.explain()["executionStats"]["executionTimeMillis"]
        return exec_time_ms / 1000


class TestGetAverageEstimation(TestCase):
    @staticmethod
    def measure_exec_time(db: Database, movie_id: str, user_id: str):
        collection = db.estimations
        pipeline = [
            {"$match": {"movie_id": "dfacc9be-a1f5-49e4-9536-3ead253b2112"}},
            {"$group": {"_id": "$movie_id", "avg_estimation": {"$avg": "$estimation"}}},
        ]
        start = time.time()
        collection.aggregate(pipeline)
        return time.time() - start


TEST_CASES = {
    "user_favorites": TestGetUserFavorites,
    "movie_likes": TestCountMovieLikes,
    "postpones": TestGetUserPostponed,
    "average_score": TestGetAverageEstimation,
}


def get_test(name: str) -> TestCase:
    return TEST_CASES[name]
