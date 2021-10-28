# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext   
from itertools import islice
import csv

def loadMovies():
    movies = {}
    with open("/home/maria_dev/data/ml-latest-small/movies.csv", "rb") as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for row in reader:
            movies[int(row[0])] = row[1]
    return movies

def parseInput(line):
    fields = line.split(',')
    return (int(fields[1]), (float(fields[2]), 1.0))


if __name__ == "__main__":
    movies = loadMovies()
    path = "hdfs:///user/maria_dev/ml-latest-small/ratings.csv"

    # create spark context
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)

    # create RDD from text file
    lines = sc.textFile(path)

    # skip header
    lines = lines.mapPartitionsWithIndex(
        lambda idx, it: islice(it, 1, None) if idx == 0 else it
    )

    # line --> (movieId, (rating, 1.0))
    ratings = lines.map(parseInput)

    # reduce to (movieId, (sumOfRating, countRating))
    sumAndCounts = ratings.reduceByKey(lambda m1, m2: (m1[0] + m2[0], m1[1] + m2[1]))

    # sumAndCount --> (movieId, (averageRating, countRating))
    avgRatings = sumAndCounts.mapValues(lambda v: (v[0]/v[1], v[1]))

    # 평균 평점 2.0 이하인 영화만
    avgFilter = avgRatings.filter(lambda v: v[1][0] < 2.0)

    # sort
    sortedMovies = avgFilter.sortBy(lambda v: v[1][1],ascending=False)

    # top 30
    results = sortedMovies.take(30)

    for result in results:
        print(movies[result[0]], result[1][0], result[1][1])