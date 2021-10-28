from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("WorstMovies").getOrCreate()
    
    df1 = spark.read.load("hdfs:///user/maria_dev/ml-latest-small/ratings.csv",
                            format="csv", sep=",", inferSchema="true", header="true")
    df2 = spark.read.load("hdfs:///user/maria_dev/ml-latest-small/movies.csv",
                            format="csv", sep=",", inferSchema="true", header="true")

    df1.createOrReplaceTempView("ratings")
    df2.createOrReplaceTempView("movies")

    result = spark.sql("""
        SELECT title, score, cnt
        FROM movies JOIN (
            SELECT movieId, avg(rating) as score, count(rating) as cnt
            FROM ratings GROUP BY movieId
        ) r ON movies.movieId = r.movieId
        WHERE score < 2.0
        ORDER BY cnt DESC 
        LIMIT 30
        """)

    
    for row in result.collect():
        print(row.title, row.score, row.cnt)