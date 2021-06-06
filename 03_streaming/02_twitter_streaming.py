import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: 02_twitter_streaming.py <hostname> <port>",
            file=sys.stderr
        )
        sys.exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    spark = SparkSession \
        .builder \
        .appName('twitter-streaming') \
        .getOrCreate()

    tweets = spark \
        .readStream \
        .format('socket') \
        .option('host', host) \
        .option('port', port) \
        .load()

    hashtags = tweets.select(
        explode(
            split(tweets.value, ' ')
        ).alias('hashtag')
    ).filter(col('hashtag').startswith('#'))

    hashtag_counts = hashtags \
        .groupBy('hashtag') \
        .count() \
        .orderBy(col('count').desc()) \

    query = hashtag_counts \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .start()

    query.awaitTermination()
