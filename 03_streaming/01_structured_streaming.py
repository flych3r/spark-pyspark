#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

r"""
Counts words in UTF8 encoded, '\n' delimited text received from the network.
Usage: structured_network_wordcount.py <hostname> <port>
<hostname> and <port> describe the TCP server that Structured Streaming
would connect to receive data.

To run this on your local machine, you need to first run a Netcat server
`$ nc -lk -p 9999`
and then run the example
`$ python 01_structured_network_streaming.py localhost 9999

$ nc -lk 9999   | $ python 01_structured_network_streaming.py localhost 9999
apache spark    |
apache hadoop   | -------------------------------------------
                | Batch: 0
                | -------------------------------------------
                | +------+-----+
                | | value|count|
                | +------+-----+
                | +------+-----+
                |
                | -------------------------------------------
                | Batch: 1
                | -------------------------------------------
                | +------+-----+
                | | value|count|
                | +------+-----+
                | |apache|    1|
                | | spark|    1|
                | +------+-----+
                |
                | -------------------------------------------
                | Batch: 2
                | -------------------------------------------
                | +------+-----+
                | | value|count|
                | +------+-----+
                | |apache|    2|
                | | spark|    1|
                | |hadoop|    1|
                | +------+-----+
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: 01_structured_network_streaming.py <hostname> <port>",
            file=sys.stderr
        )
        sys.exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    spark = SparkSession \
        .builder \
        .appName('structured-streaming') \
        .getOrCreate()

    # Create DataFrame representing the stream of input lines
    # from connection to host:port
    lines = spark\
        .readStream\
        .format('socket')\
        .option('host', host)\
        .option('port', port)\
        .load()

    # Split the lines into words
    words = lines.select(
        # explode turns each item in an array into a separate row
        explode(
            split(lines.value, ' ')
        ).alias('word')
    )

    # Generate running word count
    word_counts = words.groupBy('word').count()

    # Start running the query that prints the running counts to the console
    query = word_counts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()

    query.awaitTermination()
