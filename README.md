# twitter_top_words

## High Level Design
![HLD](https://github.com/kiwincardoza/twitter_top_words/blob/master/top_words.jpg?raw=true)


## Environment
Spark, Hadoop with YARN and HDFS, Kafka installed in a single machine (Linux/CentOS)

## Dependencies
Use 'requirements.txt' file to install python dependencies using pip

## Cron job
*/5 * * * *  <absolute_path>/read_tweets.py

## Steps
1. Start the Spark application by running the shell script 'top_words.sh'.
2. Logs can be found under the '<root_directory>/logs/' directory and in YARN logs for the Spark application.

## Result
![Result snap](https://github.com/kiwincardoza/twitter_top_words/blob/master/results/result.PNG?raw=true)
