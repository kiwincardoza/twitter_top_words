import tweepy
import preprocessor
import string
from nltk.corpus import stopwords
import time
import datetime
from pywebhdfs.webhdfs import PyWebHdfsClient
from tenacity import retry,stop_after_attempt
from kafka import KafkaProducer
import configparser
import logging
import logging.config
from logging.handlers import RotatingFileHandler
from logging.config import fileConfig

# Read log configuration from config file
fileConfig("/root/standalone_spark/top_words/cfg/log.conf")



@retry(stop=stop_after_attempt(5))
def makedir(hdfs, dir_name):
   '''
   Function to create hdfs directory
   :param hdfs: pywebhdfs object
   :param dir_name: directory to be created
   :return:
   '''
   logger = logging.getLogger("makedir")
   try:
      hdfs.make_dir(dir_name)
      logger.info(f"Hdfs directory {dir_name} successfully created")
   except Exception as e:
      logger.error(f"Hdfs mkdir exception for {dir_name}: {e}")


@retry(stop=stop_after_attempt(5))
def putfile(hdfs, data, dest_name):
   '''
   Function to store data into a new file in HDFS
   :param hdfs: pywebhdfs object
   :param data: string (pre processed)
   :param dest_name: destination file name in HDFS
   :return:
   '''
   logger = logging.getLogger("putfile")
   try:
      hdfs.create_file(dest_name, data)
      logger.info(f"Hdfs transfer to {dest_name} successful")
   except Exception as e:
      logger.error(f"Hdfs transfer exception from {src_name} to {dest_name}: {e}")


def on_send_success(record_metadata):
    '''
    Function which is triggered on successful delivery in Kafka
    :param record_metadata: Kafka message's metadata
    :return:
    '''
    logger = logging.getLogger("on_send_success")
    logger.info(f"TOPIC-{record_metadata.topic}")
    logger.info(f"PARTITION-{record_metadata.partition}")
    logger.info(f"OFFSET-{record_metadata.offset}")

def on_send_error(excp):
    '''
    Function which is triggered on unsuccessful delivery in Kafka
    :param excp: Actual exception object
    :return:
    '''
    logger = logging.getLogger("on_send_error")
    logger.error(f"Kafka error - {excp}")


def get_twitter_authentication(config):
    '''
    Function to authenticate Twitter API calls
    :param config: config object to read auth attributes
    :return: Auth object
    '''
    logger = logging.getLogger("get_twitter_authentication")
    try:
        consumer_key = config['api_auth']['consumer_key']
        consumer_secret = config['api_auth']['consumer_secret']
        access_token = config['api_auth']['access_token']
        access_token_secret = config['api_auth']['access_token_secret']

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        logger.info(f"Twitter API object creation successful")
        return auth
    except Exception as e:
        logger.error(f"Twitter API object creation ERROR: {e}")


def call_api(auth, config):
    '''
    Function to call the required API for the given search query
    :param auth: Auth object
    :param config: Config object
    :return: String returned by calling the API
    '''
    logger = logging.getLogger("call_api")
    try:
        api = tweepy.API(auth)
        topic_to_search = config['api_properties']['topic_to_search']
        search_tweets = api.search_tweets(q=topic_to_search, count=int(config['api_properties']['limit']))
        logger.info(f"API successfully called on topic {topic_to_search}")
        return search_tweets
    except Exception as e:
        logger.error(f"Error in calling the API: {e}")


def pre_process_text(search_tweets):
    '''
    Function to pre process the retrieved text from API
    :param search_tweets: Input string to pre process
    :return: Pre processed string
    '''
    logger = logging.getLogger("pre_process_text")
    try:
        word_list = []
        for each in search_tweets:
            word_list.extend(preprocessor.clean(each.text).split(" "))
        cachedStopWords = stopwords.words("english")   # Get a list of stop words in English
        refined_wl = [word.translate(str.maketrans('', '', string.punctuation)) for word in word_list]  # Remove punctuations from every string
        refined_wl1 = [word for word in refined_wl if (word not in cachedStopWords) and (word!="") and (word!=" ")]   # Remove stop words
        refined_wl2 = [word.lower() for word in refined_wl1]   # Convert each string into lowercase
        refined_str = ' '.join(refined_wl2)
        logger.info(f"Pre processed data successfully")
        return refined_str
    except Exception as e:
        logger.error(f"Error in pre processing fetched data: {e}")

def write_to_hdfs(hdfs_dir, refined_str, config):
    '''
    Function which call create directory and files functions using created pywebhdfs object
    :param hdfs_dir: Directory to be created
    :param refined_str: Content for the new file to be created
    :param config: Config object
    :return:
    '''
    logger = logging.getLogger("write_to_hdfs")
    try:
        hdfs = PyWebHdfsClient(host=config['hdfs']['namenode_ip'], port=int(config['hdfs']['port']), user_name=config['hdfs']['user'])
        makedir(hdfs, hdfs_dir)
        putfile(hdfs, refined_str, f"{hdfs_dir}/sample.txt")
        logger.info(f"Make and copy to hdfs directory successful")
    except Exception as e:
        logger.error(f"Make and copy to hdfs directory Error: {e}")

def kafka_produce(msg, config):
    '''
    Function to produce the message to topic
    :param msg: Message to be produced
    :param config: Config object
    :return:
    '''
    logger = logging.getLogger("kafka_produce")
    try:
        producer = KafkaProducer(bootstrap_servers=config['kafka']['broker_list'].split(','))
        topic = config['kafka']['topic']
        producer.send(topic, bytes(msg, 'ascii')).add_callback(on_send_success).add_errback(on_send_error)
        producer.flush()
    except Exception as e:
        logger.error(f"Error in producing message to topic: {e}")

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    try:
        config = configparser.ConfigParser()
        config.read("/root/standalone_spark/top_words/cfg/main.cfg")    # Load config from file
        logger.info("Config file loaded")
        auth = get_twitter_authentication(config)    # To authenticate API calls
        logger.info("Twitter API object created")
        search_tweets = call_api(auth, config)    # To call API with search query
        logger.info("Twitter API called using search query")
        refined_str = pre_process_text(search_tweets)    # To pre process the retrieved text
        logger.info("Pre processed result text")
        '''
        f = open(f"tmp_dir/{current_time}.txt", "w")
        f.write(f"{refined_str}\n")
        f.close()
    
        write_to_local_tmp_file(refined_str)
        '''

        current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        # Write To HDFS
        write_to_hdfs(f"/user/root/tweet_data/{current_time}", refined_str, config)    # To create directory and file in HDFS

        # Send to Kafka
        kafka_produce(f"/user/root/tweet_data/{current_time}", config)    # To produce message to Kafka topic
        logger.info("End of main function")
        time.sleep(1)
    except Exception as e:
        logger.error(f"Exception in main(): {e}")
