import tweepy
import preprocessor
import string
from nltk.corpus import stopwords
import time
import datetime
from pywebhdfs.webhdfs import PyWebHdfsClient
from tenacity import retry,stop_after_attempt
from kafka import KafkaProducer


@retry(stop=stop_after_attempt(5))
def makedir(hdfs, dir_name):
   try:
      hdfs.make_dir(dir_name)
   except Exception as e:
      print(f"Hdfs mkdir exception for {dir_name}: {str(e)}")


@retry(stop=stop_after_attempt(5))
def putfile(hdfs, data, dest_name):
   try:
      hdfs.create_file(dest_name, data)
   except Exception as e:
      print(f"Hdfs transfer exception from {src_name} to {dest_name}: {str(e)}")


def on_send_success(record_metadata):
    print(f"TOPIC-{record_metadata.topic}")
    print(f"PARTITION-{record_metadata.partition}")
    print(f"OFFSET-{record_metadata.offset}")

def on_send_error(excp):
    #print (type(excp))
    print(f"Kafka error - {excp}")


if __name__ == '__main__':
    consumer_key = 'p49A6YGRbQ7SQ71lUkOEYWTBs'
    consumer_secret = 'tnXmlngw7GsrhMWmOWRILHpMhiL6kZ46eKsDUfrX2s06qoQOYl'
    access_token = '3009962708-HiZKHPsHp6RBjnuWTyy7OjHrN1NNJb0Vacx9k5p'
    access_token_secret = 'PiWWsi0b51K4135QkOLHqvXW0jbZPZvVNqmsPGs6lg9yk'
    
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)
    topic_to_search = "cricket"
    '''
    public_tweets = api.home_timeline()
    for tweet in public_tweets:
        print(tweet.text)
    print(len(public_tweets))
    '''
    search_tweets = api.search_tweets(q=topic_to_search, count=100)
    word_list = []
    for each in search_tweets:
        word_list.extend(preprocessor.clean(each.text).split(" "))
    cachedStopWords = stopwords.words("english")
    refined_wl = [word.translate(str.maketrans('', '', string.punctuation)) for word in word_list]
    refined_wl1 = [word for word in refined_wl if (word not in cachedStopWords) and (word!="") and (word!=" ")]
    refined_wl2 = [word.lower() for word in refined_wl1]
    refined_str = ' '.join(refined_wl2)
    
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
    # Write To HDFS
    hdfs = PyWebHdfsClient(host='127.0.0.1',port=50070, user_name='root')
    makedir(hdfs, f"/user/root/tweet_data/{current_time}")
    putfile(hdfs, refined_str, f"/user/root/tweet_data/{current_time}/data.txt")

    # Send to Kafka
    producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092']) 
    topic = "top_words"
    msg = f"/user/root/tweet_data/{current_time}"
    producer.send(topic, bytes(msg, 'ascii')).add_callback(on_send_success).add_errback(on_send_error)
    producer.flush()
    time.sleep(1)
