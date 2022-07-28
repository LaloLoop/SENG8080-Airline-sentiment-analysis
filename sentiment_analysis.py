import json
import configparser
from transformers import pipeline
from elasticsearch import Elasticsearch
from confluent_kafka import Producer, Consumer, KafkaError
import ccloud_lib
from searchtweets import ResultStream, gen_request_parameters

def search_tweets(text_query: str, search_args, results_per_call=10, max_results=10):
    """
    Returns a tweet stream for the provided text query, it can then be iterated to retrieve each individual tweet.
    Args:
        text_query (str): The topic of interest or text to search for.
        search_args (dict): Arguments needed to authenticate.
    Returns:
        a result stream iterable/generator.
    """
    query = gen_request_parameters(text_query, results_per_call=results_per_call, granularity=None, tweet_fields='created_at')
    rs = ResultStream(request_parameters=query, max_results=max_results, max_pages=1, **search_args)
    return rs.stream()

class BaseProducer:
    """Defines the basic connectivity to reach Kafka instance hosted in Confluent Cloud"""
    def __init__(self, config_file, topic):
        """Creates a BaedProducer with the provided configuration and topic
        """

        conf = ccloud_lib.read_ccloud_config(config_file)

        producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
        self.producer = Producer(producer_conf)

        self.topic = topic
        ccloud_lib.create_topic(conf, topic)

        self.delivered_records = 0

    def acked(self, err, msg):
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            self.delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}".format(msg.topic(), msg.partition(), msg.offset()))

    def produce(self, values):
        for v in values:
            record_key = str(v['id'])
            record_value = json.dumps(v)
            
            print("Producing record: {}: {}".format(record_key, record_value))
            
            self.producer.produce(self.topic, key=record_key, value=record_value, on_delivery=self.acked)
            # p.poll() serves delivery reports (on_delivery)
            # from previous produce() calls.
            self.producer.poll(0)

        self.producer.flush()

        print("{} messages were produced to topic {}!".format(self.delivered_records, self.topic))


class RobertaClassifier:
    def __init__(self):

        model_path = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
        self.classifier = pipeline("sentiment-analysis", model=model_path, tokenizer=model_path)

    def classify(self, text):
        rbt_result = self.classifier(text)[0]

        return rbt_result

class ClassifierConsumer:
    def __init__(self, topic, output_topic, config_file):
        self.topic = topic
        self.output_topic = output_topic

        self.total_count = 0
        
        # Dependency configuration
        self.producer = BaseProducer(config_file, self.output_topic)
        self.classifier = RobertaClassifier()

        # Consumer configuration
        conf = ccloud_lib.read_ccloud_config(config_file)

        consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)

        consumer_conf['group.id'] = 'notebook_classifier_cg'
        consumer_conf['auto.offset.reset'] = 'earliest'

        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe([self.topic])

    def consume(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    # No message available within timeout.
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    print("Waiting for message or event/error in poll()")
                    continue

                elif msg.error():
                    print('error: {}'.format(msg.error()))

                else:
                    # Check for Kafka message
                    record_key = msg.key()
                    record_value = msg.value()
                    tweet_data = json.loads(record_value)

                    class_result = self.classifier.classify(tweet_data['text'])
                    sentiment = class_result['label']
                    score = class_result['score']

                    tweet_classified = {**tweet_data, 'classification': {'sentiment': sentiment, 'score': score}}
                    
                    self.producer.produce([tweet_classified])

                    print("Consumed record with key {} and ID {}".format(record_key, tweet_data['id']))

        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            self.consumer.close()

class ElasticClient:
    def __init__(self, index_name, config_file):
        self.index_name = index_name

        config = configparser.ConfigParser()
        config.read(config_file)

        self.es = Elasticsearch(
            cloud_id=config['ELASTIC']['cloud_id'],
            basic_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
        )

        self.es.info()

    def add_to_index(self, document: dict):
        result = self.es.index(
            index=self.index_name,
            document=document
        )

        self.es.indices.refresh(index=self.index_name)

        return result
        

class BaseConsumer:
    """Generalization to read Tweets data from any provided topic"""
    def __init__(self, topic, config_file, consumer_name):
        self.topic = topic

        # Consumer configuration
        conf = ccloud_lib.read_ccloud_config(config_file)

        consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)

        consumer_conf['group.id'] = consumer_name
        consumer_conf['auto.offset.reset'] = 'earliest'

        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe([self.topic])

    def consume(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    # No message available within timeout.
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    print("Waiting for message or event/error in poll()")
                    continue

                elif msg.error():
                    print('error: {}'.format(msg.error()))

                else:
                    # Check for Kafka message
                    record_key = msg.key()
                    record_value = msg.value()
                    tweet_data = json.loads(record_value)

                    yield tweet_data

                    print("Consumed record with key {} and ID {}".format(record_key, tweet_data['id']))

        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            self.consumer.close()

class ElasticSink:
    def __init__(self, consumer: BaseConsumer, elastic_client: ElasticClient):
        self.consumer = consumer
        self.es = elastic_client

    def sink(self):
        for t in self.consumer.consume():
            self.es.add_to_index(t)