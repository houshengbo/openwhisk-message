
import json
import pykafka
from pykafka.common import OffsetType

#from owmessage.common import MyClass
from owmessage.common import config as cfg

def fire_trigger(message):
    msg = None
    try:
        msg = json.loads(message)
    except ValueError as e:
        print ('the message is not a valid json message')
        raise e
    payload = msg.get("payload")
    trigger = msg.get("trigger")
    version = trigger.get("version", "v1")
    namespace = trigger.get("namespace", "whisk.system")
    trigger_name = trigger.get("name")
    apikey = trigger.get("apikey")
    host = trigger.get("host", "https://172.17.0.1:443")
    uri = host + '/api/' +version+ '/namespaces/' + namespace + '/triggers/' + trigger_name
    # TODO fire the trigger by the uri with the payload
    print ("Fire the trigger %s, the payload is %s." % (trigger_name, payload))

def consume_topic(client, config):
    if config.get("topic") not in client.topics:
        raise ValueError('Topic {} does not exist.'.format(config.get("topic")))
    topic = client.topics[config.get("topic")]
    consumer = topic.get_balanced_consumer(config.get("consumer_group"),
                                           consumer_timeout_ms=100,
                                           auto_offset_reset=OffsetType.EARLIEST,
                                           reset_offset_on_start=False,
                                           auto_commit_enable=True,
                                           auto_commit_interval_ms=3000,
                                           zookeeper_connect=config.get("zookeeper_connect"))
    num_consumed = 0
    while num_consumed < config.get("limit"):
        msg = consumer.consume()
        if not msg or not msg.value:
            continue
        msg = msg.value.decode('utf-8', errors='replace')
        print (msg.encode('utf-8'))
        # Fire the trigger
        fire_trigger(msg.encode('utf-8'))
        num_consumed += 1


def _parse_conf_file(config_file_path):
    CONF = cfg.CONF
    #cf = ConfigParser.ConfigParser()
    #cf.read
    CONF(config_file_path)
    config = {}

    default_section = "default"
    kafka_servers = CONF.get(default_section, "kafka_servers")
    topic = CONF.get(default_section, "topic")
    consumer_group = CONF.get(default_section, "consumer_group")
    zookeeper_connect = CONF.get(default_section, "zookeeper_connect")
    
    config["kafka_servers"] = kafka_servers
    config["topic"] = topic
    config["consumer_group"] = consumer_group
    config["zookeeper_connect"] = zookeeper_connect
    config["limit"] = 100
    
    return config


def main():
    config = _parse_conf_file("consumer.conf")
    client = pykafka.KafkaClient(hosts=config.get("kafka_servers"))
    consume_topic(client, config)
    

if __name__ == '__main__':
    main()
