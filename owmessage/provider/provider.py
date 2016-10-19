
import json
import pykafka
from pykafka.common import OffsetType

#from owmessage.common import MyClass
from owmessage.common import config as cfg

def generate_message(config):
    message = {}
    payload = {}
    # TODO generate the valid message with the trigger information, payload and the api key
    message['id'] = "random_id"
    message['event_source'] = "object storage"
    message['event_type'] = "create"
    message['trigger'] = config.get('trigger')
    payload['content'] = 'This is the payload content.'
    message['payload'] = payload
    message['timestamp'] = 'time'
    message = json.dumps(message)
    return message

def send_msg(client, config):
    if config.get("topic") not in client.topics:
        raise ValueError('Topic {} does not exist.'.format(config.get("topic")))
    topic = client.topics[config.get("topic")]
    msg = generate_message(config)
    with topic.get_sync_producer() as producer:
        print ('send message to the kafka server: %s' % msg)
        producer.produce(msg)

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
    
    trigger_section = "trigger"
    name = CONF.get(trigger_section, "name")
    namespace = CONF.get(trigger_section, "namespace")
    version = CONF.get(trigger_section, "version")
    host = CONF.get(trigger_section, "host")

    trigger = {"name": name, "namespace": namespace,
               "version": version, "host": host}
    config["trigger"] = trigger
    return config


def main():
    config = _parse_conf_file("provider.conf")
    client = pykafka.KafkaClient(hosts=config.get("kafka_servers"))
    send_msg(client, config)
    

if __name__ == '__main__':
    main()
