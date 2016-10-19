This project provides a general messaging service for OpenWhisk by using Apache Kafka.
It is a proof-of-concept for the consumer and provider model to be applied in the event driven model.
The consumer service is responsible for listening to the Kafka service, receiving the message
from a specific topic and firing the OpenWhisk trigger by calling the URL.
The provider service(producer service) is responsible for sending the message with the trigger information,
authentication, and the payload for the final action to the Kafka service. In addition, it also provides
different templates to monitor different event sources, which may generate the events. For example,
object storage, database, etc.