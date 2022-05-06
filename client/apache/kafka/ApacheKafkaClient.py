from threading import Thread
from kafka import KafkaConsumer
from kafka import KafkaProducer
from client.apache.kafka.ApacheKafkaUtil import ApacheKafkaUtil


class ApacheKafkaClient(object):
    def __init__(self, server):
        self.__server = server if isinstance(server, list) else [server]

    def list(self, stream_path):
        if ApacheKafkaUtil.is_empty(self.__server) or ApacheKafkaUtil.is_empty(stream_path):
            return None
        stream_path = ApacheKafkaUtil.clean_path(stream_path)
        bootstrap_servers = ApacheKafkaUtil.join(',', *self.__server)
        messages = KafkaConsumer(stream_path, bootstrap_servers=bootstrap_servers, enable_auto_commit=False)
        messages = [ApacheKafkaUtil.concat(message.topic, '/', message.partition, '/', message.key)
                    for message in messages]
        result = ApacheKafkaUtil.to_set(messages)
        return result

    def delete(self, stream_path):
        if ApacheKafkaUtil.is_empty(self.__server) or ApacheKafkaUtil.is_empty(stream_path):
            return False
        stream_path = ApacheKafkaUtil.clean_path(stream_path)
        bootstrap_servers = ApacheKafkaUtil.join(',', *self.__server)
        _ = KafkaConsumer(stream_path, bootstrap_servers=bootstrap_servers, enable_auto_commit=True)
        return True

    def exists(self, stream_path):
        if ApacheKafkaUtil.is_empty(self.__server) or ApacheKafkaUtil.is_empty(stream_path):
            return False
        stream_path = ApacheKafkaUtil.clean_path(stream_path)
        bootstrap_servers = ApacheKafkaUtil.join(',', *self.__server)
        messages = KafkaConsumer(stream_path, bootstrap_servers=bootstrap_servers, enable_auto_commit=False)
        result = len(messages) != 0
        return result

    def count(self, stream_path):
        if ApacheKafkaUtil.is_empty(self.__server) or ApacheKafkaUtil.is_empty(stream_path):
            return None
        stream_path = ApacheKafkaUtil.clean_path(stream_path)
        bootstrap_servers = ApacheKafkaUtil.join(',', *self.__server)
        messages = KafkaConsumer(stream_path, bootstrap_servers=bootstrap_servers, enable_auto_commit=False)
        result = len(messages)
        return result

    def produce(self, stream_path, content):
        if ApacheKafkaUtil.is_empty(self.__server) or ApacheKafkaUtil.is_empty(stream_path):
            return False
        stream_path = ApacheKafkaUtil.clean_path(stream_path)
        bootstrap_servers = ApacheKafkaUtil.join(',', *self.__server)
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        producer.send(stream_path, content)
        return True

    def consume(self, stream_path, callback, asynchronous=True):
        if ApacheKafkaUtil.is_empty(self.__server) or ApacheKafkaUtil.is_empty(stream_path):
            return False
        stream_path = ApacheKafkaUtil.clean_path(stream_path)
        bootstrap_servers = ApacheKafkaUtil.join(',', *self.__server)
        messages = KafkaConsumer(stream_path, bootstrap_servers=bootstrap_servers)
        if asynchronous:
            for message in messages:
                message = ApacheKafkaUtil.to_tuple(message.value)
                new_thread = Thread(target=callback, args=message)
                new_thread.start()
        else:
            for message in messages:
                message = message.value
                callback(message)
        return True
