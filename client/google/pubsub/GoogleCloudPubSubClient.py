from google.cloud import pubsub_v1
from client.google.util.GoogleCloudPubSubUtil import GoogleCloudPubSubUtil
from client.google.util.GoogleCloudPlataformUtil import GoogleCloudPlataformUtil


class GoogleCloudPubSubClient(object):
    def __init__(self):
        self.__publisher = pubsub_v1.PublisherClient()
        self.__subscriber = pubsub_v1.SubscriberClient()

    def list(self, stream_path):
        if GoogleCloudPubSubUtil.is_empty(stream_path):
            raise ValueError('Stream path cannot be empty')

        stream_path = GoogleCloudPubSubUtil.clean(stream_path)
        topic = GoogleCloudPubSubUtil.build_topic(stream_path)

        self.__create_topic(topic)
        subscription = self.__create_subscription(topic)
        result = self.__pull(subscription)
        result = [str(item.ack_id) for item in result.received_messages]
        result = [topic + '/' + item for item in result]
        self.__delete_subscription(subscription)

        return result

    def count(self, stream_path):
        if GoogleCloudPubSubUtil.is_empty(stream_path):
            raise ValueError('Stream path cannot be empty')

        stream_path = GoogleCloudPubSubUtil.clean(stream_path)
        topic = GoogleCloudPubSubUtil.build_topic(stream_path)

        self.__create_topic(topic)
        subscription = self.__create_subscription(topic)
        result = self.__pull(subscription)
        result = [str(item.ack_id) for item in result.received_messages]
        result = [topic + '/' + item for item in result]
        result = len(result)
        self.__delete_subscription(subscription)

        return result

    def delete(self, stream_path):
        if GoogleCloudPubSubUtil.is_empty(stream_path):
            raise ValueError('Stream path cannot be empty')

        stream_path = GoogleCloudPubSubUtil.clean(stream_path)
        topic = GoogleCloudPubSubUtil.build_topic(stream_path)
        message_id = GoogleCloudPubSubUtil.build_id(stream_path)

        if GoogleCloudPubSubUtil.is_empty(message_id):
            self.__delete_topic(topic)
        else:
            subscription = self.__create_subscription(topic)
            self.__acknowledge(subscription, message_id)
            self.__delete_subscription(subscription)

        return True

    def exists(self, stream_path):
        if GoogleCloudPubSubUtil.is_empty(stream_path):
            raise ValueError('Stream path cannot be empty')

        stream_path = GoogleCloudPubSubUtil.clean(stream_path)
        topic = GoogleCloudPubSubUtil.build_topic(stream_path)
        message_id = GoogleCloudPubSubUtil.build_id(stream_path)

        if GoogleCloudPubSubUtil.is_empty(message_id):
            result = self.__topic_exists(topic)
        else:
            subscription = self.__create_subscription(topic)
            response = self.__pull(subscription)
            ack_ids = [str(item.ack_id) for item in response.received_messages]
            result = True if message_id in ack_ids else False
            self.__delete_subscription(subscription)

        return result

    def publish(self, stream_path, message, asynchronous=True, callback=None):
        if GoogleCloudPubSubUtil.is_empty(stream_path):
            raise ValueError('Stream path cannot be empty')

        if GoogleCloudPubSubUtil.is_empty(message):
            raise ValueError('Message cannot be empty')

        topic = GoogleCloudPubSubUtil.build_topic(stream_path)
        message = str(message).encode('utf-8')
        self.__create_topic(topic)
        future = self.__publisher.publish(topic, message)

        if callback is not None:
            future.add_done_callback(callback)
        if not asynchronous:
            future.result()

        return True

    def subscribe(self, stream_path, acknowledge=True):
        if GoogleCloudPubSubUtil.is_empty(stream_path):
            raise ValueError('Stream path cannot be empty')

        topic = GoogleCloudPubSubUtil.build_topic(stream_path)
        self.__create_topic(topic)
        subscription = self.__create_subscription(topic)
        response = self.__pull(subscription)
        result = [str(item.message.data) for item in response.received_messages]

        if acknowledge:
            ack_ids = [item.ack_id for item in response.received_messages]
            self.__acknowledge(subscription, ack_ids)

        return result

    def __create_topic(self, topic):
        if self.__topic_exists(topic):
            return topic
        name = 'projects/{0}/topics/{1}'.format(
            GoogleCloudPlataformUtil.get_project_id(),
            GoogleCloudPubSubUtil.remove_invalid_characters(topic)
        )
        self.__publisher.create_topic(name=name)
        while True:  # check if already create
            if self.__topic_exists(topic):
                break
        return topic

    def __create_subscription(self, topic):
        while True:  # check if already create
            subscription = GoogleCloudPubSubUtil.generate_uuid4()
            if not self.__subscription_exists(subscription):
                break
        name = 'projects/{0}/subscriptions/{1}'.format(
            GoogleCloudPlataformUtil.get_project_id(),
            GoogleCloudPubSubUtil.remove_invalid_characters(subscription)
        )
        topic = 'projects/{0}/topics/{1}'.format(
            GoogleCloudPlataformUtil.get_project_id(),
            GoogleCloudPubSubUtil.remove_invalid_characters(topic)
        )
        self.__subscriber.create_subscription(name=name, topic=topic)
        while True:  # check if already create
            if self.__subscription_exists(subscription):
                break
        return subscription

    def __delete_subscription(self, subscription):
        if not self.__subscription_exists(subscription):
            return subscription
        name = 'projects/{0}/subscriptions/{1}'.format(
            GoogleCloudPlataformUtil.get_project_id(),
            GoogleCloudPubSubUtil.remove_invalid_characters(subscription)
        )
        self.__subscriber.delete_subscription(subscription=name)
        return subscription

    def __delete_topic(self, topic):
        if not self.__topic_exists(topic):
            return topic
        name = 'projects/{0}/topic/{1}'.format(
            GoogleCloudPlataformUtil.get_project_id(),
            GoogleCloudPubSubUtil.remove_invalid_characters(topic)
        )
        self.__publisher.delete_topic(topic=name)
        return topic

    def __acknowledge(self, subscription, ack_ids):
        name = 'projects/{0}/subscriptions/{1}'.format(
            GoogleCloudPlataformUtil.get_project_id(),
            GoogleCloudPubSubUtil.remove_invalid_characters(subscription)
        )
        ack_ids = ack_ids if isinstance(ack_ids, list) else [ack_ids]
        self.__subscriber.acknowledge(subscription=name, ack_ids=ack_ids)
        return True

    def __topic_exists(self, topic):
        project = 'projects/{0}'.format(GoogleCloudPlataformUtil.get_project_id())
        name = 'projects/{0}/topics/{1}'.format(
            GoogleCloudPlataformUtil.get_project_id(),
            GoogleCloudPubSubUtil.remove_invalid_characters(topic)
        )
        topics = self.__publisher.list_topics(project=project)
        topics = [str(item.name) for item in topics]
        exist = name in topics
        return exist

    def __pull(self, subscription):
        subscription = 'projects/{0}/subscriptions/{1}'.format(
            GoogleCloudPlataformUtil.get_project_id(),
            GoogleCloudPubSubUtil.remove_invalid_characters(subscription)
        )
        result = self.__subscriber.pull(subscription=subscription)
        return result

    def __subscription_exists(self, subscription):
        project = 'projects/{0}'.format(GoogleCloudPlataformUtil.get_project_id())
        name = 'projects/{0}/subscriptions/{1}'.format(
            GoogleCloudPlataformUtil.get_project_id(),
            GoogleCloudPubSubUtil.remove_invalid_characters(subscription)
        )
        subscriptions = self.__subscriber.list_subscriptions(project=project)
        subscriptions = [str(item.name) for item in subscriptions]
        exist = name in subscriptions
        return exist
