import re
import uuid
import string
import random


class GoogleCloudPubSubUtil(object):

    @staticmethod
    def subscription_id(subscription):

    @staticmethod
    def clean(topic):
        if topic is None:
            return None
        topic = str(topic).strip()
        return topic

    @staticmethod
    def remove_invalid_characters(value):
        if value is None:
            return None
        value = str(value).strip()
        value = re.sub(r'[^.a-zA-Z0-9\-]', '', value)
        return value

    @staticmethod
    def build_topic(topic):
        if topic is None:
            return None
        topic = str(topic).split('/')
        topic = topic[:-1]  # The last element is the message id
        topic = '/'.join(topic)
        return topic

    @staticmethod
    def build_id(topic):
        if topic is None:
            return None
        topic = str(topic).split('/')
        topic = topic[-1]  # The last element is the message id
        return topic

    @staticmethod
    def build_subscription(project, subscription):
        if project is None or subscription is None:
            return None
        project = str(project).strip()
        subscription = str(subscription).strip()
        subscription = 'projects/{0}/subscriptions/{1}'.format(project, subscription)
        return subscription

    @staticmethod
    def generate_uuid4():
        result = str(uuid.uuid4()).strip()
        first_letter = str(random.choice(string.ascii_letters)).lower()
        result = first_letter + result[1:]
        result = re.sub(r'[^.a-zA-Z0-9\-\.\_\~\%\+]', '', result)
        return result

    @staticmethod
    def is_empty(value):
        if value is None:
            return True
        value = str(value).strip()
        result = '' == value
        return result

    @staticmethod
    def is_not_empty(value):
        if value is None:
            return False
        value = str(value).strip()
        result = '' == value
        result = not result
        return result
