import os


class GoogleCloudPlataformUtil(object):

    @staticmethod
    def get_project_id():
        result = os.getenv('GOOGLE_CLOUD_PROJECT')
        result = str(result).strip()
        return result

    @staticmethod
    def is_none(value):
        if value is None:
            return True
        return False

    @staticmethod
    def is_string(value):
        if value is None:
            return False
        result = isinstance(value, str)
        return result

    @staticmethod
    def is_empty(value):
        if value is None:
            return True
        value = str(value).strip()
        result = '' == value
        return result
