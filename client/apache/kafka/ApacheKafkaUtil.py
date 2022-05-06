import re


class ApacheKafkaUtil(object):

    @staticmethod
    def clean_path(path):
        path = str(path).strip()
        path = re.sub(r'[^.a-zA-Z0-9/]', "_", path)
        path = path.replace("/", "\\")
        return path

    @staticmethod
    def join(separator, *arguments):
        if separator is None or arguments is None:
            return None
        result = str(separator).join(arguments)
        return result

    @staticmethod
    def to_tuple(*arguments):
        arguments = [item for item in arguments]
        result = tuple(arguments)
        return result

    @staticmethod
    def is_none(value):
        result = value is None
        return result

    @staticmethod
    def is_not_none(value):
        result = value is None
        result = not result
        return result

    @staticmethod
    def is_empty(value):
        if value is None:
            return True
        if isinstance(value, list):
            return [] == value
        result = str(value).strip()
        result = '' == result
        return result

    @staticmethod
    def is_not_empty(value):
        if value is None:
            return False
        if isinstance(value, list):
            return [] != value
        result = str(value).strip()
        result = '' == result
        result = not result
        return result

    @staticmethod
    def concat(*arguments):
        arguments = [str(item) for item in arguments]
        result = ''.join(arguments)
        return result

    @staticmethod
    def to_set(value):
        result = set(value)
        return result
