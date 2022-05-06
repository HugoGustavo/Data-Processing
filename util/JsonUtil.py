import json


class JsonUtil:

    @staticmethod
    def get_none_as_empty(value):
        result = dict() if value is None else value
        return result

    @staticmethod
    def to_string(value):
        value = JsonUtil.get_none_as_empty(value)
        return json.dumps(value)
