import json


class DictionaryUtil:

    @staticmethod
    def merge(input1, input2):
        if input1 is None:
            return None
        if input2 is None:
            return None
        result = input1.copy()
        result.update(input2)
        return result

    @staticmethod
    def get(dictionary, key, default=None):
        if dictionary is None or key is None:
            return None

        result = dictionary.get(key, None)
        if result is not None:
            return result

        try:
            key = [key] if not isinstance(key, str) else str(key).split('.')
            for item in key:
                result = dictionary.get(item, default)
                dictionary = result
            return result
        except Exception as exception:
            return default

    @staticmethod
    def set(dictionary, key, value):
        if dictionary is None or key is None:
            return None

        try:
            key = [key] if not isinstance(key, str) else str(key).split('.')
            for item in key[:-1]:
                dictionary[item] = dictionary.get(item, {})
                dictionary = dictionary[item]
            dictionary[key[-1]] = value
        except Exception as exception:
            pass

    @staticmethod
    def to_str(dictionary):
        if dictionary is None:
            return None
        result = json.dumps(dictionary)
        result = result.replace('"', '\"')
        return result
