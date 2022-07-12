import ast
import json


class StringUtil:
    @staticmethod
    def format(pattern, *arguments):
        if pattern is None or arguments is None:
            return None
        result = str(pattern).format(*arguments)
        return result

    @staticmethod
    def join(separator, *arguments):
        if separator is None or arguments is None:
            return None
        result = str(separator).join(arguments)
        return result

    @staticmethod
    def length(value):
        result = 0 if value is None else len(value)
        return result

    @staticmethod
    def is_empty(value):
        if value is None:
            return True
        result = '' == StringUtil.clean(value)
        return result

    @staticmethod
    def substring(value, start, end):
        if value is None:
            return None
        return str(value)[start:end]

    @staticmethod
    def replace(value, old, new):
        if value is None:
            return None
        result = str(value).replace(old, new)
        return result

    @staticmethod
    def remove(value, pattern):
        if value is None:
            return None
        result = str(value).replace(pattern, '')
        return result

    @staticmethod
    def remove_all_spaces(value):
        if value is None:
            return None
        result = str(value).replace(' ', '')
        return result

    @staticmethod
    def split(value, separator=" "):
        if value is None:
            return None
        return str(value).split(separator)

    @staticmethod
    def copy(value):
        if value is None:
            return None
        return str(value)

    @staticmethod
    def is_not_empty(value):
        return not StringUtil.is_empty(value)

    @staticmethod
    def get_none_as_empty(value):
        result = '' if value is None else value
        return result

    @staticmethod
    def get_empty_as_none(value):
        if value is None:
            return None
        result = None if StringUtil.is_empty(value) else value
        return result

    @staticmethod
    def clean(value):
        if value is None:
            return ''
        return str(value).strip()

    @staticmethod
    def equals(value1, value2):
        if value1 is None and value2 is None:
            return True
        if value1 is None:
            return False
        if value2 is None:
            return False
        return str(value1) == str(value2)

    @staticmethod
    def equals_ignore_case(value1, value2):
        if value1 is None and value2 is None:
            return True
        if value1 is None:
            return False
        if value2 is None:
            return False

        value1 = StringUtil.to_upper(value1)
        value2 = StringUtil.to_upper(value2)

        return str(value1) == str(value2)

    @staticmethod
    def contains(value1, value2):
        if value1 is None and value2 is None:
            return False
        if value1 is None:
            return False
        if value2 is None:
            return False
        result = str(value1) in str(value2)
        return result

    @staticmethod
    def to_upper(value):
        if value is None: return None
        result = str(value).upper()
        return result

    @staticmethod
    def contains_ignore_case(value1, value2):
        if value1 is None and value2 is None:
            return False
        if value1 is None:
            return False
        if value2 is None:
            return False

        value1 = StringUtil.to_upper(value1)
        value2 = StringUtil.to_upper(value2)
        result = value1 in value2

        return result

    @staticmethod
    def concat(*arguments):
        result = ''.join(arguments)
        return result

    @staticmethod
    def to_int(value):
        if value is None:
            return None
        result = StringUtil.clean(value)
        result = int(result)
        return result

    @staticmethod
    def to_float(value):
        if value is None:
            return None
        result = StringUtil.clean(value)
        result = float(result)
        return result

    @staticmethod
    def to_dict(value):
        if value is None:
            return None
        result = StringUtil.clean(value)
        result = ast.literal_eval(result)
        return result

    @staticmethod
    def to_json(value):
        if value is None:
            return None
        result = StringUtil.clean(value)
        result = str(result).replace('\"', '"')
        result = str(result).replace('"', '\"')
        result = json.loads(result)
        return result

    @staticmethod
    def to_bool(value):
        if value is None:
            return None
        result = str(value).lower()
        result = json.loads(result)
        return result
