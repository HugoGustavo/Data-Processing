class ListUtil(object):

    @staticmethod
    def get_as_list(value):
        result = [value] if not isinstance(value, list) else value
        return result

    @staticmethod
    def is_empty(value):
        if value is None: return True
        return [] == value

    @staticmethod
    def at(value, index, default=None):
        if value is None or index is None:
            return None
        try:
            result = value[index]
        except Exception as exception:
            result = default
        return result

    @staticmethod
    def get_none_as_empty(value):
        value = [] if value is None else value
        return value

    @staticmethod
    def remove_none(value):
        if value is None:
            return None
        result = [item for item in value if item]
        return result

    @staticmethod
    def contains(value_list, item):
        if value_list is None or item is None:
            return None
        result = item in value_list
        return result

    @staticmethod
    def iterate(value_list, start=0, end=-1, step=1):
        result = value_list[start:end:step]
        return result

    @staticmethod
    def map(function, value):
        if function is None or value is None:
            return None
        result = list(map(function, value))
        return result

    @staticmethod
    def zip(value1, value2):
        if value1 is None or value2 is None:
            return None
        result = zip(value1, value2)
        return result

    @staticmethod
    def count(value):
        if value is None:
            return None
        result = len(value)
        return result

    @staticmethod
    def filter(function, value):
        if function is None or value is None:
            return None
        result = list(filter(function, value))
        return result

    @staticmethod
    def remove(value, item):
        if value is None:
            return None
        try:
            value.remove(item)
        except Exception as exception:
            pass
        return value

    @staticmethod
    def to_dict(value):
        if value is None:
            return None
        result = dict(value)
        return result
