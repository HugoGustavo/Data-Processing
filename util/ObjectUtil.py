import copy
import uuid


class ObjectUtil:

    @staticmethod
    def is_none(value):
        if value is None:
            return True
        return False

    @staticmethod
    def is_not_none(value):
        if value is None:
            return False
        return True

    @staticmethod
    def get_default_if_none(value, default):
        if value is None:
            return default
        return value

    @staticmethod
    def is_empty(value):
        if value is None:
            return True
        result = len(value) == 0
        return result

    @staticmethod
    def copy(value):
        if value is None:
            return None
        result = copy.deepcopy(value)
        return result

    @staticmethod
    def equals(value1, value2):
        if value1 is None and value2 is None:
            return False
        if value1 is None:
            return False
        if value2 is None:
            return False
        result = value1 == value2
        return result

    @staticmethod
    def is_not_equal(value1, value2):
        return not ObjectUtil.equals(value1, value2)

    @staticmethod
    def is_same(value1, value2):
        if value1 is None and value2 is None:
            return True
        if value1 is None:
            return False
        if value2 is None:
            return False
        result = value1 is value2
        return result

    @staticmethod
    def is_not_same(value1, value2):
        return not ObjectUtil.is_same(value1, value2)

    @staticmethod
    def is_instance(object_input, instance):
        if object_input is None:
            return False
        return isinstance(object, instance)

    @staticmethod
    def generate_uuid4():
        result = uuid.uuid4()
        return result

    @staticmethod
    def is_dict(value):
        result = isinstance(value, dict)
        return result

    @staticmethod
    def is_str(value):
        result = isinstance(value, str)
        return result
