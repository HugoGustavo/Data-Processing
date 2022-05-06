import copy


class JobConfiguration(object):
    def __init__(self, id=None, allow_delete=None, deduplication=None):
        self.__id = id
        self.__allow_delete = allow_delete
        self.__deduplication = deduplication

    def get_id(self):
        result = copy.deepcopy(self.__id)
        return result

    def is_allow_delete(self):
        result = copy.deepcopy(self.__allow_delete)
        return result

    def have_deduplication(self):
        result = copy.deepcopy(self.__deduplication)
        return result

    def __str__(self):
        result = '['
        result = result + 'name=' + str(self.__id) + ','
        result = result + 'allow_delete=' + str(self.__allow_delete) + ','
        result = result + 'deduplication=' + str(self.__deduplication)
        result = result + ']'
        return result
