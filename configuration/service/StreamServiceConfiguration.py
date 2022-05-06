import copy


class StreamServiceConfiguration(object):
    def __init__(self, server=None):
        self.__server = server

    def get_servers(self):
        result = copy.deepcopy(self.__server)
        return result
