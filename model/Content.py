class Content(object):
    def __init__(self, schema=None, dataframe=None):
        self.__schema = schema
        self.__dataframe = dataframe

    def get_schema(self):
        return self.__schema

    def set_schema(self, schema):
        self.__schema = schema

    def get_as_dataframe(self):
        return self.__dataframe

    def set_as_dataframe(self, dataframe):
        self.__dataframe = dataframe
