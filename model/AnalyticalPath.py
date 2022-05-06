from util.ObjectUtil import ObjectUtil
from util.StringUtil import StringUtil


class AnalyticalPath(object):
    def __init__(self, dataset=None, table=None):
        self.__dataset = dataset
        self.__table = table

    def get_dataset(self):
        return self.__dataset

    def set_dataset(self, dataset):
        self.__dataset = dataset

    def get_table(self):
        return self.__table

    def set_table(self, table):
        self.__table = table

    def is_empty(self):
        return ObjectUtil.is_none(self.__dataset) \
            or ObjectUtil.is_none(self.__table)

    def __str__(self):
        if self.is_empty():
            return ''

        result = str(self.__dataset) + '/'

        if StringUtil.is_not_empty(self.__table):
            result = result + str(self.__table)

        return result
