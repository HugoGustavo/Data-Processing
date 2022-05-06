import copy


class AnalyticalFlowConfiguration(object):
    def __init__(self, from_dataset=None, to_dataset=None):
        self.__from_dataset = from_dataset
        self.__to_dataset = to_dataset

    def get_from_dataset(self):
        result = copy.deepcopy(self.__from_dataset)
        return result

    def get_to_dataset(self):
        result = copy.deepcopy(self.__to_dataset)
        return result

    def __str__(self):
        result = '['
        result = result + 'from_dataset=' + str(self.__from_dataset) + ','
        result = result + 'to_dataset=' + str(self.__to_dataset)
        result = result + ']'
        return result
