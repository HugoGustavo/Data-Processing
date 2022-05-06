from util.Logger import Logger
from util.StringUtil import StringUtil


class LoggingStandardizeService(object):
    def __init__(self, standardize_service):
        self.__standardize_service = standardize_service

    def execute(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__standardize_service.execute(data)
        return result
