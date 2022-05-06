from util.Logger import Logger
from util.StringUtil import StringUtil


class LoggingDeduplicationService(object):
    def __init__(self, deduplicaton_service):
        self.__deduplicaton_service = deduplicaton_service

    def execute(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__deduplicaton_service.execute(data)
        return result
