from util.Logger import Logger
from util.StringUtil import StringUtil


class LoggingQualityCheckService(object):
    def __init__(self, quality_check_service):
        self.__quality_check_service = quality_check_service

    def execute(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__quality_check_service.execute(data)
        return result
