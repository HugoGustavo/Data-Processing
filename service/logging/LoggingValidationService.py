from util.Logger import Logger
from util.StringUtil import StringUtil


class LoggingValidationService(object):
    def __init__(self, validation_service):
        self.__validation_service = validation_service

    def execute(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__validation_service.execute(data)
        return result
