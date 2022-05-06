from util.Logger import Logger
from util.StringUtil import StringUtil


class LoggingTransformationService(object):
    def __init__(self, transformation_service):
        self.__transformation_service = transformation_service

    def execute(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__transformation_service.execute(data)
        return result
