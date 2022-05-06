from util.Logger import Logger
from util.StringUtil import StringUtil


class ExceptionHandlingValidationService(object):
    def __init__(self, validation_service):
        self.__validation_service = validation_service

    def execute(self, data):
        try:
            result = self.__validation_service.execute(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass
