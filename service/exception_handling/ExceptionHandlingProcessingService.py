from util.Logger import Logger
from util.StringUtil import StringUtil


class ExceptionHandlingProcessingService(object):
    def __init__(self, processing_service):
        self.__processing_service = processing_service

    def execute(self):
        try:
            result = self.__processing_service.execute()
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass
