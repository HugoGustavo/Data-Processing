from util.Logger import Logger
from util.StringUtil import StringUtil


class ExceptionHandlingDeduplicationService(object):
    def __init__(self, deduplication_service):
        self.__deduplication_service = deduplication_service

    def execute(self, data):
        try:
            result = self.__deduplication_service.execute(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass
