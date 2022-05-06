from util.Logger import Logger
from util.StringUtil import StringUtil


class ExceptionHandlingQualityCheckService(object):
    def __init__(self, quality_check_service):
        self.__quality_check_service = quality_check_service

    def execute(self, data):
        try:
            result = self.__quality_check_service.execute(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass
