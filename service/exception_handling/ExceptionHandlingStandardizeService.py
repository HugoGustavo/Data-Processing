from util.Logger import Logger
from util.StringUtil import StringUtil


class ExceptionHandlingStandardizeService(object):
    def __init__(self, standardize_service):
        self.__standardize_service = standardize_service

    def execute(self, data):
        try:
            result = self.__standardize_service.execute(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass
