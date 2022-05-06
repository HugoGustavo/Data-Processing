from util.Logger import Logger
from util.StringUtil import StringUtil


class ExceptionHandlingTransformationService(object):
    def __init__(self, transformation_service):
        self.__transformation_service = transformation_service

    def execute(self, data):
        try:
            result = self.__transformation_service.execute(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass
