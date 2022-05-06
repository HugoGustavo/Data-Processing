from util.Logger import Logger
from util.StringUtil import StringUtil


class ExceptionHandlingDataService(object):
    def __init__(self, data_service):
        self.__data_service = data_service

    def count(self):
        try:
            result = self.__data_service.count()
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass

    def delete(self, data):
        try:
            result = self.__data_service.delete(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass

    def delete_all(self, data):
        try:
            result = self.__data_service.delete_all(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass

    def exists(self, data):
        try:
            result = self.__data_service.exists(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass

    def find(self, data):
        try:
            result = self.__data_service.find(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass

    def find_all(self):
        try:
            result = self.__data_service.find_all()
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass

    def save(self, data):
        try:
            result = self.__data_service.save(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass

    def save_all(self, data):
        try:
            result = self.__data_service.save_all(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass
