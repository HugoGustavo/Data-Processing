from util.Logger import Logger
from util.StringUtil import StringUtil


class ExceptionHandlingGoogleBigQueryProxy(object):
    def __init__(self, google_big_query_proxy):
        self.__google_big_query_proxy = google_big_query_proxy

    def count(self, data):
        try:
            result = self.__google_big_query_proxy.count(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass

    def delete(self, data):
        try:
            result = self.__google_big_query_proxy.delete(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass

    def delete_all(self, data):
        try:
            result = self.__google_big_query_proxy.delete_all(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass

    def exists(self, data):
        try:
            result = self.__google_big_query_proxy.exists(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass

    def find(self, data):
        try:
            result = self.__google_big_query_proxy.find(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass

    def find_all(self):
        try:
            result = self.__google_big_query_proxy.find_all()
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass

    def list(self, data):
        try:
            result = self.__google_big_query_proxy.list(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass

    def save(self, data):
        try:
            result = self.__google_big_query_proxy.save(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass

    def save_all(self, data):
        try:
            result = self.__google_big_query_proxy.save_all(data)
            return result
        except Exception as exception:
            Logger.error(StringUtil.clean(exception))
            raise exception
        finally:
            pass
