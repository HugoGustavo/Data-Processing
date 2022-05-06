from util.Logger import Logger
from util.StringUtil import StringUtil


class LoggingGoogleBigQueryProxy(object):
    def __init__(self, google_big_query_proxy):
        self.__google_big_query_proxy = google_big_query_proxy

    def count(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__google_big_query_proxy.count(data)
        return result

    def delete(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__google_big_query_proxy.delete(data)
        return result

    def delete_all(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__google_big_query_proxy.delete_all(data)
        return result

    def exists(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__google_big_query_proxy.exists(data)
        return result

    def find(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__google_big_query_proxy.find(data)
        return result

    def find_all(self):
        Logger.info(None)
        result = self.__google_big_query_proxy.find_all()
        return result

    def list(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__google_big_query_proxy.list(data)
        return result

    def save(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__google_big_query_proxy.save(data)
        return result

    def save_all(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__google_big_query_proxy.save_all(data)
        return result
