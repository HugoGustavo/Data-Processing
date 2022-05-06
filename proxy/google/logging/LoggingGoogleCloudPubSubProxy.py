from util.Logger import Logger
from util.StringUtil import StringUtil


class LoggingGoogleCloudPubSubProxy(object):
    def __init__(self, google_pub_sub_proxy):
        self.__google_pub_sub_proxy = google_pub_sub_proxy

    def count(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__google_pub_sub_proxy.count(data)
        return result

    def delete(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__google_pub_sub_proxy.delete(data)
        return result

    def delete_all(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__google_pub_sub_proxy.delete_all(data)
        return result

    def exists(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__google_pub_sub_proxy.exists(data)
        return result

    def find(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__google_pub_sub_proxy.find(data)
        return result

    def find_all(self):
        result = self.__google_pub_sub_proxy.find_all()
        return result

    def list(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__google_pub_sub_proxy.list(data)
        return result

    def save(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__google_pub_sub_proxy.save(data)
        return result

    def save_all(self, data):
        Logger.info(StringUtil.clean(data))
        result = self.__google_pub_sub_proxy.save_all(data)
        return result
