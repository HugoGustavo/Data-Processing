class MonitoringDataService(object):
    def __init__(self, data_service):
        self.__data_service = data_service

    def count(self):
        result = self.__data_service.count()
        return result

    def delete(self, data):
        result = self.__data_service.delete(data)
        return result

    def delete_all(self, data):
        result = self.__data_service.delete_all(data)
        return result

    def exists(self, data):
        result = self.__data_service.exists(data)
        return result

    def find(self, data):
        result = self.__data_service.find(data)
        return result

    def find_all(self):
        result = self.__data_service.find_all()
        return result

    def save(self, data):
        result = self.__data_service.save(data)
        return result

    def save_all(self, data):
        result = self.__data_service.save_all(data)
        return result
