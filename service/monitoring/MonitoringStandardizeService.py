class MonitoringStandardizeService(object):
    def __init__(self, standardize_service):
        self.__standardize_service = standardize_service

    def execute(self, data):
        result = self.__standardize_service.execute(data)
        return result
