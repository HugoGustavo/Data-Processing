class MonitoringQualityCheckService(object):
    def __init__(self, quality_check_service):
        self.__quality_check_service = quality_check_service

    def execute(self, data):
        result = self.__quality_check_service.execute(data)
        return result
