class MonitoringDeduplicationService(object):
    def __init__(self, deduplication_service):
        self.__deduplication_service = deduplication_service

    def execute(self, data):
        result = self.__deduplication_service.execute(data)
        return result
