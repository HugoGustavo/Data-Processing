class MonitoringProcessingService(object):
    def __init__(self, processing_service):
        self.__processing_service = processing_service

    def execute(self):
        result = self.__processing_service.execute()
        return result
