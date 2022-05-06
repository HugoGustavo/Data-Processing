class MonitoringValidationService(object):
    def __init__(self, validation_service):
        self.__validation_service = validation_service

    def execute(self, data):
        result = self.__validation_service.execute(data)
        return result
