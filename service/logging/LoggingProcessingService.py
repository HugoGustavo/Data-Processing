from util.Logger import Logger


class LoggingProcessingService(object):
    def __init__(self, processing_service):
        self.__processing_service = processing_service

    def execute(self):
        Logger.info(None)
        result = self.__processing_service.execute()
        return result
