class SecurityTransformationService(object):
    def __init__(self, transformation_service):
        self.__transformation_service = transformation_service

    def execute(self, data):
        result = self.__transformation_service.execute(data)
        return result
