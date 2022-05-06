class ProcessingService(object):
    def __init__(self, data_service, standardize_service, deduplication_service, quality_check_service,
                 validation_service, transformation_service):
        self.__data_service = data_service
        self.__standardize_service = standardize_service
        self.__deduplication_service = deduplication_service
        self.__quality_check_service = quality_check_service
        self.__transformation_service = transformation_service
        self.__validation_service = validation_service

    def execute(self):
        data_original = self.__data_service.find_all()

        data_modified = self.__standardize_service.execute(data_original)
        data_modified = self.__deduplication_service.execute(data_modified)
        data_modified = self.__quality_check_service.execute(data_modified)
        data_modified = self.__transformation_service.execute(data_modified)
        data_modified = self.__validation_service.execute(data_modified)

        self.__data_service.save_all(data_modified)
        self.__data_service.delete_all(data_original)
