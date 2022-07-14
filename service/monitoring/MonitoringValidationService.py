from util.StringUtil import StringUtil
from factory.flow.BatchFlowConfigurationFactory import BatchFlowConfigurationFactory
from factory.flow.AnalyticalFlowConfigurationFactory import AnalyticalFlowConfigurationFactory


class MonitoringValidationService(object):
    def __init__(self, validation_service):
        self.__validation_service = validation_service

    def execute(self, data):
        batch_configuration = BatchFlowConfigurationFactory.get_instance().build()
        analytical_configuration = AnalyticalFlowConfigurationFactory.get_instance().build()

        from_bucket_staging = StringUtil.contains_ignore_case("STAGING", batch_configuration.get_from_bucket())
        to_bucket_curated = StringUtil.contains_ignore_case("CURATED", batch_configuration.get_to_bucket())

        from_dataset = StringUtil.is_not_empty(analytical_configuration.get_from_dataset())
        to_dataset = StringUtil.is_not_empty(analytical_configuration.get_to_dataset())

        if from_bucket_staging and to_bucket_curated:
            result = self.__validation_service.execute(data)
        elif from_dataset or to_dataset:
            result = self.__validation_service.execute(data)
        else:
            result = data

        return result
