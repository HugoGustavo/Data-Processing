from factory.flow.AnalyticalFlowConfigurationFactory import AnalyticalFlowConfigurationFactory
from factory.flow.BatchFlowConfigurationFactory import BatchFlowConfigurationFactory
from util.StringUtil import StringUtil


class MonitoringStandardizeService(object):
    def __init__(self, standardize_service):
        self.__standardize_service = standardize_service

    def execute(self, data):
        batch_configuration = BatchFlowConfigurationFactory.get_instance().build()
        analytical_configuration = AnalyticalFlowConfigurationFactory.get_instance().build()

        from_bucket_raw = StringUtil.contains_ignore_case("RAW", batch_configuration.get_from_bucket())
        to_bucket_staging = StringUtil.contains_ignore_case("STAGING", batch_configuration.get_to_bucket())

        from_dataset = StringUtil.is_not_empty(analytical_configuration.get_from_dataset())
        to_dataset = StringUtil.is_not_empty(analytical_configuration.get_to_dataset())

        if from_bucket_raw and to_bucket_staging:
            result = self.__standardize_service.execute(data)
        elif from_dataset or to_dataset:
            result = self.__standardize_service.execute(data)
        else:
            result = data

        return result
