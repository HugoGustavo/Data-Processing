from util.Logger import Logger
from util.ObjectUtil import ObjectUtil
from util.StringUtil import StringUtil
from util.ArgumentsUtil import ArgumentsUtil
from util.DictionaryUtil import DictionaryUtil
from configuration.flow.AnalyticalFlowConfiguration import AnalyticalFlowConfiguration


class AnalyticalFlowConfigurationFactory(object):
    __instance = None

    def __init__(self):
        self.__configuration = None

    @classmethod
    def get_instance(cls):
        cls.__instance = ObjectUtil.get_default_if_none(cls.__instance, cls())
        return cls.__instance

    def build(self):
        if ObjectUtil.is_not_none(self.__configuration):
            return self.__configuration

        arguments = ArgumentsUtil.get()
        configuration = DictionaryUtil.get(arguments, '--configuration', default='{}')
        configuration = StringUtil.to_dict(configuration)

        from_dataset = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.analytical.fromDataset', ''))
        to_dataset = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.analytical.toDataset', ''))

        self.__configuration = AnalyticalFlowConfiguration(
            from_dataset=from_dataset,
            to_dataset=to_dataset
        )

        Logger.debug(self.__configuration)

        return self.__configuration
