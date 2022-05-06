from util.Logger import Logger
from util.ListUtil import ListUtil
from util.ObjectUtil import ObjectUtil
from util.StringUtil import StringUtil
from util.ArgumentsUtil import ArgumentsUtil
from util.DictionaryUtil import DictionaryUtil
from configuration.service.AnalyticalServiceConfiguration import AnalyticalServiceConfiguration


class AnalyticalServiceConfigurationFactory(object):
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

        server = DictionaryUtil.get(configuration, 'service.analytical.server', '')
        server = ListUtil.get_as_list(server)
        server = ListUtil.map(lambda item: StringUtil.clean(item), server)

        self.__configuration = AnalyticalServiceConfiguration(
            server=server
        )

        Logger.debug(self.__configuration)

        return self.__configuration
