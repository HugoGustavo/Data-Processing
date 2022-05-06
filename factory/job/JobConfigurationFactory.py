from util.JobUtil import JobUtil
from util.Logger import Logger
from util.ObjectUtil import ObjectUtil
from util.StringUtil import StringUtil
from util.ArgumentsUtil import ArgumentsUtil
from util.DictionaryUtil import DictionaryUtil
from configuration.job.JobConfiguration import JobConfiguration


class JobConfigurationFactory(object):
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

        id = StringUtil.clean(DictionaryUtil.get(configuration, 'job.id', JobUtil.generate_name()))
        allow_delete = StringUtil.clean(DictionaryUtil.get(configuration, 'job.allowDelete', False))
        deduplication = StringUtil.clean(DictionaryUtil.get(configuration, 'job.deduplication', False))

        self.__configuration = JobConfiguration(
            id=id,
            allow_delete=allow_delete,
            deduplication=deduplication
        )

        Logger.debug(self.__configuration)

        return self.__configuration
