from util.Logger import Logger
from util.UUIDUtil import UUIDUtil
from util.ObjectUtil import ObjectUtil
from util.DateTimeUtil import DateTimeUtil
from util.ArgumentsUtil import ArgumentsUtil
from util.DictionaryUtil import DictionaryUtil
from configuration.flow.StreamFlowConfiguration import StreamFlowConfiguration


class StreamFlowConfigurationFactory(object):
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

        # arguments = ArgumentsUtil.get()
        # configuration = DictionaryUtil.get(arguments, '--configuration', default='{}')
        # configuration = StringUtil.to_dict(configuration)
        # from_topic = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.fromTopic', ''))
        # to_topic = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.toTopic', ''))
        # company = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.company', ''))
        # region = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.region', ''))
        # business_unit = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.businessUnit', ''))
        # vice_presidency = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.vicePresidency', ''))
        # domain = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.domain', ''))
        # subdomain = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.subdomain', ''))
        # context = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.context', ''))
        # pipeline = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.pipeline', ''))
        # data_source = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.dataSource', ''))
        # year = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.year', ''))
        # month = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.month', ''))
        # day = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.day', ''))
        # execution = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.execution', ''))
        # schema = DictionaryUtil.get(configuration, 'flow.stream.schema', None)

        arguments = ArgumentsUtil.get()
        from_topic = DictionaryUtil.get(arguments, '--fromTopic', default=None)
        to_topic = DictionaryUtil.get(arguments, '--toTopic', default=None)
        company = DictionaryUtil.get(arguments, '--company', default=None)
        region = DictionaryUtil.get(arguments, '--region', default=None)
        business_unit = DictionaryUtil.get(arguments, '--businessUnit', default=None)
        vice_presidency = DictionaryUtil.get(arguments, '--vicePresidency', default=None)
        domain = DictionaryUtil.get(arguments, '--domain', default=None)
        subdomain = DictionaryUtil.get(arguments, '--subdomain', default=None)
        context = DictionaryUtil.get(arguments, '--context', default=subdomain)
        pipeline = DictionaryUtil.get(arguments, '--pipeline', default=None)
        data_source = DictionaryUtil.get(arguments, '--dataSource', default=None)
        year = DictionaryUtil.get(arguments, '--year', default=DateTimeUtil.get_current_year())
        month = DictionaryUtil.get(arguments, '--month', default=DateTimeUtil.get_current_month())
        day = DictionaryUtil.get(arguments, '--day', default=DateTimeUtil.get_current_day())
        execution = DictionaryUtil.get(arguments, '--execution', default=UUIDUtil.generate())
        schema = DictionaryUtil.get(arguments, '--schema', default=None)

        self.__configuration = StreamFlowConfiguration(from_topic=from_topic, to_topic=to_topic, company=company,
                                                       region=region, business_unit=business_unit,
                                                       vice_presidency=vice_presidency, domain=domain,
                                                       subdomain=subdomain, context=context, pipeline=pipeline,
                                                       data_source=data_source, year=year, month=month, day=day,
                                                       execution=execution, schema=schema)

        Logger.debug(self.__configuration)

        return self.__configuration
