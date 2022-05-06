from util.Logger import Logger
from util.ObjectUtil import ObjectUtil
from util.StringUtil import StringUtil
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

        arguments = ArgumentsUtil.get()
        configuration = DictionaryUtil.get(arguments, '--configuration', default='{}')
        configuration = StringUtil.to_dict(configuration)

        from_topic = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.fromTopic', ''))
        to_topic = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.toTopic', ''))
        company = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.company', ''))
        region = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.region', ''))
        business_unit = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.businessUnit', ''))
        vice_presidency = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.vicePresidency', ''))
        domain = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.domain', ''))
        subdomain = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.subdomain', ''))
        context = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.context', ''))
        pipeline = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.pipeline', ''))
        data_source = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.dataSource', ''))
        year = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.year', ''))
        month = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.month', ''))
        day = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.day', ''))
        execution = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.stream.execution', ''))
        schema = DictionaryUtil.get(configuration, 'flow.stream.schema', None)

        self.__configuration = StreamFlowConfiguration(
            from_topic=from_topic,
            to_topic=to_topic,
            company=company,
            region=region,
            business_unit=business_unit,
            vice_presidency=vice_presidency,
            domain=domain,
            subdomain=subdomain,
            context=context,
            pipeline=pipeline,
            data_source=data_source,
            year=year,
            month=month,
            day=day,
            execution=execution,
            schema=schema
        )

        Logger.debug(self.__configuration)

        return self.__configuration
