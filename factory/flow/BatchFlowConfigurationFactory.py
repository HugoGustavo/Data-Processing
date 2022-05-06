from util.Logger import Logger
from util.ObjectUtil import ObjectUtil
from util.StringUtil import StringUtil
from util.ArgumentsUtil import ArgumentsUtil
from util.DictionaryUtil import DictionaryUtil
from configuration.flow.BatchFlowConfiguration import BatchFlowConfiguration


class BatchFlowConfigurationFactory(object):
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

        from_bucket = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.batch.fromBucket', ''))
        to_bucket = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.batch.toBucket', ''))
        company = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.batch.company', ''))
        region = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.batch.region', ''))
        business_unit = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.batch.businessUnit', ''))
        vice_presidency = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.batch.vicePresidency', ''))
        domain = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.batch.domain', ''))
        subdomain = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.batch.subdomain', ''))
        context = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.batch.context', ''))
        pipeline = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.batch.pipeline', ''))
        data_source = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.batch.dataSource', ''))
        year = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.batch.year', ''))
        month = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.batch.month', ''))
        day = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.batch.day', ''))
        execution = StringUtil.clean(DictionaryUtil.get(configuration, 'flow.batch.execution', ''))
        schema = DictionaryUtil.get(configuration, 'flow.batch.schema', None)

        self.__configuration = BatchFlowConfiguration(
            from_bucket=from_bucket,
            to_bucket=to_bucket,
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
