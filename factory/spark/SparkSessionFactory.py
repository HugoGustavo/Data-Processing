from pyspark.sql import SparkSession
from util.ObjectUtil import ObjectUtil
from util.StringUtil import StringUtil
from util.ArgumentsUtil import ArgumentsUtil
from util.DictionaryUtil import DictionaryUtil
from factory.job.JobConfigurationFactory import JobConfigurationFactory


class SparkSessionFactory(object):
    __instance = None

    def __init__(self):
        self.__session = None

    @classmethod
    def get_instance(cls):
        cls.__instance = ObjectUtil.get_default_if_none(cls.__instance, cls())
        return cls.__instance

    def build(self):
        if ObjectUtil.is_not_none(self.__session):
            return self.__session

        arguments = ArgumentsUtil.get()
        configuration = DictionaryUtil.get(arguments, '--configuration', default='{}')
        configuration = StringUtil.to_dict(configuration)

        master = DictionaryUtil.get(configuration, 'processing.sparkSession.master', 'local[1]')
        app_name = JobConfigurationFactory.get_instance().build().get_id()
        temporary_bucket = DictionaryUtil.get(configuration, 'processing.sparkSession.temporaryGcsBucket', {})

        self.__session = SparkSession.builder \
            .master(master) \
            .appName(app_name) \
            .getOrCreate()

        self.__session.conf.set('temporaryGcsBucket', temporary_bucket)

        spark_context_configuration = self.__session.sparkContext._jsc.hadoopConfiguration()
        parameters = DictionaryUtil.get(configuration, 'processing.sparkContext', {})
        for (key, value) in parameters.items():
            spark_context_configuration.set(key, value)

        return self.__session
