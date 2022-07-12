from pyspark.sql import SparkSession
from util.ObjectUtil import ObjectUtil
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

        # arguments = ArgumentsUtil.get()
        # configuration = DictionaryUtil.get(arguments, '--configuration', default='{}')
        # configuration = StringUtil.to_dict(configuration)

        # master = DictionaryUtil.get(configuration, 'processing.sparkSession.master', 'local[*]')
        # app_name = JobConfigurationFactory.get_instance().build().get_id()
        # temporary_bucket = DictionaryUtil.get(configuration, 'processing.sparkSession.temporaryGcsBucket', {})

        arguments = ArgumentsUtil.get()
        app_name = JobConfigurationFactory.get_instance().build().get_id()
        master = DictionaryUtil.get(arguments, '--masterServer', default='local[*]')
        temporary_bucket = DictionaryUtil.get(arguments, '--temporaryBucket', default=None)

        self.__session = SparkSession.builder \
            .master(master) \
            .appName(app_name) \
            .getOrCreate()

        # Default configurations for Spark work on Cloud Storage
        spark_context = self.__session.sparkContext._jsc.hadoopConfiguration()
        spark_context.set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
        spark_context.set('fs.AbstractFileSystem.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS')
        spark_context.set('mapreduce.fileoutputcommitter.marksuccessfuljobs', 'false')

        # Default configurations for Spark work on BigQuery
        self.__session.conf.set('temporaryGcsBucket', temporary_bucket)

        return self.__session
