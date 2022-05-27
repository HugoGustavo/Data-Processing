from model.Data import Data
from model.Content import Content
from util.ListUtil import ListUtil
from model.BatchPath import BatchPath
from util.StringUtil import StringUtil
from util.ObjectUtil import ObjectUtil
from model.StreamPath import StreamPath
from model.AnalyticalPath import AnalyticalPath
from factory.spark.SparkSessionFactory import SparkSessionFactory
from factory.flow.AnalyticalFlowConfigurationFactory import AnalyticalFlowConfigurationFactory


class GoogleBigQueryProxy(object):
    def __init__(self, google_bigquery_client):
        self.__google_bigquery_client = google_bigquery_client

    def count(self, data):
        analytical_path = data.get_analytical_path()
        analytical_path = StringUtil.clean(analytical_path)
        analytical_paths = self.__google_bigquery_client.list(analytical_path)
        analytical_paths = ListUtil.remove(analytical_paths, analytical_path)
        result = ListUtil.count(analytical_paths)
        return result

    def delete(self, data):
        analytical_path = StringUtil.clean(data.get_analytical_path())
        self.__google_bigquery_client.delete(analytical_path)

        batch_path = ObjectUtil.copy(data.get_batch_path())
        analytical_path = ObjectUtil.copy(data.get_analytical_path())
        stream_path = ObjectUtil.copy(data.get_stream_path())

        content = Content()
        content.set_schema(data.get_content().get_schema())
        content.set_as_dataframe(data.get_content().get_as_dataframe())

        data = Data()
        data.set_batch_path(batch_path)
        data.set_analytical_path(analytical_path)
        data.set_stream_path(stream_path)
        data.set_content(content)

        return data

    def delete_all(self, data):
        data = ListUtil.get_as_list(data)
        data = ListUtil.get_none_as_empty(data)
        data = ListUtil.remove_none(data)

        if ListUtil.is_empty(data):
            analytical_configuration = AnalyticalFlowConfigurationFactory.get_instance().build()
            analytical_path = AnalyticalPath()
            analytical_path.set_dataset(StringUtil.clean(analytical_configuration.get_from_dataset()))
            analytical_path.set_table(None)

            data = Data()
            data.set_batch_path(BatchPath())
            data.set_analytical_path(analytical_path)
            data.set_stream_path(StreamPath())
            data.set_content(Content())

            data = self.list(data)

        data = ListUtil.map(lambda item: self.delete(item), data)

        return data

    def exists(self, data):
        analytical_path = StringUtil.clean(data.get_analytical_path())
        result = self.__google_bigquery_client.exists(analytical_path)
        return result

    def find(self, data):
        spark_session = SparkSessionFactory.get_instance().build()

        table = StringUtil.clean(data.get_analytical_path())
        table = StringUtil.replace(table, '/', '.')
        dataframe = spark_session.read\
            .format('bigquery') \
            .option('table', table) \
            .load()

        batch_path = ObjectUtil.copy(data.get_batch_path())
        analytical_path = ObjectUtil.copy(data.get_analytical_path())
        stream_path = ObjectUtil.copy(data.get_stream_path())

        content = Content()
        content.set_schema(dataframe.schema)
        content.set_as_dataframe(dataframe)

        data = Data()
        data.set_batch_path(batch_path)
        data.set_analytical_path(analytical_path)
        data.set_stream_path(stream_path)
        data.set_content(content)

        return data

    def find_all(self):
        analytical_configuration = AnalyticalFlowConfigurationFactory.get_instance().build()
        analytical_path = AnalyticalPath()
        analytical_path.set_dataset(analytical_configuration.get_from_dataset())

        data = Data()
        data.set_batch_path(BatchPath())
        data.set_analytical_path(analytical_path)
        data.set_stream_path(StreamPath())
        data.set_content(Content())

        result = self.list(data)
        result = ListUtil.map(lambda item: self.find(item), result)

        return result

    def list(self, data):
        analytical_path = StringUtil.clean(data.get_analytical_path())
        analytical_paths = self.__google_bigquery_client.list(analytical_path)
        analytical_paths = ListUtil.remove(analytical_paths, analytical_path)

        result = ListUtil.get_none_as_empty(None)
        for item in analytical_paths:
            parameters = StringUtil.split(item, '/')
            analytical_path = AnalyticalPath()
            analytical_path.set_dataset(StringUtil.clean(ListUtil.at(parameters, 0)))
            analytical_path.set_table(StringUtil.clean(ListUtil.at(parameters, 1)))

            batch_path = ObjectUtil.copy(data.get_batch_path())
            stream_path = ObjectUtil.copy(data.get_stream_path())

            data = Data()
            data.set_batch_path(batch_path)
            data.set_analytical_path(analytical_path)
            data.set_stream_path(stream_path)
            data.set_content(Content())

            result.append(data)

        return result

    def save(self, data):
        dataframe = data.get_content().get_as_dataframe()
        table = StringUtil.clean(data.get_analytical_path())
        table = StringUtil.replace(table, '/', '.')

        dataframe.write\
            .format('bigquery') \
            .option('table', table) \
            .mode('append') \
            .save()

        batch_path = ObjectUtil.copy(data.get_batch_path())
        analytical_path = ObjectUtil.copy(data.get_analytical_path())
        stream_path = ObjectUtil.copy(data.get_stream_path())

        content = Content()
        content.set_schema(dataframe.schema)
        content.set_as_dataframe(dataframe)

        data = Data()
        data.set_batch_path(batch_path)
        data.set_analytical_path(analytical_path)
        data.set_stream_path(stream_path)
        data.set_content(content)

        return data

    def save_all(self, data):
        data = ListUtil.get_as_list(data)
        data = ListUtil.get_none_as_empty(data)
        data = ListUtil.remove_none(data)
        result = ListUtil.map(lambda item: self.save(item), data)
        return result
