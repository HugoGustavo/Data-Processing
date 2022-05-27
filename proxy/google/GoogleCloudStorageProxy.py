from model.Data import Data
from model.Content import Content
from util.PathUtil import PathUtil
from util.ListUtil import ListUtil
from util.FileUtil import FileUtil
from model.BatchPath import BatchPath
from util.StringUtil import StringUtil
from util.ObjectUtil import ObjectUtil
from model.StreamPath import StreamPath
from model.AnalyticalPath import AnalyticalPath
from factory.spark.SparkSessionFactory import SparkSessionFactory
from factory.flow.BatchFlowConfigurationFactory import BatchFlowConfigurationFactory


class GoogleCloudStorageProxy(object):
    def __init__(self, google_cloud_storage_client):
        self.__google_cloud_storage_client = google_cloud_storage_client

    def count(self, data):
        batch_path = StringUtil.clean(data.get_batch_path())
        result = self.__google_cloud_storage_client.list(batch_path)
        result = ListUtil.remove(result, batch_path)
        result = ListUtil.count(result)
        return result

    def delete(self, data):
        batch_path = StringUtil.clean(data.get_batch_path())
        self.__google_cloud_storage_client.delete(batch_path)

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
            batch_flow_configuration = BatchFlowConfigurationFactory.get_instance().build()

            batch_path = BatchPath()
            batch_path.set_bucket(StringUtil.clean(batch_flow_configuration.get_from_bucket()))
            batch_path.set_company(StringUtil.clean(batch_flow_configuration.get_company()))
            batch_path.set_region(StringUtil.clean(batch_flow_configuration.get_region()))
            batch_path.set_business_unit(StringUtil.clean(batch_flow_configuration.get_business_unit()))
            batch_path.set_vice_presidency(StringUtil.clean(batch_flow_configuration.get_vice_presidency()))
            batch_path.set_domain(StringUtil.clean(batch_flow_configuration.get_domain()))
            batch_path.set_subdomain(StringUtil.clean(batch_flow_configuration.get_subdomain()))
            batch_path.set_context(StringUtil.clean(batch_flow_configuration.get_context()))
            batch_path.set_pipeline(StringUtil.clean(batch_flow_configuration.get_pipeline()))
            batch_path.set_data_source(StringUtil.clean(batch_flow_configuration.get_data_source()))
            batch_path.set_year(StringUtil.clean(batch_flow_configuration.get_year()))
            batch_path.set_month(StringUtil.clean(batch_flow_configuration.get_month()))
            batch_path.set_day(StringUtil.clean(batch_flow_configuration.get_day()))
            batch_path.set_execution(StringUtil.clean(batch_flow_configuration.get_execution()))

            data = Data()
            data.set_batch_path(batch_path)
            data.set_analytical_path(AnalyticalPath())
            data.set_stream_path(StreamPath())
            data.set_content(Content())

            data = self.list(data)

        result = ListUtil.map(lambda item: self.delete(item), data)

        return result

    def exists(self, data):
        batch_path = StringUtil.clean(data.get_batch_path())
        result = self.__google_cloud_storage_client.exists(batch_path)
        return result

    def find(self, data):
        spark_session = SparkSessionFactory.get_instance().build()

        filename = data.get_batch_path().get_filename()
        schema = data.get_content().get_schema()
        batch_path = StringUtil.concat("gs://", StringUtil.clean(data.get_batch_path()))

        if FileUtil.is_txt(filename):
            dataframe = spark_session.read \
                .schema(schema) \
                .text(batch_path)

        elif FileUtil.is_csv(filename):
            dataframe = spark_session.read \
                .schema(schema) \
                .option("delimiter", ";") \
                .csv(batch_path)

        elif FileUtil.is_parquet(filename):
            dataframe = spark_session.read\
                .schema(schema)\
                .parquet(batch_path)

        elif FileUtil.is_orc(filename):
            dataframe = spark_session.read\
                .schema(schema)\
                .orc(batch_path)

        elif FileUtil.is_avro(filename):
            dataframe = spark_session.read\
                .format("avro")\
                .schema(schema)\
                .load(batch_path)

        elif FileUtil.is_json(filename):
            dataframe = spark_session.read\
                .format("json")\
                .schema(schema)\
                .load(batch_path)

        elif FileUtil.is_xml(filename):
            dataframe = spark_session.read\
                .format("com.databricks.spark_session.xml")\
                .schema(schema)\
                .xml(batch_path)

        else:
            dataframe = self.__google_cloud_storage_client.download(batch_path)

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
        batch_flow_configuration = BatchFlowConfigurationFactory.get_instance().build()

        batch_path = BatchPath()
        batch_path.set_bucket(StringUtil.clean(batch_flow_configuration.get_from_bucket()))
        batch_path.set_company(StringUtil.clean(batch_flow_configuration.get_company()))
        batch_path.set_region(StringUtil.clean(batch_flow_configuration.get_region()))
        batch_path.set_business_unit(StringUtil.clean(batch_flow_configuration.get_business_unit()))
        batch_path.set_vice_presidency(StringUtil.clean(batch_flow_configuration.get_vice_presidency()))
        batch_path.set_domain(StringUtil.clean(batch_flow_configuration.get_domain()))
        batch_path.set_subdomain(StringUtil.clean(batch_flow_configuration.get_subdomain()))
        batch_path.set_context(StringUtil.clean(batch_flow_configuration.get_context()))
        batch_path.set_pipeline(StringUtil.clean(batch_flow_configuration.get_pipeline()))
        batch_path.set_data_source(StringUtil.clean(batch_flow_configuration.get_data_source()))
        batch_path.set_year(StringUtil.clean(batch_flow_configuration.get_year()))
        batch_path.set_month(StringUtil.clean(batch_flow_configuration.get_month()))
        batch_path.set_day(StringUtil.clean(batch_flow_configuration.get_day()))
        batch_path.set_execution(StringUtil.clean(batch_flow_configuration.get_execution()))

        content = Content()
        content.set_schema(batch_flow_configuration.get_schema())

        data = Data()
        data.set_batch_path(batch_path)
        data.set_analytical_path(AnalyticalPath())
        data.set_stream_path(StreamPath())
        data.set_content(content)

        data = self.list(data)
        data = ListUtil.map(lambda item: self.find(item), data)

        return data

    def list(self, data):
        batch_path = StringUtil.clean(data.get_batch_path())
        batch_paths = self.__google_cloud_storage_client.list(batch_path)
        batch_paths = ListUtil.filter(lambda item : PathUtil.is_not_folder(item), batch_paths)
        batch_paths = ListUtil.remove(batch_paths, batch_path)

        result = ListUtil.get_none_as_empty(None)
        for item in batch_paths:
            parameters = StringUtil.split(item, '/')

            batch_path = BatchPath()
            batch_path.set_bucket(StringUtil.clean(ListUtil.at(parameters, 0)))
            batch_path.set_company(StringUtil.clean(ListUtil.at(parameters, 1)))
            batch_path.set_region(StringUtil.clean(ListUtil.at(parameters, 2)))
            batch_path.set_business_unit(StringUtil.clean(ListUtil.at(parameters, 3)))
            batch_path.set_vice_presidency(StringUtil.clean(ListUtil.at(parameters, 4)))
            batch_path.set_domain(StringUtil.clean(ListUtil.at(parameters, 5)))
            batch_path.set_subdomain(StringUtil.clean(ListUtil.at(parameters, 6)))
            batch_path.set_context(StringUtil.clean(ListUtil.at(parameters, 7)))
            batch_path.set_pipeline(StringUtil.clean(ListUtil.at(parameters, 8)))
            batch_path.set_data_source(StringUtil.clean(ListUtil.at(parameters, 9)))
            batch_path.set_year(StringUtil.clean(ListUtil.at(parameters, 10)))
            batch_path.set_month(StringUtil.clean(ListUtil.at(parameters, 11)))
            batch_path.set_day(StringUtil.clean(ListUtil.at(parameters, 12)))
            batch_path.set_execution(StringUtil.clean(ListUtil.at(parameters, 13)))
            batch_path.set_filename(StringUtil.clean(ListUtil.at(parameters, 14)))

            analytical_path = ObjectUtil.copy(data.get_analytical_path())
            stream_path = ObjectUtil.copy(data.get_stream_path())

            content = Content()
            content.set_schema(data.get_content().get_schema())
            content.set_as_dataframe(data.get_content().get_as_dataframe())

            new_data = Data()
            new_data.set_batch_path(batch_path)
            new_data.set_analytical_path(analytical_path)
            new_data.set_stream_path(stream_path)
            new_data.set_content(content)

            result.append(new_data)

        return result

    def save(self, data):
        filename = data.get_batch_path().get_filename()
        dataframe = data.get_content().get_as_dataframe()

        batch_path = StringUtil.clean(data.get_batch_path())
        batch_path = StringUtil.concat("gs://", batch_path)

        if FileUtil.is_txt(filename):
            dataframe.write\
                .mode("overwrite")\
                .text(batch_path)

        elif FileUtil.is_csv(filename):
            dataframe.write\
                .mode("overwrite")\
                .csv(batch_path)

        elif FileUtil.is_parquet(filename):
            dataframe.write\
                .mode("overwrite")\
                .parquet(batch_path)

        elif FileUtil.is_orc(filename):
            dataframe.write\
                .mode("overwrite")\
                .orc(batch_path)

        elif FileUtil.is_avro(filename):
            dataframe.write\
                .mode("overwrite")\
                .format("avro")\
                .save(batch_path)

        elif FileUtil.is_json(filename):
            dataframe.write\
                .mode("overwrite")\
                .json(batch_path)

        elif FileUtil.is_xml(filename):
            dataframe.write\
                .mode("overwrite")\
                .format("com.databricks.spark_session.xml")\
                .save(batch_path)

        else:
            self.__google_cloud_storage_client.upload(batch_path, dataframe.to_str())

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
