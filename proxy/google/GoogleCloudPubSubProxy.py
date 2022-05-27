from model.Data import Data
from model.Content import Content
from util.JsonUtil import JsonUtil
from util.ListUtil import ListUtil
from model.BatchPath import BatchPath
from util.SchemaUtil import SchemaUtil
from util.StringUtil import StringUtil
from util.ObjectUtil import ObjectUtil
from model.StreamPath import StreamPath
from model.AnalyticalPath import AnalyticalPath
from factory.spark.SparkSessionFactory import SparkSessionFactory
from factory.flow.StreamFlowConfigurationFactory import StreamFlowConfigurationFactory


class GoogleCloudPubSubProxy(object):
    def __init__(self, google_cloud_pub_sub_client):
        self.__google_cloud_pub_sub_client = google_cloud_pub_sub_client

    def count(self, data):
        stream_path = StringUtil.clean(data.get_stream_path())
        result = self.__google_cloud_pub_sub_client.list(stream_path)
        result = ListUtil.remove(result, stream_path)
        result = ListUtil.count(result)
        return result

    def delete(self, data):
        stream_path = StringUtil.clean(data.get_stream_path())
        self.__google_cloud_pub_sub_client.delete(stream_path)

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
            stream_flow_configuration = StreamFlowConfigurationFactory.get_instance().build()

            stream_path = StreamPath()
            stream_path.set_topic(StringUtil.clean(stream_flow_configuration.get_from_topic()))
            stream_path.set_company(StringUtil.clean(stream_flow_configuration.get_company()))
            stream_path.set_region(StringUtil.clean(stream_flow_configuration.get_region()))
            stream_path.set_business_unit(StringUtil.clean(stream_flow_configuration.get_business_unit()))
            stream_path.set_vice_presidency(StringUtil.clean(stream_flow_configuration.get_vice_presidency()))
            stream_path.set_domain(StringUtil.clean(stream_flow_configuration.get_domain()))
            stream_path.set_subdomain(StringUtil.clean(stream_flow_configuration.get_subdomain()))
            stream_path.set_context(StringUtil.clean(stream_flow_configuration.get_context()))
            stream_path.set_pipeline(StringUtil.clean(stream_flow_configuration.get_pipeline()))
            stream_path.set_data_source(StringUtil.clean(stream_flow_configuration.get_data_source()))
            stream_path.set_year(StringUtil.clean(stream_flow_configuration.get_year()))
            stream_path.set_month(StringUtil.clean(stream_flow_configuration.get_month()))
            stream_path.set_day(StringUtil.clean(stream_flow_configuration.get_day()))
            stream_path.set_execution(StringUtil.clean(stream_flow_configuration.get_execution()))

            content = Content()
            content.set_schema(stream_flow_configuration.get_schema())

            data = Data()
            data.set_batch_path(BatchPath())
            data.set_analytical_path(AnalyticalPath())
            data.set_stream_path(stream_path)
            data.set_content(content)

            data = self.list(data)

        result = ListUtil.map(lambda item: self.delete(item), data)

        return result

    def exists(self, data):
        stream_path = StringUtil.clean(data.get_stream_path())
        result = self.__google_cloud_pub_sub_client.exists(stream_path)
        return result

    def find(self, data):
        spark_session = SparkSessionFactory.get_instance().build()

        stream_path = StringUtil.clean(data.get_stream_path())
        dataframe = ListUtil.get_as_list(self.__google_cloud_pub_sub_client.subscribe(stream_path))
        dataframe = spark_session.sparkContext.parallelize(dataframe)

        schema = data.get_content().get_schema()
        if SchemaUtil.is_json(schema):
            dataframe = spark_session \
                .read \
                .schema(schema) \
                .json(dataframe)
        else:
            dataframe = spark_session \
                .read \
                .schema(schema) \
                .csv(dataframe)

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
        stream_flow_configuration = StreamFlowConfigurationFactory.get_instance().build()

        stream_path = StreamPath()
        stream_path.set_topic(StringUtil.clean(stream_flow_configuration.get_from_topic()))
        stream_path.set_company(StringUtil.clean(stream_flow_configuration.get_company()))
        stream_path.set_region(StringUtil.clean(stream_flow_configuration.get_region()))
        stream_path.set_business_unit(StringUtil.clean(stream_flow_configuration.get_business_unit()))
        stream_path.set_vice_presidency(StringUtil.clean(stream_flow_configuration.get_vice_presidency()))
        stream_path.set_domain(StringUtil.clean(stream_flow_configuration.get_domain()))
        stream_path.set_subdomain(StringUtil.clean(stream_flow_configuration.get_subdomain()))
        stream_path.set_context(StringUtil.clean(stream_flow_configuration.get_context()))
        stream_path.set_pipeline(StringUtil.clean(stream_flow_configuration.get_pipeline()))
        stream_path.set_data_source(StringUtil.clean(stream_flow_configuration.get_data_source()))
        stream_path.set_year(StringUtil.clean(stream_flow_configuration.get_year()))
        stream_path.set_month(StringUtil.clean(stream_flow_configuration.get_month()))
        stream_path.set_day(StringUtil.clean(stream_flow_configuration.get_day()))
        stream_path.set_execution(StringUtil.clean(stream_flow_configuration.get_execution()))

        content = Content()
        content.set_schema(stream_flow_configuration.get_schema())

        data = Data()
        data.set_batch_path(BatchPath())
        data.set_analytical_path(AnalyticalPath())
        data.set_stream_path(stream_path)
        data.set_content(content)

        data = self.list(data)
        data = ListUtil.map(lambda item: self.find(item), data)

        return data

    def list(self, data):
        stream_paths = StringUtil.clean(data.get_stream_path())
        stream_paths = self.__google_cloud_pub_sub_client.list(stream_paths)

        result = ListUtil.get_none_as_empty(None)
        for item in stream_paths:
            parameters = StringUtil.split(item, '/')
            stream_path = StreamPath()
            stream_path.set_topic(StringUtil.clean(ListUtil.at(parameters, 0)))
            stream_path.set_company(StringUtil.clean(ListUtil.at(parameters, 1)))
            stream_path.set_region(StringUtil.clean(ListUtil.at(parameters, 2)))
            stream_path.set_business_unit(StringUtil.clean(ListUtil.at(parameters, 3)))
            stream_path.set_vice_presidency(StringUtil.clean(ListUtil.at(parameters, 4)))
            stream_path.set_domain(StringUtil.clean(ListUtil.at(parameters, 5)))
            stream_path.set_subdomain(StringUtil.clean(ListUtil.at(parameters, 6)))
            stream_path.set_context(StringUtil.clean(ListUtil.at(parameters, 7)))
            stream_path.set_pipeline(StringUtil.clean(ListUtil.at(parameters, 8)))
            stream_path.set_data_source(StringUtil.clean(ListUtil.at(parameters, 9)))
            stream_path.set_year(StringUtil.clean(ListUtil.at(parameters, 10)))
            stream_path.set_month(StringUtil.clean(ListUtil.at(parameters, 11)))
            stream_path.set_day(StringUtil.clean(ListUtil.at(parameters, 12)))
            stream_path.set_execution(StringUtil.clean(ListUtil.at(parameters, 13)))
            stream_path.set_message_id(StringUtil.clean(ListUtil.at(parameters, 14)))

            batch_path = ObjectUtil.copy(data.get_batch_path())
            analytical_path = ObjectUtil.copy(data.get_analytical_path())

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
        stream_path = StringUtil.clean(data.get_stream_path())
        dataframe = data.get_content().get_as_dataframe()
        message = JsonUtil.to_string(dataframe.toJSON())
        self.__google_cloud_pub_sub_client.publish(stream_path, message)

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

    def save_all(self, data):
        data = ListUtil.get_as_list(data)
        data = ListUtil.get_none_as_empty(data)
        data = ListUtil.remove_none(data)
        result = ListUtil.map(lambda item: self.save(item), data)
        return result
