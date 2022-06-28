from util.ListUtil import ListUtil
from util.FileUtil import FileUtil
from util.StringUtil import StringUtil
from util.ObjectUtil import ObjectUtil
from factory.flow.AnalyticalFlowConfigurationFactory import AnalyticalFlowConfigurationFactory
from factory.flow.BatchFlowConfigurationFactory import BatchFlowConfigurationFactory
from factory.flow.StreamFlowConfigurationFactory import StreamFlowConfigurationFactory


class TrackingDataService(object):
    def __init__(self, data_service):
        self.__data_service = data_service

    def count(self):
        result = self.__data_service.count()
        return result

    def delete(self, data):
        result = self.__data_service.delete(data)
        return result

    def delete_all(self, data):
        result = self.__data_service.delete_all(data)
        return result

    def exists(self, data):
        result = self.__data_service.exists(data)
        return result

    def find(self, data):
        result = self.__data_service.find(data)
        return result

    def find_all(self):
        result = self.__data_service.find_all()
        return result

    def save(self, data):
        batch_configuration = BatchFlowConfigurationFactory.get_instance().build()
        analytical_configuration = AnalyticalFlowConfigurationFactory.get_instance().build()
        stream_configuration = StreamFlowConfigurationFactory.get_instance().build()

        data_is_not_none = ObjectUtil.is_not_none(data)
        to_bucket = StringUtil.is_not_empty(batch_configuration.get_to_bucket())
        to_dataset = StringUtil.is_not_empty(analytical_configuration.get_to_dataset())
        to_topic = StringUtil.is_not_empty(stream_configuration.get_to_topic())

        # Set destination
        if data_is_not_none and to_bucket:
            batch_path = data.get_batch_path()
            batch_path.set_bucket(StringUtil.clean(batch_configuration.get_to_bucket()))

        if data_is_not_none and to_dataset:
            analytical_path = data.get_analytical_path()
            analytical_path.set_dataset(StringUtil.clean(analytical_configuration.get_to_dataset()))
            analytical_table = FileUtil.remove_extension(data.get_batch_path().get_filename())
            analytical_path.set_table(ObjectUtil.get_default_if_none(analytical_path.get_table(), analytical_table))

        if data_is_not_none and to_topic:
            stream_path = data.get_stream_path()
            stream_path.set_topic(StringUtil.clean(stream_configuration.get_to_topic()))

        # make a copy of data
        if data_is_not_none and to_topic and not to_bucket:
            batch_path = data.get_batch_path()
            stream_path = data.get_stream_path()
            batch_path.set_bucket(StringUtil.clean(stream_path.get_to_topic()))
            batch_path.set_company(StringUtil.clean(stream_path.get_company()))
            batch_path.set_region(StringUtil.clean(stream_path.get_region()))
            batch_path.set_business_unit(StringUtil.clean(stream_path.get_business_unit()))
            batch_path.set_vice_presidency(StringUtil.clean(stream_path.get_vice_presidency()))
            batch_path.set_domain(StringUtil.clean(stream_path.get_domain()))
            batch_path.set_subdomain(StringUtil.clean(stream_path.get_subdomain()))
            batch_path.set_context(StringUtil.clean(stream_path.get_context()))
            batch_path.set_pipeline(StringUtil.clean(stream_path.get_pipeline()))
            batch_path.set_data_source(StringUtil.clean(stream_path.get_data_source()))
            batch_path.set_year(StringUtil.clean(stream_path.get_year()))
            batch_path.set_month(StringUtil.clean(stream_path.get_month()))
            batch_path.set_day(StringUtil.clean(stream_path.get_day()))
            batch_path.set_execution(StringUtil.clean(stream_path.get_execution()))
            batch_path.set_filename(StringUtil.clean(stream_path.get_message_id()))

        to_bucket_staging = StringUtil.contains_ignore_case("STAGING", batch_configuration.get_to_bucket())
        to_bucket_curated = StringUtil.contains_ignore_case("CURATED", batch_configuration.get_to_bucket())
        to_topic_curated = StringUtil.contains_ignore_case("CURATED", stream_configuration.get_to_topic())

        # Format File
        if data_is_not_none and to_bucket_staging:
            batch_path = data.get_batch_path()
            filename = batch_path.get_filename()
            filename = FileUtil.to_avro(filename)
            batch_path.set_filename(filename)

        if data_is_not_none and to_bucket_curated:
            batch_path = data.get_batch_path()
            filename = batch_path.get_filename()
            filename = FileUtil.to_parquet(filename)
            batch_path.set_filename(filename)

        # Remove unnecessary fields
        if data_is_not_none and to_bucket_curated:
            content = data.get_content()
            dataframe = content.get_as_dataframe().drop("LAST_MODIFIED", "INGEST_DATE")
            content.set_as_dataframe(dataframe)
            content.set_schema(dataframe.schema)
            data.set_content(content)

        if data_is_not_none and to_dataset:
            content = data.get_content()
            dataframe = content.get_as_dataframe().drop("LAST_MODIFIED", "INGEST_DATE")
            content.set_as_dataframe(dataframe)
            content.set_schema(dataframe.schema)
            data.set_content(content)

        if data_is_not_none and to_topic_curated:
            content = data.get_content()
            dataframe = content.get_as_dataframe().drop("LAST_MODIFIED", "INGEST_DATE")
            content.set_as_dataframe(dataframe)
            content.set_schema(dataframe.schema)
            data.set_content(content)

        result = self.__data_service.save_all(data)

        return result

    def save_all(self, data):
        batch_configuration = BatchFlowConfigurationFactory.get_instance().build()
        analytical_configuration = AnalyticalFlowConfigurationFactory.get_instance().build()
        stream_configuration = StreamFlowConfigurationFactory.get_instance().build()

        data_is_not_none = ObjectUtil.is_not_none(data)
        to_bucket = StringUtil.is_not_empty(batch_configuration.get_to_bucket())
        to_dataset = StringUtil.is_not_empty(analytical_configuration.get_to_dataset())
        to_topic = StringUtil.is_not_empty(stream_configuration.get_to_topic())

        # Set destination
        if data_is_not_none and to_bucket:
            for item in ListUtil.get_as_list(data):
                batch_path = item.get_batch_path()
                batch_path.set_bucket(StringUtil.clean(batch_configuration.get_to_bucket()))

        if data_is_not_none and to_dataset:
            for item in ListUtil.get_as_list(data):
                analytical_path = item.get_analytical_path()
                analytical_path.set_dataset(StringUtil.clean(analytical_configuration.get_to_dataset()))
                analytical_table = FileUtil.remove_extension(item.get_batch_path().get_filename())
                analytical_path.set_table(ObjectUtil.get_default_if_none(analytical_path.get_table(), analytical_table))

        if data_is_not_none and to_topic:
            for item in ListUtil.get_as_list(data):
                stream_path = item.get_stream_path()
                stream_path.set_topic(StringUtil.clean(stream_configuration.get_to_topic()))

        # make a copy of data
        if data_is_not_none and to_topic and not to_bucket:
            for item in ListUtil.get_as_list(data):
                batch_path = item.get_batch_path()
                stream_path = item.get_stream_path()
                batch_path.set_bucket(StringUtil.clean(stream_path.get_to_topic()))
                batch_path.set_company(StringUtil.clean(stream_path.get_company()))
                batch_path.set_region(StringUtil.clean(stream_path.get_region()))
                batch_path.set_business_unit(StringUtil.clean(stream_path.get_business_unit()))
                batch_path.set_vice_presidency(StringUtil.clean(stream_path.get_vice_presidency()))
                batch_path.set_domain(StringUtil.clean(stream_path.get_domain()))
                batch_path.set_subdomain(StringUtil.clean(stream_path.get_subdomain()))
                batch_path.set_context(StringUtil.clean(stream_path.get_context()))
                batch_path.set_pipeline(StringUtil.clean(stream_path.get_pipeline()))
                batch_path.set_data_source(StringUtil.clean(stream_path.get_data_source()))
                batch_path.set_year(StringUtil.clean(stream_path.get_year()))
                batch_path.set_month(StringUtil.clean(stream_path.get_month()))
                batch_path.set_day(StringUtil.clean(stream_path.get_day()))
                batch_path.set_execution(StringUtil.clean(stream_path.get_execution()))
                batch_path.set_filename(StringUtil.clean(stream_path.get_message_id()))

        to_bucket_staging = StringUtil.contains_ignore_case("STAGING", batch_configuration.get_to_bucket())
        to_bucket_curated = StringUtil.contains_ignore_case("CURATED", batch_configuration.get_to_bucket())
        to_topic_curated = StringUtil.contains_ignore_case("CURATED",  stream_configuration.get_to_topic())

        # Format File
        if data_is_not_none and to_bucket_staging:
            for item in ListUtil.get_as_list(data):
                batch_path = item.get_batch_path()
                filename = batch_path.get_filename()
                filename = FileUtil.to_avro(filename)
                batch_path.set_filename(filename)

        if data_is_not_none and to_bucket_curated:
            for item in ListUtil.get_as_list(data):
                batch_path = item.get_batch_path()
                filename = batch_path.get_filename()
                filename = FileUtil.to_parquet(filename)
                batch_path.set_filename(filename)

        # Remove unnecessary fields
        if data_is_not_none and to_bucket_curated:
            for item in ListUtil.get_as_list(data):
                content = item.get_content()
                dataframe = content.get_as_dataframe().drop("LAST_MODIFIED", "INGEST_DATE")
                content.set_as_dataframe(dataframe)
                content.set_schema(dataframe.schema)
                item.set_content(content)

        if data_is_not_none and to_dataset:
            for item in ListUtil.get_as_list(data):
                content = item.get_content()
                dataframe = content.get_as_dataframe().drop("LAST_MODIFIED", "INGEST_DATE")
                content.set_as_dataframe(dataframe)
                content.set_schema(dataframe.schema)
                item.set_content(content)

        if data_is_not_none and to_topic_curated:
            for item in ListUtil.get_as_list(data):
                content = item.get_content()
                dataframe = content.get_as_dataframe().drop("LAST_MODIFIED", "INGEST_DATE")
                content.set_as_dataframe(dataframe)
                content.set_schema(dataframe.schema)
                item.set_content(content)

        result = self.__data_service.save_all(data)

        return result
