from pyspark.sql.functions import lit

from model.Data import Data
from model.Content import Content
from util.FileUtil import FileUtil
from util.ListUtil import ListUtil
from util.ObjectUtil import ObjectUtil
from factory.flow.AnalyticalFlowConfigurationFactory import AnalyticalFlowConfigurationFactory


class TransformationService(object):
    def __init__(self):
        pass

    def execute(self, data):
        data = self.select_columns(data)
        data = self.add_new_column(data)
        data = self.to_file(data)
        data = self.to_table(data)
        return data

    def check_data_content(self, data):
        content = data.get_content()
        dataframe = content.get_as_dataframe()

        batch_path = ObjectUtil.copy(data.get_batch_path())
        analytical_path = ObjectUtil.copy(data.get_analytical_path())
        stream_path = ObjectUtil.copy(data.get_stream_path())

        content = Content()
        content.set_schema(dataframe.schema)
        content.set_as_dataframe(dataframe)

        new_data = Data()
        new_data.set_batch_path(batch_path)
        new_data.set_analytical_path(analytical_path)
        new_data.set_stream_path(stream_path)
        new_data.set_content(content)

    def select_columns(self, data):
        result = []
        for item in ListUtil.get_as_list(data):
            content = item.get_content()
            dataframe = content.get_as_dataframe()
            dataframe = dataframe.select("matnr", "werks", "mmsta", "ekgrp", "perkz", "zzsegoper", "peinh")

            batch_path = ObjectUtil.copy(item.get_batch_path())
            analytical_path = ObjectUtil.copy(item.get_analytical_path())
            stream_path = ObjectUtil.copy(item.get_stream_path())

            content = Content()
            content.set_schema(dataframe.schema)
            content.set_as_dataframe(dataframe)

            new_data = Data()
            new_data.set_batch_path(batch_path)
            new_data.set_analytical_path(analytical_path)
            new_data.set_stream_path(stream_path)
            new_data.set_content(content)

            result.append(new_data)

        return result

    def add_new_column(self, data):
        result = []
        for item in ListUtil.get_as_list(data):
            content = item.get_content()
            dataframe = content.get_as_dataframe()
            dataframe = dataframe.withColumn("percentage", lit(0.03))

            batch_path = ObjectUtil.copy(item.get_batch_path())
            analytical_path = ObjectUtil.copy(item.get_analytical_path())
            stream_path = ObjectUtil.copy(item.get_stream_path())

            content = Content()
            content.set_schema(dataframe.schema)
            content.set_as_dataframe(dataframe)

            new_data = Data()
            new_data.set_batch_path(batch_path)
            new_data.set_analytical_path(analytical_path)
            new_data.set_stream_path(stream_path)
            new_data.set_content(content)

            result.append(new_data)

        return result

    def to_table(self, data):
        result = []
        for item in ListUtil.get_as_list(data):
            content = item.get_content()
            dataframe = content.get_as_dataframe()

            batch_path = ObjectUtil.copy(item.get_batch_path())
            analytical_path = ObjectUtil.copy(item.get_analytical_path())
            analytical_path.set_table("DEMO")
            stream_path = ObjectUtil.copy(item.get_stream_path())

            content = Content()
            content.set_schema(dataframe.schema)
            content.set_as_dataframe(dataframe)

            new_data = Data()
            new_data.set_batch_path(batch_path)
            new_data.set_analytical_path(analytical_path)
            new_data.set_stream_path(stream_path)
            new_data.set_content(content)

            result.append(new_data)

        return result

    def to_file(self, data):
        result = []
        for item in ListUtil.get_as_list(data):
            content = item.get_content()
            dataframe = content.get_as_dataframe()

            batch_path = ObjectUtil.copy(item.get_batch_path())
            batch_path.set_filename(FileUtil.to_parquet("DEMO"))
            analytical_path = ObjectUtil.copy(item.get_analytical_path())
            stream_path = ObjectUtil.copy(item.get_stream_path())

            content = Content()
            content.set_schema(dataframe.schema)
            content.set_as_dataframe(dataframe)

            new_data = Data()
            new_data.set_batch_path(batch_path)
            new_data.set_analytical_path(analytical_path)
            new_data.set_stream_path(stream_path)
            new_data.set_content(content)

            result.append(new_data)

        return result

    def is_to_warehouse(self):
        analytical_configuration = AnalyticalFlowConfigurationFactory.get_instance().build()
        result = analytical_configuration.get_to_dataset() is not None
        return result
