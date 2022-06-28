from model.Data import Data
from model.Content import Content
from util.ListUtil import ListUtil
from util.ObjectUtil import ObjectUtil
from factory.job.JobConfigurationFactory import JobConfigurationFactory


class DeduplicationService(object):
    def __init__(self):
        pass

    def execute(self, data):
        job_configuration = JobConfigurationFactory.get_instance().build()
        have_not_deduplication = not job_configuration.have_deduplication()
        if have_not_deduplication:
            return data

        data = ListUtil.remove_none(ListUtil.get_as_list(data))

        result = ListUtil.get_none_as_empty(None)

        for item in data:
            batch_path = ObjectUtil.copy(item.get_batch_path())
            analytical_path = ObjectUtil.copy(item.get_analytical_path())
            stream_path = ObjectUtil.copy(item.get_stream_path())

            dataframe = item.get_content().get_as_dataframe()
            dataframe = dataframe.drop_duplicates() if dataframe else dataframe

            content = Content()
            content.set_as_dataframe(dataframe)
            content.set_schema(dataframe.schema)

            result.append(Data(batch_path, analytical_path, stream_path, content))

        return result
