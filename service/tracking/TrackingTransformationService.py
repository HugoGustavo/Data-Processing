from util.ListUtil import ListUtil
from util.SparkUtil import SparkUtil
from util.ObjectUtil import ObjectUtil


class TrackingTransformationService(object):
    def __init__(self, transformation_service):
        self.__transformation_service = transformation_service

    def execute(self, data):
        result = self.__transformation_service.execute(data)

        if ObjectUtil.is_same(result, data):
            return result

        for item in ListUtil.get_as_list(result):
            content = item.get_content()
            dataframe = content.get_as_dataframe()
            dataframe = dataframe.withColumn("LAST_MODIFIED", SparkUtil.get_current_timestamp())
            content.set_as_dataframe(dataframe)
            item.set_content(content)

        return result
