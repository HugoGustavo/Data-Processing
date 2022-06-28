from util.ListUtil import ListUtil
from util.StringUtil import StringUtil
from util.ObjectUtil import ObjectUtil
from factory.flow.BatchFlowConfigurationFactory import BatchFlowConfigurationFactory
from factory.flow.StreamFlowConfigurationFactory import StreamFlowConfigurationFactory
from factory.flow.AnalyticalFlowConfigurationFactory import AnalyticalFlowConfigurationFactory


class DataService(object):
    def __init__(self, data_lake_proxy, data_warehouse_proxy, data_stream_proxy):
        self.__data_lake_proxy = data_lake_proxy
        self.__data_warehouse_proxy = data_warehouse_proxy
        self.__data_stream_proxy = data_stream_proxy

    def count(self):
        batch_flow_configuration = BatchFlowConfigurationFactory.get_instance().build()
        analytical_flow_configuration = AnalyticalFlowConfigurationFactory.get_instance().build()

        from_data_lake = StringUtil.is_not_empty(batch_flow_configuration.get_from_bucket())
        from_data_analytical = StringUtil.is_not_empty(analytical_flow_configuration.get_from_dataset())

        result = 0
        if from_data_lake:
            result += self.__data_lake_proxy.count()
        if from_data_analytical:
            result += self.__data_warehouse_proxy.count()

        return result

    def delete(self, data):
        if ObjectUtil.is_none(data):
            raise ValueError('Data cannot be None')

        from_data_lake = ObjectUtil.is_not_none(data.get_batch_path())
        from_data_analytical = ObjectUtil.is_not_none(data.get_analytical_path())

        result = None
        if from_data_lake:
            result = ObjectUtil.get_default_if_none(self.__data_lake_proxy.delete(data), result)
        if from_data_analytical:
            result = ObjectUtil.get_default_if_none(self.__data_warehouse_proxy.delete(data), result)

        return result

    def delete_all(self, data):
        data = ListUtil.get_none_as_empty(data)
        data = ListUtil.remove_none(data)
        result = ListUtil.map(lambda item: self.delete(item), data)
        result = ListUtil.get_none_as_empty(result)
        result = ListUtil.remove_none(result)
        return result

    def exists(self, data):
        if ObjectUtil.is_none(data):
            raise ValueError('Data cannot be None')

        on_data_lake = ObjectUtil.is_not_none(data.get_batch_path())
        on_data_analytical = ObjectUtil.is_not_none(data.get_analytical_path())

        result = False
        if on_data_lake:
            result = result or self.__data_lake_proxy.exists(data)
        if on_data_analytical:
            result = result or self.__data_warehouse_proxy.exists(data)

        return result

    def find(self, data):
        if ObjectUtil.is_none(data):
            raise ValueError('Data cannot be None')

        on_data_lake = ObjectUtil.is_not_none(data.get_batch_path())
        on_data_analytical = ObjectUtil.is_not_none(data.get_analytical_path())

        result = None
        if on_data_lake:
            result = self.__data_lake_proxy.find(data)
        if on_data_analytical:
            result = self.__data_warehouse_proxy.find(data)

        return result

    def find_all(self):
        batch_flow_configuration = BatchFlowConfigurationFactory.get_instance().build()
        analytical_flow_configuration = AnalyticalFlowConfigurationFactory.get_instance().build()

        on_data_lake = StringUtil.is_not_empty(batch_flow_configuration.get_from_bucket())
        on_data_analytical = StringUtil.is_not_empty(analytical_flow_configuration.get_from_dataset())

        result = ListUtil.get_none_as_empty(None)
        if on_data_lake:
            result.extend(self.__data_lake_proxy.find_all())
        if on_data_analytical:
            result.extend(self.__data_warehouse_proxy.find_all())

        result = ListUtil.get_none_as_empty(result)
        result = ListUtil.remove_none(result)

        return result

    def save(self, data):
        if ObjectUtil.is_none(data):
            raise ValueError('Data cannot be None')

        batch_flow_configuration = BatchFlowConfigurationFactory.get_instance().build()
        analytical_flow_configuration = AnalyticalFlowConfigurationFactory.get_instance().build()

        to_data_lake = StringUtil.is_not_empty(batch_flow_configuration.get_to_bucket())
        to_data_analytical = StringUtil.is_not_empty(analytical_flow_configuration.get_to_dataset())

        result = None
        if to_data_lake:
            result = self.__data_lake_proxy.save(data)
        if to_data_analytical:
            result = self.__data_warehouse_proxy.save(data)

        return result

    def save_all(self, data):
        data = ListUtil.get_none_as_empty(data)
        data = ListUtil.remove_none(data)
        result = ListUtil.map(lambda item: self.save(item), data)
        result = ListUtil.get_none_as_empty(result)
        result = ListUtil.remove_none(result)
        return result
