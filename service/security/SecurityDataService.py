from util.Logger import Logger
from util.ListUtil import ListUtil
from util.StringUtil import StringUtil
from factory.job.JobConfigurationFactory import JobConfigurationFactory


class SecurityDataService(object):
    def __init__(self, data_service):
        self.__data_service = data_service

    def count(self):
        result = self.__data_service.count()
        return result

    def delete(self, data):
        job_configuration = JobConfigurationFactory.get_instance().build()

        is_not_allowed_to_delete = not job_configuration.is_allow_delete()
        if is_not_allowed_to_delete:
            job_id = job_configuration.get_id()
            message = '{0} does not have permissions to delete data.'.format(job_id)
            Logger.warn(message)
            return None

        for item in ListUtil.get_as_list(data):
            batch_path = item.get_batch_path()
            batch_path.set_bucket(StringUtil.clean(job_configuration.get_from_bucket()))

        result = self.__data_service.delete(data)
        return result

    def delete_all(self, data):
        job_configuration = JobConfigurationFactory.get_instance().build()

        is_not_allowed_to_delete = job_configuration.is_allow_delete()
        if is_not_allowed_to_delete:
            job_id = job_configuration.get_id()
            message = '{0} does not have permissions to delete data.'.format(job_id)
            Logger.warn(message)
            return None

        for item in ListUtil.get_as_list(data):
            batch_path = item.get_batch_path()
            batch_path.set_bucket(StringUtil.clean(job_configuration.get_from_bucket()))

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
        result = self.__data_service.save(data)
        return result

    def save_all(self, data):
        result = self.__data_service.save_all(data)
        return result
