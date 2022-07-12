import os
import json


class GoogleCloudPlataformUtil(object):
    @staticmethod
    def get_project_id():
        if 'GOOGLE_CLOUD_PROJECT' in os.environ:
            project_id = os.environ['GOOGLE_CLOUD_PROJECT']
            project_id = str(project_id).strip()
            return project_id

        if 'GCP_PROJECT' in os.environ:
            project_id = os.environ['GCP_PROJECT']
            project_id = str(project_id).strip()
            return project_id

        if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
            with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], 'r') as service_account_file:
                credentials = json.load(service_account_file)
                project_id = credentials['project_id']
                project_id = str(project_id).strip()
                return project_id

        raise Exception('Failed to determine project_id')

    @staticmethod
    def is_none(value):
        if value is None:
            return True
        return False

    @staticmethod
    def is_string(value):
        if value is None:
            return False
        result = isinstance(value, str)
        return result

    @staticmethod
    def is_empty(value):
        if value is None:
            return True
        value = str(value).strip()
        result = '' == value
        return result
