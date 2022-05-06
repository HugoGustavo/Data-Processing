import os


class GoogleCloudManageResourceClient(object):
    def __init__(self):
        pass

    @staticmethod
    def get_project_id():
        result = os.getenv('GOOGLE_CLOUD_PROJECT')
        result = str(result).strip()
        return result
