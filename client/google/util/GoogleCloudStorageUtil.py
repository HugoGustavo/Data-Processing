class GoogleCloudStorageUtil(object):

    @staticmethod
    def clean(bucket_path):
        if bucket_path is None:
            return None
        bucket_path = str(bucket_path).strip()
        return bucket_path

    @staticmethod
    def build_bucket(path):
        path = str(path).split('/')
        result = path[0]
        return result

    @staticmethod
    def build_blob(path):
        path = str(path).split('/')
        path = path[1:]  # remove bucket name
        result = '/'.join(path)
        return result

    @staticmethod
    def build_prefix(path):
        path = str(path).split('/')
        path = path[1:]  # remove bucket name
        if '.' in path[-1]:
            path = path[:-1]  # remove file name
        result = '/'.join(path)
        return result

    @staticmethod
    def remove_protocol(path):
        if path is None:
            return None
        result = str(path).replace("gs://", "")
        return result
