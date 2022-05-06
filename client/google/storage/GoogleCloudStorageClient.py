from google.cloud import storage

from client.google.util.GoogleCloudStorageUtil import GoogleCloudStorageUtil
from client.google.util.GoogleCloudPlataformUtil import GoogleCloudPlataformUtil


class GoogleCloudStorageClient(object):
    def __init__(self):
        self.__storage_client = storage.Client()

    def list(self, batch_path, delimiter=None):
        if GoogleCloudPlataformUtil.is_empty(batch_path):
            raise ValueError('Batch path cannot be empty')

        batch_path = GoogleCloudStorageUtil.clean(batch_path)
        batch_path = GoogleCloudStorageUtil.remove_protocol(batch_path)
        bucket = GoogleCloudStorageUtil.build_bucket(batch_path)
        prefix = GoogleCloudStorageUtil.build_prefix(batch_path)

        blobs = self.__storage_client.list_blobs(bucket, prefix=prefix, delimiter=delimiter)
        result = [str(bucket) + '/' + str(blob.name) for blob in blobs]
        return result

    def count(self, batch_path, delimiter=None):
        if GoogleCloudPlataformUtil.is_empty(batch_path):
            raise ValueError('Batch path cannot be empty')

        batch_path = GoogleCloudStorageUtil.clean(batch_path)
        batch_path = GoogleCloudStorageUtil.remove_protocol(batch_path)
        bucket = GoogleCloudStorageUtil.build_bucket(batch_path)
        prefix = GoogleCloudStorageUtil.build_prefix(batch_path)

        blobs = self.__storage_client.list_blobs(bucket, prefix=prefix, delimiter=delimiter)
        result = [str(bucket) + '/' + str(blob.name) for blob in blobs]
        result = len(result)
        return result

    def delete(self, batch_path):
        if GoogleCloudPlataformUtil.is_empty(batch_path):
            raise ValueError('Batch path cannot be empty')

        batch_path = GoogleCloudStorageUtil.clean(batch_path)
        batch_path = GoogleCloudStorageUtil.remove_protocol(batch_path)
        bucket = GoogleCloudStorageUtil.build_bucket(batch_path)
        blob = GoogleCloudStorageUtil.build_blob(batch_path)

        bucket = self.__storage_client.bucket(bucket)
        blob = bucket.blob(blob)
        blob.delete()

        return batch_path

    def exists(self, batch_path):
        if GoogleCloudPlataformUtil.is_empty(batch_path):
            raise ValueError('Batch path cannot be empty')

        batch_path = GoogleCloudStorageUtil.clean(batch_path)
        batch_path = GoogleCloudStorageUtil.remove_protocol(batch_path)
        bucket = GoogleCloudStorageUtil.build_bucket(batch_path)
        blob = GoogleCloudStorageUtil.build_blob(batch_path)

        bucket = self.__storage_client.bucket(bucket)
        blob = bucket.get_blob(blob)
        result = False if blob is None else True

        return result

    def upload(self, batch_path, content):
        if GoogleCloudPlataformUtil.is_empty(batch_path):
            raise ValueError('Lake path cannot be empty')
        if GoogleCloudPlataformUtil.is_empty(content):
            raise ValueError('Content cannot be empty')

        batch_path = GoogleCloudStorageUtil.clean(batch_path)
        batch_path = GoogleCloudStorageUtil.remove_protocol(batch_path)
        bucket = GoogleCloudStorageUtil.build_bucket(batch_path)
        blob = GoogleCloudStorageUtil.build_blob(batch_path)

        bucket = self.__storage_client.bucket(bucket)
        blob = bucket.blob(blob)
        blob.upload_from_string(str(content))

        result = (batch_path, content)

        return result

    def download(self, batch_path):
        if GoogleCloudPlataformUtil.is_empty(batch_path):
            raise ValueError('Lake path cannot be empty')

        batch_path = GoogleCloudStorageUtil.clean(batch_path)
        batch_path = GoogleCloudStorageUtil.remove_protocol(batch_path)
        bucket = GoogleCloudStorageUtil.build_bucket(batch_path)
        blob = GoogleCloudStorageUtil.build_blob(batch_path)

        bucket = self.__storage_client.bucket(bucket)
        blob = bucket.blob(blob)
        content = blob.download_as_string()

        return content
