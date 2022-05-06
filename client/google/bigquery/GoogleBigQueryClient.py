from google.cloud import bigquery

from client.google.util.GoogleBigQueryUtil import GoogleBigQueryUtil
from client.google.util.GoogleCloudPlataformUtil import GoogleCloudPlataformUtil


class GoogleBigQueryClient(object):
    def __init__(self):
        self.__bigquery_client = bigquery.Client()

    def list(self, analytical_path):
        if GoogleCloudPlataformUtil.is_empty(analytical_path):
            raise ValueError('Analytical path cannot be empty')

        analytical_path = GoogleBigQueryUtil.clean(analytical_path)
        dataset = GoogleBigQueryUtil.build_dataset(analytical_path)
        tables = self.__bigquery_client.list_tables(dataset)
        tables = [str(table.table_id) for table in tables]
        tables = [table.split('.')[-1] for table in tables]

        result = [str(dataset) + '/' + str(table) for table in tables]

        return result

    def delete(self, analytical_path):
        if GoogleCloudPlataformUtil.is_empty(analytical_path):
            raise ValueError('Analytical path cannot be empty')

        analytical_path = GoogleBigQueryUtil.clean(analytical_path)
        project_id = GoogleCloudPlataformUtil.get_project_id()
        dataset = GoogleBigQueryUtil.build_dataset(analytical_path)
        table = GoogleBigQueryUtil.build_table(analytical_path)
        table = project_id + '.' + dataset + '.' + table

        self.__bigquery_client.delete_table(table, not_found_ok=False)

        return analytical_path

    def exists(self, analytical_path):
        if GoogleCloudPlataformUtil.is_empty(analytical_path):
            raise ValueError('Analytical path cannot be empty')

        analytical_path = GoogleBigQueryUtil.clean(analytical_path)
        project_id = GoogleCloudPlataformUtil.get_project_id()
        dataset = GoogleBigQueryUtil.build_dataset(analytical_path)
        table = GoogleBigQueryUtil.build_table(analytical_path)
        table = project_id + '.' + dataset + '.' + table

        try:
            self.__bigquery_client.get_table(table)
            result = True
        except Exception as exception:
            result = False

        return result

    def insert(self, analytical_path, content):
        if GoogleCloudPlataformUtil.is_empty(analytical_path):
            raise ValueError('Analytical path cannot be empty')
        if GoogleCloudPlataformUtil.is_empty(content):
            raise ValueError('Content cannot be empty')

        analytical_path = GoogleBigQueryUtil.clean(analytical_path)
        project_id = GoogleCloudPlataformUtil.get_project_id()
        dataset = GoogleBigQueryUtil.build_dataset(analytical_path)
        table = GoogleBigQueryUtil.build_table(analytical_path)
        table = project_id + '.' + dataset + '.' + table

        self.__bigquery_client.insert_rows_json(table, content)

        result = (analytical_path, content)

        return result

    def select(self, analytical_path, max_rows=None):
        if GoogleCloudPlataformUtil.is_empty(analytical_path):
            raise ValueError('Analytical path cannot be empty')

        analytical_path = GoogleBigQueryUtil.clean(analytical_path)
        project_id = GoogleCloudPlataformUtil.get_project_id()
        dataset = GoogleBigQueryUtil.build_dataset(analytical_path)
        table = GoogleBigQueryUtil.build_table(analytical_path)
        table = project_id + '.' + dataset + '.' + table

        result = self.__bigquery_client.list_rows(table, max_results=max_rows)
        result = list(result)

        return result
