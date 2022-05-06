from pyspark.sql.functions import current_timestamp


class SparkUtil:

    @staticmethod
    def get_current_timestamp():
        result = current_timestamp()
        return result
