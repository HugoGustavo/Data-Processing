class GoogleBigQueryUtil(object):

    @staticmethod
    def build_dataset(path):
        if path is None:
            return None
        result = str(path).split('/')
        result = result[0]
        return result

    @staticmethod
    def build_table(path):
        if path is None:
            return None
        result = str(path).split('/')
        result = result[1]
        return result

    @staticmethod
    def is_empty(value):
        if value is None:
            return True
        if not value:  # value == []
            return True
        return False
