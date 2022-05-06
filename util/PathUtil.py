class PathUtil:

    @staticmethod
    def is_folder(path):
        if input is None:
            return False
        result = str(path)
        result = result[-1] == '/'
        return result

    @staticmethod
    def is_not_folder(path):
        if input is None:
            return False
        result = str(path)
        result = result[-1] == '/'
        result = not result
        return result

    @staticmethod
    def is_file(path):
        if input is None:
            return False
        result = str(path)
        result = result[-1] != '/'
        return result

    @staticmethod
    def is_not_file(path):
        if input is None:
            return False
        result = str(path)
        result = result[-1] != '/'
        result = not result
        return result
