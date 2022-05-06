import sys


class ArgumentsUtil:

    @staticmethod
    def get():
        arguments = sys.argv[1:]  # The first argument is the python file
        arguments = [str(item).strip() for item in arguments]
        keys = arguments[0::2]
        values = arguments[1::2]
        result = zip(keys, values)
        result = dict(result)
        return result
