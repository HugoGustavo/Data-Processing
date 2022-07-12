import sys


class ArgumentsUtil:

    @staticmethod
    def get():
        arguments = sys.argv[1:]  # The first argument is the python file
        arguments = [str(item).strip() for item in arguments]
        # keys = arguments[0::2]
        # values = arguments[1::2]
        # result = zip(keys, values)
        # result = dict(result)
        result = dict()
        for item in arguments:
            key_and_values = item.split('=')
            key = key_and_values[0].strip()
            value = ''.join(key_and_values[1:]).strip()
            result[key] = value
        return result
