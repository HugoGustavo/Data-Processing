import inspect
import datetime


class Logger:

    @staticmethod
    def __build_message(value, level='DEBUG'):
        timestamp = str(datetime.datetime.now())
        level = str('[' + str(level) + ']')
        clazz = str(inspect.stack()[2][1]).replace(".py", "")
        function = str(inspect.stack()[2][3])
        value = '' if value is None else ' : ' + str(value)
        message = timestamp + ' ' + level + ' - ' + clazz + '.' + function + value
        return message

    @staticmethod
    def debug(value):
        message = Logger.__build_message(value, 'DEBUG')
        print(message)

    @staticmethod
    def info(value):
        message = Logger.__build_message(value, 'INFO')
        print(message)

    @staticmethod
    def warn(value):
        message = Logger.__build_message(value, 'WARN')
        print(message)

    @staticmethod
    def error(value):
        message = Logger.__build_message(value, 'ERROR')
        print(message)

    @staticmethod
    def fatal(value):
        message = Logger.__build_message(value, 'FATAL')
        print(message)

    @staticmethod
    def off(value):
        message = Logger.__build_message(value, 'OFF')
        print(message)

    @staticmethod
    def trace(value):
        message = Logger.__build_message(value, 'TRACE')
        print(message)
