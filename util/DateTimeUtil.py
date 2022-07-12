from datetime import datetime


class DateTimeUtil(object):
    @staticmethod
    def get_current_year():
        current_time = datetime.now()
        result = current_time.year
        return result

    @staticmethod
    def get_current_month():
        current_time = datetime.now()
        result = current_time.month
        return result

    @staticmethod
    def get_current_day():
        current_time = datetime.now()
        result = current_time.day
        return result

    @staticmethod
    def parse(value, mask='%Y-%m-%dT%H:%M:%S.%fZ'):
        try:
            result = datetime.strptime(value, mask)
        except:
            result = None
        return result
