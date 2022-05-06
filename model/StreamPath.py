from util.ObjectUtil import ObjectUtil
from util.StringUtil import StringUtil


class StreamPath(object):
    def __init__(self, topic=None, company=None, region=None, business_unit=None, vice_presidency=None, domain=None,
                 subdomain=None, context=None, pipeline=None, data_source=None, year=None, month=None, day=None,
                 execution=None, messageId=None):
        self.__topic = topic
        self.__company = company
        self.__region = region
        self.__business_unit = business_unit
        self.__vice_presidency = vice_presidency
        self.__domain = domain
        self.__subdomain = subdomain
        self.__context = context
        self.__pipeline = pipeline
        self.__data_source = data_source
        self.__year = year
        self.__month = month
        self.__day = day
        self.__execution = execution
        self.__messageId = messageId

    def get_topic(self):
        return self.__topic

    def set_topic(self, topic):
        self.__topic = topic

    def get_company(self):
        return self.__company

    def set_company(self, company):
        self.__company = company

    def get_region(self):
        return self.__region

    def set_region(self, region):
        self.__region = region

    def get_business_unit(self):
        return self.__business_unit

    def set_business_unit(self, business_unit):
        self.__business_unit = business_unit

    def get_vice_presidency(self):
        return self.__vice_presidency

    def set_vice_presidency(self, vice_presidency):
        self.__vice_presidency = vice_presidency

    def get_domain(self):
        return self.__domain

    def set_domain(self, domain):
        self.__domain = domain

    def get_subdomain(self):
        return self.__subdomain

    def set_subdomain(self, subdomain):
        self.__subdomain = subdomain

    def get_context(self):
        return self.__context

    def set_context(self, context):
        self.__context = context

    def get_pipeline(self):
        return self.__pipeline

    def set_pipeline(self, pipeline):
        self.__pipeline = pipeline

    def get_data_source(self):
        return self.__data_source

    def set_data_source(self, data_source):
        self.__data_source = data_source

    def get_year(self):
        return self.__year

    def set_year(self, year):
        self.__year = str(year).split('=')
        self.__year = int(self.__year[-1])
        self.__year = "{:04d}".format(self.__year)

    def get_month(self):
        return self.__month

    def set_month(self, month):
        self.__month = str(month).split('=')
        self.__month = int(self.__month[-1])
        self.__month = "{:02d}".format(self.__month)

    def get_day(self):
        return self.__day

    def set_day(self, day):
        self.__day = str(day).split('=')
        self.__day = int(self.__day[-1])
        self.__day = "{:02d}".format(self.__day)

    def get_execution(self):
        return self.__execution

    def set_execution(self, execution):
        self.__execution = execution

    def get_message_id(self):
        return self.__messageId

    def set_message_id(self, messageId):
        self.__messageId = messageId

    def is_empty(self):
        return ObjectUtil.is_none(self.__topic) \
               or ObjectUtil.is_none(self.__company) \
               or ObjectUtil.is_none(self.__region) \
               or ObjectUtil.is_none(self.__business_unit) \
               or ObjectUtil.is_none(self.__vice_presidency) \
               or ObjectUtil.is_none(self.__domain) \
               or ObjectUtil.is_none(self.__subdomain) \
               or ObjectUtil.is_none(self.__context) \
               or ObjectUtil.is_none(self.__pipeline) \
               or ObjectUtil.is_none(self.__data_source) \
               or ObjectUtil.is_none(self.__year) \
               or ObjectUtil.is_none(self.__month) \
               or ObjectUtil.is_none(self.__day) \
               or ObjectUtil.is_none(self.__execution) \
               or ObjectUtil.is_none(self.__messageId)

    def __str__(self):
        if self.is_empty():
            return ''

        result = str(self.__topic) + '/'
        result = result + str(self.__company) + '/'
        result = result + str(self.__region) + '/'
        result = result + str(self.__business_unit) + '/'
        result = result + str(self.__vice_presidency) + '/'
        result = result + str(self.__domain) + '/'
        result = result + str(self.__subdomain) + '/'
        result = result + str(self.__context) + '/'
        result = result + str(self.__pipeline) + '/'
        result = result + str(self.__data_source) + '/'
        result = result + 'yyyy=' + str(self.__year) + '/'
        result = result + 'mm=' + str(self.__month) + '/'
        result = result + 'dd=' + str(self.__day) + '/'
        result = result + str(self.__execution) + '/'

        if StringUtil.is_not_empty(self.__messageId):
            result = result + str(self.__messageId)

        return result
