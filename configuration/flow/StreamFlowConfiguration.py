import copy


class StreamFlowConfiguration(object):
    def __init__(self, from_topic=None, to_topic=None, company=None, region=None, business_unit=None,
                 vice_presidency=None, domain=None, subdomain=None, context=None, pipeline=None, data_source=None,
                 year=None, month=None, day=None, execution=None, schema=None):
        self.__from_topic = from_topic
        self.__to_topic = to_topic
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
        self.__schema = schema

    def get_from_topic(self):
        result = copy.deepcopy(self.__from_topic)
        return result

    def get_to_topic(self):
        result = copy.deepcopy(self.__to_topic)
        return result

    def get_company(self):
        result = copy.deepcopy(self.__company)
        return result

    def get_region(self):
        result = copy.deepcopy(self.__region)
        return result

    def get_business_unit(self):
        result = copy.deepcopy(self.__business_unit)
        return result

    def get_vice_presidency(self):
        result = copy.deepcopy(self.__vice_presidency)
        return result

    def get_domain(self):
        result = copy.deepcopy(self.__domain)
        return result

    def get_subdomain(self):
        result = copy.deepcopy(self.__subdomain)
        return result

    def get_context(self):
        result = copy.deepcopy(self.__context)
        return result

    def get_pipeline(self):
        result = copy.deepcopy(self.__pipeline)
        return result

    def get_data_source(self):
        result = copy.deepcopy(self.__data_source)
        return result

    def get_year(self):
        result = copy.deepcopy(self.__year)
        return result

    def get_month(self):
        result = copy.deepcopy(self.__month)
        return result

    def get_day(self):
        result = copy.deepcopy(self.__day)
        return result

    def get_execution(self):
        result = copy.deepcopy(self.__execution)
        return result

    def get_schema(self):
        result = copy.deepcopy(self.__schema)
        return result

    def __str__(self):
        result = '['
        result = result + 'from_topic=' + str(self.__from_topic) + ','
        result = result + 'to_topic=' + str(self.__to_topic) + ','
        result = result + 'company=' + str(self.__company) + ','
        result = result + 'region=' + str(self.__region) + ','
        result = result + 'business_unit=' + str(self.__business_unit) + ','
        result = result + 'vice_presidency=' + str(self.__vice_presidency) + ','
        result = result + 'domain=' + str(self.__domain) + ','
        result = result + 'subdomain=' + str(self.__subdomain) + ','
        result = result + 'context=' + str(self.__context) + ','
        result = result + 'pipeline=' + str(self.__pipeline) + ','
        result = result + 'data_source=' + str(self.__data_source) + ','
        result = result + 'year=' + str(self.__year) + ','
        result = result + 'month=' + str(self.__month) + ','
        result = result + 'day=' + str(self.__day) + ','
        result = result + 'execution=' + str(self.__execution) + ','
        result = result + 'schema=' + str(self.__schema)
        result = result + ']'
        return result
