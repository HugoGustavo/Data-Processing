import json
import avro.schema


class SchemaUtil(object):

    @staticmethod
    def is_avro(value):
        try:
            if isinstance(value, dict):
                value = json.dumps(value)
            value = str(value).replace('"', '\"').strip()
            avro.schema.parse(value)
            result = True
        except Exception as exception:
            result = False
        return result

    @staticmethod
    def is_json(value):
        try:
            if isinstance(value, dict):
                value = json.dumps(value)
            value = str(value).strip().replace('"', '\"')
            json.loads(value)
            result = True
        except Exception as exception:
            result = False
        return result
