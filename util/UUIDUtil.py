import uuid
import random
import string


class UUIDUtil(object):
    @staticmethod
    def generate(version='v4'):
        if 'v1' in version:
            result = str(uuid.uuid1()).strip()
        elif 'v3' in version:
            result = str(uuid.uuid3()).strip()
        elif 'v4' in version:
            result = str(uuid.uuid4()).strip()
        elif 'v5' in version:
            result = str(uuid.uuid5()).strip()
        else:
            raise ValueError('Invalid uuid version')
        first_letter = str(random.choice(string.ascii_letters)).lower()
        result = first_letter + result[1:]
        return result
