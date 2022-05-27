import glob
import uuid
import string
import random


class JobUtil:

    @staticmethod
    def generate_name():
        result = glob.glob("/tmp/job-*")
        if result is None or result == []:
            result = str(uuid.uuid4()).strip()
            first_letter = str(random.choice(string.ascii_letters)).lower()
            result = first_letter + result[1:]
            result = 'job-' + result
        else:
            result = result[0].split('/')[2]
        return result
