import uuid
import glob


class JobUtil:

    @staticmethod
    def generate_name():
        result = glob.glob("/tmp/job-*")
        if result is None or result == []:
            result = 'job-' + str(uuid.uuid4())
        else:
            result = result[0].split('/')[2]
        return result
    
