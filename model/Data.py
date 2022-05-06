class Data(object):
    def __init__(self, batch_path=None, analytical_path=None, stream_path=None, content=None):
        self.__batch_path = batch_path
        self.__analytical_path = analytical_path
        self.__stream_path = stream_path
        self.__content = content

    def get_batch_path(self):
        return self.__batch_path

    def set_batch_path(self, batch_path):
        self.__batch_path = batch_path

    def get_analytical_path(self):
        return self.__analytical_path

    def set_analytical_path(self, analytical_path):
        self.__analytical_path = analytical_path

    def get_stream_path(self):
        return self.__stream_path

    def set_stream_path(self, stream_path):
        self.__stream_path = stream_path

    def get_content(self):
        return self.__content

    def set_content(self, content):
        self.__content = content

    def __str__(self):
        result = '['
        result = result + 'batch_path=' + str(self.__batch_path) + ","
        result = result + 'analytical_path=' + str(self.__analytical_path) + ","
        result = result + 'stream_path=' + str(self.__stream_path) + ","
        result = result + 'content=' + str(self.__content)
        result = result + ']'
        return result
