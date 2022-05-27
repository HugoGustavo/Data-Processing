class FileUtil:

    @staticmethod
    def remove_extension(filename):
        if filename is None:
            return None
        filename = str(filename).strip()
        filename = filename.split('.')[0]
        return filename

    @staticmethod
    def to_avro(filename):
        if filename is None:
            return None
        filename = str(filename).strip()
        filename = filename.split('.')[0]
        filename = filename + ".avro"
        return filename

    @staticmethod
    def to_parquet(filename):
        if filename is None:
            return None
        filename = str(filename).strip()
        filename = filename.split('.')[0]
        filename = filename + ".parquet"
        return filename

    @staticmethod
    def is_txt(filename):
        if filename is None:
            return False
        extension = str(".TXT")
        filename = str(filename).strip().upper()
        return extension in filename

    @staticmethod
    def is_csv(filename):
        if filename is None:
            return False
        extension = str(".CSV")
        filename = str(filename).strip().upper()
        return extension in filename

    @staticmethod
    def is_parquet(filename):
        if filename is None:
            return False
        extension = str(".PARQUET")
        filename = str(filename).strip().upper()
        return extension in filename

    @staticmethod
    def is_orc(filename):
        if filename is None:
            return False
        extension = str(".ORC")
        filename = str(filename).strip().upper()
        return extension in filename

    @staticmethod
    def is_avro(filename):
        if filename is None:
            return False
        extension = str(".AVRO")
        filename = str(filename).strip().upper()
        return extension in filename

    @staticmethod
    def is_json(filename):
        if filename is None:
            return False
        extension = str(".JSON")
        filename = str(filename).strip().upper()
        return extension in filename

    @staticmethod
    def is_xml(filename):
        if filename is None:
            return False
        extension = str(".XML")
        filename = str(filename).strip().upper()
        return extension in filename
