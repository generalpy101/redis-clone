from hiredis import Reader

from redis_clone.parser.base import BaseParser


class HiRedisParser(BaseParser):
    def __init__(self):
        self.reader_class = Reader

    def parse(self, data, *args, **kwargs):
        reader = self.reader_class(*args, **kwargs)
        reader.feed(data)
        if data := reader.gets():
            command = data[0]
            arguments = data[1:]
            return command, arguments
        
        raise Exception("Invalid data received")