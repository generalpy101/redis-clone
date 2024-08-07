# Using pytest for tests
from redis_clone.parser.redis_parser import Parser, Protocol_2_Data_Types


class TestParserClient:
    def test_initial_command_request(self):
        """
        Test initial COMMAND request
        """
        parser = Parser(protocol_version=2)

        # Test initial connection
        test_str = b"*1\r\n$7\r\nCOMMAND\r\n"
        command, args = self.parser.parse(test_str)

        assert command == "COMMAND"
        assert args == []

    def test_set_command_request(self):
        """
        Test SET command request
        """
        test_str = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
        command, args = self.parser.parse(test_str)

        assert command == "SET"
        assert args == ["mykey", "myvalue"]

    def test_get_command_request(self):
        """
        Test GET command request
        """
        test_str = b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n"
        command, args = self.parser.parse(test_str)

        assert command == "GET"
        assert args == ["mykey"]
        
    def test_subargs_parsing(self):
        '''
        Some commands in redis supports optional subargs.
        eg: SET mykey myvalue EX 10 NX
        '''
        test_str = b"*6\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n$2\r\nEX\r\n$2\r\n10\r\n$2\r\nNX\r\n"
        command, args = self.parser.parse(test_str)
        
        print(args)
        assert command == "SET"
        assert args == ['mykey', 'myvalue', ('EX', '10'), ('NX', True)]

    def setup_method(self):
        self.parser = Parser(protocol_version=2)
