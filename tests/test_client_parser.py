# Using pytest for tests
from redis_clone.redis_parser import Parser, Protocol_2_Data_Types

class TestParserClient:
    
    def test_initial_command_request(self):
        '''
        Test initial COMMAND request
        '''
        parser = Parser(protocol_version=2)
        
        # Test initial connection
        test_str = '*1\r\n$7\r\nCOMMAND\r\n'
        command, args = self.parser.parse_client_request(test_str)
        
        assert command == 'COMMAND'
        assert args == []
    
    def test_set_command_request(self):
        '''
        Test SET command request
        '''        
        # Test initial connection
        test_str = '*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n'
        command, args = self.parser.parse_client_request(test_str)

        assert command == 'SET'
        assert args == ['mykey', 'myvalue']
        
    def test_get_command_request(self):
        '''
        Test GET command request
        '''        
        # Test initial connection
        test_str = '*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n'
        command, args = self.parser.parse_client_request(test_str)

        assert command == 'GET'
        assert args == ['mykey']
    
    def setup_method(self):
        self.parser = Parser(protocol_version=2)