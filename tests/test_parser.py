# Using pytest for tests
from redis_clone.redis_parser import Parser, Protocol_2_Data_Types

def test_parse_client_request():
    '''
    Test for parsing client request
    '''
    parser = Parser(protocol_version=2)
    
    # Test initial connection
    test_str = '*1\r\n$7\r\nCOMMAND\r\n'
    command, args = parser.parse_client_request(test_str)
    
    assert command == 'COMMAND'
    assert args == []