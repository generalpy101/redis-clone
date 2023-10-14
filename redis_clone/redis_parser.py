from enum import Enum

PROTOCOL_SEPARATOR = '\r\n'

class Protocol_2_Data_Types(Enum):
    '''
    For Simple Strings, the first byte of the reply is "+"
    For Errors, the first byte of the reply is "-"
    For Integers, the first byte of the reply is ":"
    For Bulk Strings, the first byte of the reply is "$"
    For Arrays, the first byte of the reply is "*"
    '''
    SIMPLE_STRING = '+'
    ERROR = '-'
    INTEGER = ':'
    BULK_STRING = '$'
    ARRAY = '*'
    
class Parser:
    def __init__(self, protocol_version) -> None:
        self.protocol_version = protocol_version
    
    def parse_client_request(self, data):
        '''
        This function parses the client request and returns the command name and arguments
        '''
        if self.protocol_version == 2:
            return self._parse_v2_client_request(data)
        else:
            raise Exception('Protocol version not supported')
    
    def _parse_v2_client_request(self, data):
        '''
        Implementing the RESP2 protocol ref: https://redis.io/docs/reference/protocol-spec/#resp-versions
        Commands are Array of Bulk Strings
        Syntax for arrays is: *<number-of-elements>\r\n<element-1>...<element-n>
        Where each element has its own type specifier
        Syntax for Bulk Strings is: $<length>\r\n<data>\r\n
        Where length is the number of bytes in data
        '''
        if not data:
            return None
        # Check if first byte is an array specifier else raise exception
        if data[0] != Protocol_2_Data_Types.ARRAY.value:
            raise Exception('Invalid protocol data')
        
        # Split data according to separator of protocol
        # We'll split only once because we need to get number of elements in array
        command_items = data.split(PROTOCOL_SEPARATOR, 1)
        
        # Get number of elements in array
        # First item will be * rest should be number of elements
        num_elements = int(command_items[0][1:])
        
        # Get command name
        # Syntax of command is <command-name> <arg1> <arg2> ... <argn>
        # So command name is first element after array specifier
        # But we have both command name and arguments in the same array
        # But we also know that command will be like $<length>\r\n<command-name>\r\n<text>...
        # We need first 2 elements after array specifier as full string for parsing command name as we'll use data parser
        command_name = self.parse_data('\r\n'.join(command_items[1].split(PROTOCOL_SEPARATOR)[:2]))
        
        # Get command arguments
        # Syntax of command is <command-name> <arg1> <arg2> ... <argn>
        # So command arguments are elements after command name
        # But here we have both command name and arguments in the same array
        # Since args are also bulk strings, we need is full string for parsing command args as we'll use data parser
        # After command name we have $<length>\r\n<arg1>\r\n$<length>\r\n<arg2>\r\n...
        # For data parser, we need to 2 items each, length and data
        command_args = []
        unparsed_args = [
            '\r\n'.join(command_items[1].split(PROTOCOL_SEPARATOR)[i:i+2]) for i in range(2, (num_elements * 2)-1, 2)
        ]
        
        for arg in unparsed_args:
            command_args.append(self.parse_data(arg))
        
        return command_name, command_args
    
    def parse_data(self, data):
        '''
        Parses normal redis data and returns the parsed to python data type
        
        Data format differs based on the type of data but general syntax is
        <type>[data-specific-fields\r\n]<data>\r\n
        '''
        if self.protocol_version != 2:
            raise Exception('Protocol version not supported')
        
        # Get first byte of data to determine type
        data_type = data[0]
        
        # Using simple if else ladder because data types are mutually exclusive
        if data_type == Protocol_2_Data_Types.SIMPLE_STRING.value:
            return self._parse_simple_string(data)
        elif data_type == Protocol_2_Data_Types.ERROR.value:
            return self._parse_error(data)
        elif data_type == Protocol_2_Data_Types.INTEGER.value:
            return self._parse_integer(data)
        elif data_type == Protocol_2_Data_Types.BULK_STRING.value:
            return self._parse_bulk_string(data)
        elif data_type == Protocol_2_Data_Types.ARRAY.value:
            return self._parse_array(data)
        else:
            raise Exception('Invalid protocol data')
        
    def _parse_simple_string(self, data):
        '''
        Simple Strings are used to transmit non binary safe strings with minimal overhead.
        They are encoded in the following way:
        +<data>\r\n
        '''
        # Split data according to separator of protocol
        data_items = data.split(PROTOCOL_SEPARATOR)
        
        # Get data
        # Syntax of simple string is +<data>
        # So data is second element after simple string specifier
        data = data_items[0][1:]
        
        return data
    
    def _parse_error(self, data):
        '''
        Errors are used in order to signal client errors.
        They are encoded in the following way:
        -<data>\r\n
        '''
        # Split data according to separator of protocol
        data_items = data.split(PROTOCOL_SEPARATOR)
        
        # Get data
        # Syntax of error is -<data>
        # So data is second element after error specifier
        data = data_items[0][1:]
        
        return data

    def _parse_integer(self, data):
        '''
        Integers are used in order to transmit integers from the Redis server to the client.
        They are encoded in the following way:
        :[<+|->]<value>\r\n
        An optional plus (+) or minus (-) as the sign.
        '''
        # Split data according to separator of protocol
        data_items = data.split(PROTOCOL_SEPARATOR)
        
        # Get data
        # Syntax of integer is :[<+|->]<value>
        # So data is second element after integer specifier
        data = data_items[0][1:]
        
        return int(data)
    
    def _parse_bulk_string(self, data):
        '''
        Bulk Strings are used in order to represent a single binary safe string up to 512 MB in length.
        They are encoded in the following way:
        $<length>\r\n<data>\r\n
        Where length is the number of bytes in data
        '''
        
        # Split data according to separator of protocol
        data_items = data.split(PROTOCOL_SEPARATOR)
        
        # Get length
        # Syntax of bulk string is $<length>
        # So length is second element after bulk string specifier
        length = int(data_items[0][1:])
        
        # Get data
        # Syntax of bulk string is $<length>\r\n<data>\r\n
        # So data is third element after bulk string specifier
        data = data_items[1]
        
        # Check if length of data is same as length specified
        if len(data) != length:
            raise Exception('Invalid protocol data')
        
        return data
    
    def _parse_array(self, data):
        '''
        Arrays are used in order to represent a list of other RESP data types.
        They are encoded in the following way:
        *<number-of-elements>\r\n<element-1>...<element-n>
        Where each element has its own type specifier
        '''
        # Split data according to separator of protocol
        data_items = data.split(PROTOCOL_SEPARATOR)
        
        # Get number of elements in array
        # First item will be * rest should be number of elements
        num_elements = int(data_items[0][1:])
        
        # Get elements
        # Syntax of array is *<number-of-elements>\r\n<element-1>...<element-n>
        # So elements are from second element after array specifier to end
        # We need to parse each element
        elements = []
        for element in data_items[1:]:
            elements.append(self.parse_data(element))
        
        return elements
