from enum import Enum

from redis_clone.parser.base import BaseParser

PROTOCOL_SEPARATOR = b'\r\n'

COMMANDS_METADATA = {
    "SET": {
        "EX": {"takes_value": True},
        "PX": {"takes_value": True},
        "EXAT": {"takes_value": True},
        "PXAT": {"takes_value": True},
        "NX": {"takes_value": False},
        "XX": {"takes_value": False},
        "KEEPTTL": {"takes_value": False},
        "GET": {"takes_value": False},
    }
}


class Protocol_2_Data_Types(Enum):
    """
    For Simple Strings, the first byte of the reply is "+"
    For Errors, the first byte of the reply is "-"
    For Integers, the first byte of the reply is ":"
    For Bulk Strings, the first byte of the reply is "$"
    For Arrays, the first byte of the reply is "*"
    """
    SIMPLE_STRING = b"+"
    ERROR = b"-"
    INTEGER = b":"
    BULK_STRING = b"$"
    ARRAY = b"*"


class Parser(BaseParser):
    def __init__(self, protocol_version) -> None:
        self.protocol_version = protocol_version

    def parse(self, data, *args, **kwargs):
        """
        This function parses the client request and returns the command name and arguments
        """
        if self.protocol_version == 2:
            return self._parse_v2_client_request(data)
        else:
            raise Exception("Protocol version not supported")

    def _parse_v2_client_request(self, data):
        """
        Implementing the RESP2 protocol ref: https://redis.io/docs/reference/protocol-spec/#resp-versions
        Commands are Array of Bulk Strings
        Syntax for arrays is: *<number-of-elements>\r\n<element-1>...<element-n>
        Where each element has its own type specifier
        Syntax for Bulk Strings is: $<length>\r\n<data>\r\n
        Where length is the number of bytes in data
        """
        if not data:
            return None

        if data[0:1] != Protocol_2_Data_Types.ARRAY.value:
            raise Exception("Invalid protocol data")

        num_elements = int(data[1:data.index(PROTOCOL_SEPARATOR)])
        remaining_data = data.split(PROTOCOL_SEPARATOR, num_elements * 2 + 1)[1:]
        
        # Convert only the command name to uppercase, leaving arguments in their original case.
        command_name = self._parse_bulk_string(remaining_data[0] + PROTOCOL_SEPARATOR + remaining_data[1]).upper()

        idx = 2
        command_args = []

        while idx < num_elements * 2:
            # Fetch the argument but keep its original casing.
            arg = self._parse_bulk_string(remaining_data[idx] + PROTOCOL_SEPARATOR + remaining_data[idx+1])
            
            # If the command is recognized and the argument is a subargument, uppercase it for consistent processing.
            if command_name in COMMANDS_METADATA and arg.upper() in COMMANDS_METADATA[command_name]:
                arg = arg.upper()  # Convert subarguments to uppercase.

                if COMMANDS_METADATA[command_name][arg]["takes_value"]:
                    idx += 2
                    if idx < num_elements * 2:
                        subarg_value = self._parse_bulk_string(remaining_data[idx] + PROTOCOL_SEPARATOR + remaining_data[idx+1])
                        command_args.append((arg, subarg_value))
                    else:
                        raise Exception(f"Expected value for subargument {arg}, but none provided.")
                else:
                    # Subargument does not take a value, so just append it to the command args.
                    # Adding True as a placeholder value to indicate that the subargument is present.
                    command_args.append((arg, True))
            
            else:
                command_args.append(arg)
            
            idx += 2

        return command_name, command_args

    def parse_data(self, data):
        """
        Parses normal redis data and returns the parsed to python data type

        Data format differs based on the type of data but general syntax is
        <type>[data-specific-fields\r\n]<data>\r\n
        """
        data_type = data[0]

        # Using dictionary mapping for performance
        parsing_funcs = {
            Protocol_2_Data_Types.SIMPLE_STRING: self._parse_simple_string,
            Protocol_2_Data_Types.ERROR: self._parse_error,
            Protocol_2_Data_Types.INTEGER: self._parse_integer,
            Protocol_2_Data_Types.BULK_STRING: self._parse_bulk_string,
            Protocol_2_Data_Types.ARRAY: self._parse_array,
        }
        
        return parsing_funcs[data_type](data)

    def _parse_simple_string(self, data):
        """
        Simple Strings are used to transmit non binary safe strings with minimal overhead.
        They are encoded in the following way:
        +<data>\r\n
        """
        return data[1:-2].decode('utf-8')

    def _parse_error(self, data):
        """
        Errors are used in order to signal client errors.
        They are encoded in the following way:
        -<data>\r\n
        """
        return data[1:-2].decode('utf-8')

    def _parse_integer(self, data):
        """
        Integers are used in order to transmit integers from the Redis server to the client.
        They are encoded in the following way:
        :[<+|->]<value>\r\n
        An optional plus (+) or minus (-) as the sign.
        """
        return int(data[1:-2].decode('utf-8'))

    def _parse_bulk_string(self, data):
        """
        Bulk Strings are used in order to represent a single binary safe string up to 512 MB in length.
        They are encoded in the following way:
        $<length>\r\n<data>\r\n
        Where length is the number of bytes in data
        """
        length = int(data[1:data.index(PROTOCOL_SEPARATOR)])
        # Get data from index after separator till length of data
        return data[data.index(PROTOCOL_SEPARATOR) + 2:data.index(PROTOCOL_SEPARATOR) + 2 + length].decode('utf-8')

    def _parse_array(self, data):
        """
        Arrays are used in order to represent a list of other RESP data types.
        They are encoded in the following way:
        *<number-of-elements>\r\n<element-1>...<element-n>
        Where each element has its own type specifier
        """
        num_elements = int(data[1:data.index(PROTOCOL_SEPARATOR)])
        remaining_data = data.split(PROTOCOL_SEPARATOR, num_elements * 2 + 1)[1:]
        # Need to parse each element in the array
        return [self.parse_data(remaining_data[i] + PROTOCOL_SEPARATOR + remaining_data[i+1]) for i in range(0, num_elements * 2, 2)]
