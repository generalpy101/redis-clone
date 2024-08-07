from redis_clone.parser.redis_parser import Protocol_2_Data_Types, PROTOCOL_SEPARATOR


class ResponseBuilder:
    """
    Builds the response that will be sent to the client
    Data is encoded according to the Redis protocol and types are converted to bytes
    """

    def __init__(self, protocol_version=2) -> None:
        self.protocol_version = protocol_version

    def respond_with_ok(self):
        """
        Respond with ok
        """
        return self.build_response("OK", Protocol_2_Data_Types.SIMPLE_STRING)

    def build_response(self, type, data=None):
        """
        Build response according to protocol version
        """
        if self.protocol_version == 2:
            return self._build_protocol_2_response(type, data)
        else:
            raise Exception("Protocol version not supported")

    def _build_protocol_2_response(self, data_type, data):
        """
        Build response according to protocol version 2
        """
        function_dict = {
            Protocol_2_Data_Types.ERROR: self._build_protocol_2_error,
            Protocol_2_Data_Types.SIMPLE_STRING: self._build_protocol_2_simple_string,
            Protocol_2_Data_Types.BULK_STRING: self._build_protocol_2_bulk_string,
            Protocol_2_Data_Types.INTEGER: self._build_protocol_2_integer,
        }
        if data_type not in function_dict:
            raise Exception("Invalid response type")
        return function_dict[data_type](data)

    def _build_protocol_2_error(self, data):
        """
        Errors are used in order to signal client errors.
        They are encoded in the following way:
        -<data>\r\n
        """
        # Syntax of error is -<data>
        # So data is second element after error specifier
        data = b"-" + data.encode("utf-8") + PROTOCOL_SEPARATOR
        return data

    def _build_protocol_2_simple_string(self, data):
        """
        Simple Strings are used to transmit non binary safe strings with minimal overhead.
        They are encoded in the following way:
        +<data>\r\n
        """
        # Syntax of simple string is +<data>
        # So data is second element after simple string specifier
        data = b"+" + data.encode("utf-8") + PROTOCOL_SEPARATOR
        return data

    def _build_protocol_2_bulk_string(self, data):
        """
        Bulk Strings are used in order to represent a single binary safe string up to 512 MB in length.
        They are encoded in the following way:
        $<data length>\r\n
        For example, "foobar" is encoded as "$6\r\nfoobar\r\n".
        For nil values bulk strings are encoded with $-1\r\n
        """

        # If data is None then return nil value
        if data is None:
            return b"$-1" + PROTOCOL_SEPARATOR
        else:
            # Syntax of bulk string is $<data length>
            # So data is second element after bulk string specifier
            length = str(len(data)).encode("utf-8")
            data = b"$" + length + PROTOCOL_SEPARATOR + data.encode("utf-8") + PROTOCOL_SEPARATOR

            return data
        
    def _build_protocol_2_integer(self, data):
        """
        Integers are used in order to represent whole numbers between -(2^63) and 2^63-1.
        They are encoded in the following way:
        :<data>\r\n
        """
        # Syntax of integer is :<data>
        # So data is second element after integer specifier
        data = b":" + str(data).encode("utf-8") + PROTOCOL_SEPARATOR
        return data
