import time
import sys
import os
import asyncio
import logging


from enum import Enum
from redis_clone.redis_parser import Parser, Protocol_2_Data_Types
from redis_clone.response_builder import ResponseBuilder

logger = logging.getLogger(__name__)

HOST = os.environ.get("REDIS_HOST", "0.0.0.0")
PORT = os.environ.get("REDIS_PORT", 9999)


class Protocol_2_Commands(Enum):
    """
    Some common redis commands
    """

    SET = "SET"
    GET = "GET"
    DEL = "DEL"
    EXISTS = "EXISTS"
    INCR = "INCR"
    DECR = "DECR"
    PING = "PING"
    ECHO = "ECHO"
    
class ExpiryValue:
    def __init__(self, value, expiry_seconds=None, expiry_milliseconds=None, expiry_unix_timestamp_seconds=None, expiry_unix_timestamp_milliseconds=None) -> None:
        self.value = value
        self.expiry_seconds = time.time() + expiry_seconds if expiry_seconds else None
        self.expiry_milliseconds = time.time() * 1000 + expiry_milliseconds if expiry_milliseconds else None
        self.expiry_unix_timestamp_seconds = expiry_unix_timestamp_seconds
        self.expiry_unix_timestamp_milliseconds = expiry_unix_timestamp_milliseconds

    def get_value(self):
        if self.expiry_milliseconds:
            if self.expiry_milliseconds < int(time.time() * 1000):
                return None
        elif self.expiry_seconds:
            if self.expiry_seconds < int(time.time()):
                return None
        elif self.expiry_unix_timestamp_milliseconds:
            if self.expiry_unix_timestamp_milliseconds < int(time.time() * 1000):
                return None
        elif self.expiry_unix_timestamp_seconds:
            if self.expiry_unix_timestamp_seconds < int(time.time()):
                return None

        return self.value
    
    def get_expiry_seconds(self):
        return self.expiry_seconds

    def get_expiry_milliseconds(self):
        return self.expiry_milliseconds

    def get_expiry_unix_timestamp_seconds(self):
        return self.expiry_unix_timestamp_seconds
    
    def get_expiry_unix_timestamp_milliseconds(self):
        return self.expiry_unix_timestamp_milliseconds



class RedisServer:
    def __init__(self, host, port) -> None:
        self.host = host
        self.port = port
        self.parser = Parser(protocol_version=2)
        self.response_builder = ResponseBuilder(protocol_version=2)
        self.data_store = {}
        self.running = False

    async def start(self):
        logger.info("Starting server...")
        self.server = await asyncio.start_server(
            self._handle_connection, self.host, self.port
        )
        async with self.server:
            await self.server.serve_forever()

    async def _handle_connection(self, reader, writer):
        addr = writer.get_extra_info("peername")
        logger.info(f"Connection established with {addr}")

        while True:
            data = await reader.read(1024)
            if not data:
                break

            logger.info(f"Received data: {data}")
            command_name, command_args = self.parser.parse_client_request(data)
            logger.info(f"Command name: {command_name}")
            logger.info(f"Command args: {command_args}")
            response = self._process_command(command_name, command_args)
            logger.info(f"Response: {response}")
            writer.write(response)
            await writer.drain()

        logger.info(f"Connection closed with {addr}")
        writer.close()
        await writer.wait_closed()

    def _process_command(self, command_name, command_args) -> bytes:
        # Convert command name to uppercase
        command_name = command_name.upper()
        if command_name == Protocol_2_Commands.PING.value:
            return self.response_builder.build_response(
                Protocol_2_Data_Types.SIMPLE_STRING, "PONG"
            )
        elif command_name == Protocol_2_Commands.ECHO.value:
            # Echo command returns the same string
            if len(command_args) == 0:
                return self.response_builder.build_response(
                    Protocol_2_Data_Types.ERROR,
                    "ERR wrong number of arguments for 'ECHO' command",
                )
            return self.response_builder.build_response(
                Protocol_2_Data_Types.SIMPLE_STRING, " ".join(command_args)
            )
        elif command_name == Protocol_2_Commands.SET.value:
            return self._handle_set_command(command_args)

        elif command_name == Protocol_2_Commands.GET.value:
            # Only 1 argument required key
            if len(command_args) != 1:
                return self.response_builder.build_response(
                    Protocol_2_Data_Types.ERROR,
                    "ERR wrong number of arguments for 'GET' command",
                )
            key = command_args[0]
            value = None
            if key not in self.data_store:
                return self.response_builder.build_response(
                    Protocol_2_Data_Types.BULK_STRING
                )
            else:
                # Check the key is of type ExpiryValue
                # This is to ensure uniformity in the when setting and getting values
                if isinstance(self.data_store[key], ExpiryValue):
                    value = self.data_store[key].get_value()

            if value is None:
                self._delete_expired_key(key)
            
            return self.response_builder.build_response(
                Protocol_2_Data_Types.BULK_STRING, value
            )
        
        elif command_name == Protocol_2_Commands.DEL.value:
            # Minimum 1 argument required key
            if len(command_args) < 1:
                return self.response_builder.build_response(
                    Protocol_2_Data_Types.ERROR,
                    "ERR wrong number of arguments for 'DEL' command",
                )
            
            keys_deleted = 0
            for key in command_args:
                if key in self.data_store:
                    del self.data_store[key]
                    keys_deleted += 1 
            
            return self.response_builder.build_response(
                Protocol_2_Data_Types.INTEGER, keys_deleted
            )
                

        return self.response_builder.build_response(
            Protocol_2_Data_Types.ERROR, "ERR unknown command '{}'".format(command_name)
        )
        
    def _handle_set_command(self, command_args):
        # Minimum 2 arguments required key and value
        if len(command_args) < 2:
            return self.response_builder.build_response(
                Protocol_2_Data_Types.ERROR,
                "ERR wrong number of arguments for 'SET' command",
            )
        key = command_args[0]
        value = command_args[1]
        
        subarg_values = {
            "EX": None, # seconds
            "PX": None, # milliseconds
            "EXAT": None, # unix timestamp in seconds
            "PXAT": None, # unix timestamp in milliseconds
            "KEEPTTL": None, # keep the ttl of the key boolean
            "GET": None, # return the value of the key booelan
            "NX": None, # set if key does not exist boolean
            "XX": None, # set if key exists boolean
        }

        # Check set command has optional arguments
        if len(command_args) > 2:
            # Subargs are in format (arg, value)
            for subarg in command_args[2:]:
                subarg_values[subarg[0]] = subarg[1]
        
        # Process subargs
        # Check keepttl is not set with any other expiry subarg
        if subarg_values["KEEPTTL"] and (subarg_values["EX"] or subarg_values["PX"] or subarg_values["EXAT"] or subarg_values["PXAT"]):
            return self.response_builder.build_response(
                Protocol_2_Data_Types.ERROR,
                "ERR invalid expire command syntax",
            )
        
        # Return error if both NX and XX are set
        if subarg_values["NX"] and subarg_values["XX"]:
            return self.response_builder.build_response(
                Protocol_2_Data_Types.ERROR,
                "ERR XX and NX options at the same time are not compatible",
            )
        
        # Handle NX
        # NX -- Only set the key if it does not already exist.
        if subarg_values["NX"]:
            if key in self.data_store:
                return self.response_builder.build_response(
                    Protocol_2_Data_Types.BULK_STRING
                )
            else:
                return self._assign_key_to_value(key, value, subarg_values)
        
        # Handle XX
        # XX -- Only set the key if it already exists.
        if subarg_values["XX"]:
            if key not in self.data_store:
                return self.response_builder.build_response(
                    Protocol_2_Data_Types.BULK_STRING
                )
            else:
                return self._assign_key_to_value(key, value, subarg_values)
        
        # Handle GET
        # GET -- Return the value of key
        if subarg_values["GET"]:
            if key in self.data_store:
                return self.response_builder.build_response(
                    Protocol_2_Data_Types.BULK_STRING, self.data_store[key].get_value()
                )
            else:
                return self.response_builder.build_response(
                    Protocol_2_Data_Types.BULK_STRING
                )
        
        # Handle KEEPTTL
        # KEEPTTL -- Retain the time to live associated with the key.
        if subarg_values["KEEPTTL"]:
            if key in self.data_store:
                self.data_store[key] = ExpiryValue(
                    value=value,
                    expiry_seconds=self.data_store[key].get_expiry_seconds(),
                    expiry_milliseconds=self.data_store[key].get_expiry_milliseconds(),
                    expiry_unix_timestamp_seconds=self.data_store[key].get_expiry_unix_timestamp_seconds(),
                    expiry_unix_timestamp_milliseconds=self.data_store[key].get_expiry_unix_timestamp_milliseconds(),
                )
                
                return self.response_builder.build_response(
                    Protocol_2_Data_Types.SIMPLE_STRING, "OK"
                )
            else:
                return self.response_builder.build_response(
                    Protocol_2_Data_Types.BULK_STRING
                )
        
        # Normal case for set
        return self._assign_key_to_value(key, value, subarg_values)

    def _assign_key_to_value(self, key, value, subargs):
        try:
            self.data_store[key] = ExpiryValue(
                value=value,
                expiry_seconds=int(subargs["EX"]) if subargs["EX"] else None,
                expiry_milliseconds=int(subargs["PX"]) if subargs["PX"] else None,
                expiry_unix_timestamp_seconds=int(subargs["EXAT"]) if subargs["EXAT"] else None,
                expiry_unix_timestamp_milliseconds=int(subargs["PXAT"]) if subargs["PXAT"] else None,
            )
            return self.response_builder.build_response(
                Protocol_2_Data_Types.SIMPLE_STRING, "OK"
            )
        except ValueError:
            return self.response_builder.build_response(
                Protocol_2_Data_Types.ERROR,
                "ERR value is not an integer or out of range",
            )
            
    def _delete_expired_key(self, key):
        if key in self.data_store:
            del self.data_store[key]
    
    def stop(self):
        logger.info("Stopping server...")
        self.server.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    server = RedisServer(host=HOST, port=PORT)
    asyncio.run(server.start())
