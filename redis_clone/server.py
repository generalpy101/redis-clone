import socket
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
            # Minimum 2 arguments required key and value
            if len(command_args) < 2:
                return self.response_builder.build_response(
                    Protocol_2_Data_Types.ERROR,
                    "ERR wrong number of arguments for 'SET' command",
                )
            key = command_args[0]
            value = command_args[1]

            # Even if key exists, redis will overwrite the value
            self.data_store[key] = value
            return self.response_builder.build_response(
                Protocol_2_Data_Types.SIMPLE_STRING, "OK"
            )
        elif command_name == Protocol_2_Commands.GET.value:
            # Only 1 argument required key
            if len(command_args) != 1:
                return self.response_builder.build_response(
                    Protocol_2_Data_Types.ERROR,
                    "ERR wrong number of arguments for 'GET' command",
                )
            key = command_args[0]
            if key not in self.data_store:
                return self.response_builder.build_response(
                    Protocol_2_Data_Types.BULK_STRING
                )

            return self.response_builder.build_response(
                Protocol_2_Data_Types.BULK_STRING, self.data_store[key]
            )

        return self.response_builder.build_response(
            Protocol_2_Data_Types.ERROR, "ERR unknown command '{}'".format(command_name)
        )

    def stop(self):
        logger.info("Stopping server...")
        self.server.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    server = RedisServer(host=HOST, port=PORT)
    asyncio.run(server.start())
