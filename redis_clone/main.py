import socket
import sys
import os
import time
import logging


from enum import Enum
from redis_clone.redis_parser import Parser, Protocol_2_Data_Types
from redis_clone.response_builder import ResponseBuilder

logger = logging.getLogger(__name__)

class Protocol_2_Commands(Enum):
    '''
    Some common redis commands
    '''
    SET = 'SET'
    GET = 'GET'
    DEL = 'DEL'
    EXISTS = 'EXISTS'
    INCR = 'INCR'
    DECR = 'DECR'
    PING = 'PING'
    ECHO = 'ECHO'
    

class RedisServer:
    def __init__(self, host, port) -> None:
        self.host = host
        self.port = port
        self.parser = Parser(protocol_version=2)
        self.response_builder = ResponseBuilder(protocol_version=2)
        self.data_store = {}
        
    def start(self):
        logger.info('Starting server...')
        self._create_socket()
        self._bind_socket()
        self._listen()
        self._accept_connections()
        
    def _create_socket(self):
        logger.info('Creating socket...')
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error as e:
            logger.error(f'Error creating socket: {e}')
            sys.exit(1)
    
    def _bind_socket(self):
        logger.info(f'Binding socket to {self.host}:{self.port}')
        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind((self.host, self.port))
        except socket.error as e:
            logger.error(f'Error binding socket: {e}')
            sys.exit(1)
            
    def _listen(self):
        logger.info('Listening...')
        self.socket.listen(5)
    
    def _accept_connections(self):
        logger.info('Accepting connections...')
        while True:
            conn, addr = self.socket.accept()
            logger.info(f'Connection established with {addr}')
            self._handle_connection(conn, addr)

    def _handle_connection(self, conn, addr):
        while True:
            data = conn.recv(1024)
            if not data:
                break
            logger.info(f'Received data: {data}')
            # Convert bytes to string
            data = data.decode('utf-8')
            command_name, command_args = self.parser.parse_client_request(data)
            logger.info(f'Command name: {command_name}')
            logger.info(f'Command args: {command_args}')
            response = self._process_command(command_name, command_args)
            logger.info(f'Response: {response}')
            conn.sendall(response)
        conn.close()

    def _process_command(self, command_name, command_args):
        # Convert command name to uppercase
        command_name = command_name.upper()
        if command_name == Protocol_2_Commands.PING.value:
            return self.response_builder.build_response(Protocol_2_Data_Types.SIMPLE_STRING, 'PONG')
        if command_name == Protocol_2_Commands.ECHO.value:
            # Echo command returns the same string
            if len(command_args) == 0:
                return self.response_builder.build_response(Protocol_2_Data_Types.ERROR, 'ERR wrong number of arguments for \'ECHO\' command')
            return self.response_builder.build_response(Protocol_2_Data_Types.SIMPLE_STRING, " ".join(command_args))
        return self.response_builder.build_response(Protocol_2_Data_Types.ERROR, 'ERR unknown command \'{}\''.format(command_name))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    server = RedisServer(host='localhost', port=9999)
    server.start()