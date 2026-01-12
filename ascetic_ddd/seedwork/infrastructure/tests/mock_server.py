# Standard library imports...
import socket
import typing
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer


def get_free_port():
    s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    address, port = s.getsockname()
    s.close()
    return port


def start_mock_server(port: int, request_handler: typing.Type[BaseHTTPRequestHandler]) -> HTTPServer:
    mock_server = HTTPServer(('localhost', port), request_handler)
    mock_server_thread = Thread(target=mock_server.serve_forever)
    mock_server_thread.setDaemon(True)
    mock_server_thread.start()
    return mock_server
