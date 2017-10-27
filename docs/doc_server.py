import socket

from livereload import Server
from livereload import shell
server = Server()
server.watch('./docs/source/', shell('make docs'))
server.serve(root='./docs/build/html', host=socket.getfqdn())
