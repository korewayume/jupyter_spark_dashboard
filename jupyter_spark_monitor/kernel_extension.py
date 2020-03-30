# -*- coding: utf-8 -*-
import os
import json
import socket
from threading import Thread
from ipykernel.comm import Comm
# import logging
#
# logger = logging.getLogger(__name__)
# fh = logging.FileHandler("kernel_extension.log")
# fh.setFormatter(logging.Formatter(fmt='[%(asctime)s %(name)s] %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
# logger.setLevel(logging.DEBUG)
# logger.addHandler(fh)


class SocketServer(Thread):
    SEP = ';EOD:'

    def __init__(self):
        super().__init__(name="SocketServer")
        self.m_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.m_socket.bind(("localhost", 0))
        self.m_socket.listen(5)
        self.port = self.m_socket.getsockname()[1]

    def run(self):
        while True:
            client, addr = self.m_socket.accept()
            buffered_message = ''

            appId = ''
            appName = ''
            sparkUser = ''
            numCores = 0
            totalCores = 0

            while client:
                message = client.recv(4096).decode()
                if not message:
                    break
                buffered_message = buffered_message + message
                last_sep_pos = buffered_message.rfind(self.SEP)
                if last_sep_pos > 0:
                    send_message = buffered_message[:last_sep_pos]
                    buffered_message = buffered_message[last_sep_pos + 5:]
                else:
                    send_message = ""
                for msg in [json.loads(x) for x in send_message.split(self.SEP) if x]:
                    if msg['msgtype'] == "sparkApplicationStart":
                        try:
                            appId = msg['appId']
                            appName = msg['appName']
                            sparkUser = msg['sparkUser']
                        except Exception:
                            raise msg
                    elif msg['msgtype'] == "sparkExecutorAdded":
                        numCores = msg['numCores']
                        totalCores = msg['totalCores']
                    else:
                        msg['application'] = dict(
                            appId=appId,
                            appName=appName,
                            sparkUser=sparkUser,
                            numCores=numCores,
                            totalCores=totalCores,
                        )
                        comm = Comm('spark-monitor', data='open from server')
                        comm.send(data=msg)
            client.close()


def load_ipython_extension(ipy):
    socket_server = SocketServer()
    socket_server.start()
    os.environ['SPARKMONITOR_KERNEL_PORT'] = str(socket_server.port)


def unload_ipython_extension(ipy):
    pass
