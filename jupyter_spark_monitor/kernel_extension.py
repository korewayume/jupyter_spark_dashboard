# -*- coding: utf-8 -*-
import os
import json
import socket
from threading import Thread
from ipykernel.comm import Comm
from pyspark.sql import DataFrame
from IPython.core import magic_arguments
from IPython.core.magic import (
    Magics,
    magics_class,
    line_cell_magic,
    needs_local_scope,
)


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


@magics_class
class PySparkDataFrameMagic(Magics):

    @line_cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--help', action='help', help="""Displays the help and usage text for this magic.""")
    @magic_arguments.argument('--limit', type=int, default=10, help="""Limit of DataFrame rows: (default: 10).""")
    @magic_arguments.argument('--output', type=str, default='html', help="""Output of DataFrame: html(default), csv.""")
    @needs_local_scope
    def pyspark_dataframe(self, line=None, cell=None, local_ns=None):
        try:
            args = magic_arguments.parse_argstring(self.pyspark_dataframe, line)
        except SystemExit:
            return
        if cell is None or not cell.strip():
            return
        code = "rv = {}".format(cell.strip())
        ns = {}
        glob = self.shell.user_ns
        # handles global vars with same name as local vars. We store them in conflict_globs.
        conflict_globs = {}
        if local_ns and cell is None:
            for var_name, var_val in glob.items():
                if var_name in local_ns:
                    conflict_globs[var_name] = var_val
            glob.update(local_ns)
        exec(code, glob, ns)
        rv = ns['rv']
        if conflict_globs:
            glob.update(conflict_globs)
        if not isinstance(rv, DataFrame):
            raise TypeError("value {!r} is not instance of {!r}".format(rv, DataFrame))
        if args.output == 'html':
            return rv.limit(args.limit).toPandas()
        else:
            print(rv.limit(args.limit).toPandas().to_csv(index=False))


def load_ipython_extension(ipy):
    ipy.register_magics(PySparkDataFrameMagic)
    socket_server = SocketServer()
    socket_server.start()
    os.environ['SPARKMONITOR_KERNEL_PORT'] = str(socket_server.port)


def unload_ipython_extension(ipy):
    pass
