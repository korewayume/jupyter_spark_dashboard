# -*- coding: utf-8 -*-
# import logging
#
# logger = logging.getLogger(__name__)
# fh = logging.FileHandler("kernel_extension.log")
# fh.setFormatter(logging.Formatter(fmt='[%(asctime)s %(name)s] %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
# logger.setLevel(logging.DEBUG)
# logger.addHandler(fh)


def load_ipython_extension(ipy):
    from .startup import init_extension
    init_extension(ipy)
    from .monitor import init_extension
    init_extension(ipy)
    from .magic import init_extension
    init_extension(ipy)


def unload_ipython_extension(ipy):
    pass
