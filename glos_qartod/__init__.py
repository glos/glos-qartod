#!/usr/bin/env python
'''
glos_qartod/__init__.py
'''

import logging


__version__ = '0.0.1'


LOGGER = None


def get_logger():
    '''
    Returns an initialized logger
    '''
    global LOGGER
    if LOGGER is None:
        LOGGER = logging.getLogger(__name__)
    return LOGGER


