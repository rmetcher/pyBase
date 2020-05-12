#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import logging.config


class RequireINFOTrue(logging.Filter):
    def filter(self, record):
        return record.levelno <= logging.INFO


class RequireERRORTrue(logging.Filter):
    def filter(self, record):
        return record.levelno >= logging.ERROR


config = {
    'version': 1,
    'formatters': {
        'simple': {
            'format': '[%(asctime)s][%(threadName)s:%(thread)d][%(name)s:%(levelname)s(%(lineno)d)]--[%(module)s:%(funcName)s]:%(message)s'
            # 'format': '%(asctime)s - %(levelname)s - %(filename)s - %(lineno)d - %(message)s',
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'simple'
        },
        "info_file_handler": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "simple",
            "filename": "eps-info.log",
            "maxBytes": 10485760,
            "backupCount": 5,
            "encoding": "utf8",
            'filters': ['require_info_true']
        },
        "error_file_handler": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "ERROR",
            "formatter": "simple",
            "filename": "eps-errors.log",
            "maxBytes": 10485760,
            "backupCount": 5,
            "encoding": "utf8",
            'filters': ['require_error_true']
        }
    },
    'filters': {
        'require_info_true': {
            '()': RequireINFOTrue
        },
        'require_error_true': {
            '()': RequireERRORTrue
        }
    },
    'loggers': {
        'in': {
            'handlers': ['info_file_handler', 'error_file_handler'],
            'level': 'INFO',
            # 'propagate': True,
        },
        'out': {
            'handlers': ['info_file_handler', 'error_file_handler'],
            'level': 'INFO',
        }
    }
}

logging.config.dictConfig(config)


def create_in_logger():
    logger = logging.getLogger('in')
    return logger


def create_out_logger():
    logger = logging.getLogger('out')
    return logger
