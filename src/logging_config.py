import logging
import logging.config

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(message)s'
        },
    },
    'handlers': {
        'file': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'filename': 'pipeline.log',
            'formatter': 'standard'
        },
    },
    'root': {
        'handlers': ['file'],
        'level': 'DEBUG',
    }
}

def configure_logging():
    logging.config.dictConfig(LOGGING_CONFIG)
