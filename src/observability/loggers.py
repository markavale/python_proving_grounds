# pip install colorlog tzlocal

from datetime import datetime
import logging
import colorlog
import json
import tzlocal

__all__ = ["logger", "Logger"]

class Logger:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._initialize_logger()
        return cls._instance
    
    def _initialize_logger(self):
        """Initialize the logger with only a console handler."""
        self.logger = logging.getLogger('windscribe_vpn')
        self.logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create formatters and add it to the handler
        log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(log_format)
        
        # Add the handler to the logger
        self.logger.addHandler(console_handler)
    
    def get_logger(self):
        """Get the logger instance."""
        return self.logger 

class CustomFormatter(colorlog.ColoredFormatter):
    def __init__(self):
        super().__init__(
            '%(log_color)s%(asctime)s %(levelname)s %(funcName)s: %(message)s',
            datefmt="%Y-%m-%d %H:%M:%S %z",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            },
            reset=True,
            style="%",
        )


class JsonFormatter(logging.Formatter):
    def format(self, record):
        # Get the system's local timezone
        local_timezone = tzlocal.get_localzone()
        formatted_time = datetime.fromtimestamp(record.created, local_timezone).strftime('%Y-%m-%d %H:%M:%S %z')
        log_record = {
            "time": formatted_time,
            "log.level": record.levelname,
            "service.function.name": record.funcName,
            "message": record.getMessage(),
        }
        return json.dumps(log_record)


def add_coloured_handler(logger, use_json=False):
    if use_json:
        formatter = JsonFormatter()
    else:
        formatter = CustomFormatter()
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


def setup_custom_logger(name, use_json=False):
    logger = logging.getLogger(name)
    logger.handlers = []

    add_coloured_handler(logger, use_json)

    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    return logger


logger: logging.Logger = setup_custom_logger(__name__, use_json=True)
