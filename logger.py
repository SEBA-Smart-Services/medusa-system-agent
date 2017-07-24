"""
# logger.py

Revision 0.1.2

Clive Gross
Schneider Electric
2017

## License
Borrowed from GPLv3 personal project by Clive Gross. Same license applies.

## Description
Create a logger object to write logs for an application.

## Example

    ```
    from logger import Logger

    logger = Logger('path/to/logfile, loggername='myapp_log')
    logger.write('log started.')
    ```

## Todo

 * Fix loglevels

"""
import logging
from logging.handlers import RotatingFileHandler
import time


class Logger(object):

    def __init__(self, logfile, loggername='log', loglevel=6, maxBytes=1024*1024*2, backupCount=10):
        logger = logging.getLogger(loggername)
        logger.setLevel(logging.DEBUG)
        self.logfile = logfile
        handler = RotatingFileHandler(self.logfile, maxBytes=maxBytes, backupCount=backupCount)
        logger.addHandler(handler)
        self.set_loglevel(loglevel)
        self.logger = logger
        self.enabled = True
        self.write('Log started.')

    def set_enabled(self, enabled):
        """
        enable or disable logging
        """
        self.enabled = enabled

    def write(self, entry, level=7):
        if self.enabled:
            log_prefix = str(time.asctime()) + ': '
            if level == 7:
                self.logger.debug(log_prefix + entry)
            else:
                self.logger.error(log_prefix + entry)

    def set_loglevel(self, level=3):
        """
        The list of severities is defined by RFC 5424:
        See https://en.wikipedia.org/wiki/Syslog
        ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        Value   Severity    Description
        ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        0       Emergency   System is unusable.
        1       Alert         Action must be taken immediately.
        2       Critical    Critical conditions, such as hard device errors.
        3       Error       Error conditions.
        4       Warning     Warning conditions.
        5       Notice      Normal but significant conditions.
        6       Informational Informational messages.
        7       Debug       Debug-level messages.
        ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        """
        if level > 7:
            self.loglevel = 7
        elif level < 0:
            self.loglevel = 0
        else:
            self.loglevel = int(level)
