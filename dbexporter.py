import configparser
import pymssql
import csv
import time
import logging
from logging.handlers import RotatingFileHandler


class DBConnector(object):

    def __init__(self, config):
        self.config = DBConfigLoader(config)

    def raw_connect(self, timeout=600):
        """
        create a db connection using pymssql.connect()

        """
        conn = pymssql.connect(
        	server = self.config.hostname,
        	user = self.config.username,
        	password = self.config.password,
        	database = self.config.db,
        	timeout = timeout,
        	port = self.config.port
        )
        return conn

    def close(self):
        pass


class DBConfigLoader(object):
    """
    a config loader for the DBConnector class

    loads database connection configuration based on a configparser config object

    """

    def __init__(self, config):
        self.config = config
        self.config_section = 'WebReports'
        self.set_hostname()
        self.set_port()
        self.set_servername()
        self.set_db()
        self.set_driver()
        self.set_username()
        self.set_password()
        self.set_logfile()
        self.set_datastore()

    def set_hostname(self):
        self.hostname = self.config.get(self.config_section, 'hostname')

    def set_port(self):
        self.port = self.config.get(self.config_section, 'port')

    def set_servername(self):
        self.servername = self.config.get(self.config_section, 'servername')

    def set_db(self):
        self.db = self.config.get(self.config_section, 'db')

    def set_driver(self):
        self.driver = self.config.get(self.config_section, 'driver')

    def set_username(self):
        self.username = self.config.get(self.config_section, 'username')

    def set_password(self):
        self.password = self.config.get(self.config_section, 'password')

    def set_logfile(self):
        self.logfile = self.config.get('paths', 'logfile')

    def set_datastore(self):
        self.datastore = self.config.get('paths', 'datastore')


class DBTableExporter(object):

    """
    exports SQL queries to CSV

    """

    def __init__(self, config):
        self.connector = DBConnector(config=config)
        self.set_outdir()
        self.logging = False

    def set_outdir(self):
        self.outdir = self.connector.config.datastore

    def connect(self):
        self.conn = self.connector.raw_connect()

    def close(self):
        self.conn.close()

    def create_cursor(self):
        self.connect()
        self.cursor = self.conn.cursor()

    def query(self, query):
        if self.logging:
            self.logger.write(str(time.asctime()) + ': ' + 'executing query ' + query)
        result = self.cursor.execute(query)
        self.last_result = [row for row in self.cursor]
        return result

    def select_by_id_range(self, tablename, start_id=0, max_rows=5000, id_colname='ID', order='ASC'):
        # if order descending we want to select records less than id
        # if order ascending we want to select records greater than
        operators = {
            'ASC': '>=',
            'DESC': '<='
        }
        query_template = """
        SELECT TOP {{ max_rows }} *
        FROM {{ tablename }}
        WHERE {{ id_colname }} {{ operator }} {{ start_id }}
        ORDER BY {{ id_colname }} {{ order }}
        """
        query = query_template
        query = query.replace('{{ max_rows }}', str(max_rows))
        query = query.replace('{{ tablename }}', tablename)
        query = query.replace('{{ start_id }}', str(start_id))
        query = query.replace('{{ id_colname }}', str(id_colname))
        query = query.replace('{{ operator }}', operators[order])
        query = query.replace('{{ order }}', order)
        print(query)
        result = self.query(query)
        return result

    def export_to_csv(self, outfile):
        """
        write select query results to csv

        """
        outfile_fullpath = self.connector.config.datastore + '/' + outfile
        with open(outfile_fullpath, "w", newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([i[0] for i in self.cursor.description]) # write headers
            # print(self.cursor.fetchall())
            csv_writer.writerows(self.last_result)
        if self.logging:
            self.logger.write(str(time.asctime()) + ': ' + 'writing file ' + outfile)

    def csv_export_by_id_range(self, tablename, start_id=0, max_rows=5000, id_colname='ID', prefix='', order='ASC'):
        """
        write select query results to csv

        TODO:
        fix result dict

        """
        self.select_by_id_range(tablename=tablename, start_id=start_id, max_rows=max_rows, id_colname=id_colname, order=order)
        # get largest id value in query result
        # first get index of ID column
        id_index = [i[0] for i in self.cursor.description].index(id_colname)
        # now get max of  ID column
        min_id = min([i[id_index] for i in self.last_result])
        max_id = max([i[id_index] for i in self.last_result])
        print('min id: ' + str(min_id))
        print('max id: ' + str(max_id))
        outfile = str(prefix) + '_' + '_'.join([tablename, str(min_id), str(max_id)]) + '.csv'
        self.export_to_csv(outfile)
        return {
            'result': 'success',
            'min id': min_id,
            'max id': max_id,
            'record count': len(self.last_result)
        }

    def bulk_csv_export_by_id_range(self, tablename, start_id=0, max_files=100, max_rows_per_file=5000, id_colname='ID', prefix='', order='ASC'):
        """
        bulk write select query results to many 'max_files' csv files

        TODO:
        - fix result dict
        - read in max files and max rows form config
        - add setting to keep going until all records exported

        """
        record_count = 0
        first_id = start_id
        for n in range(max_files):
            print("export starting at " + str(start_id))
            result = self.csv_export_by_id_range(tablename=tablename, start_id=first_id, max_rows=max_rows_per_file, id_colname='SeqNo', prefix=prefix, order=order)
            if order == 'ASC':
                first_id = result['max id']
            else:
                first_id = result['min id']
            record_count += result['record count']
        return {
            'result': 'success',
            'min id': result['min id'],
            'max id': result['max id'],
            'record count': record_count
        }
        if self.logging:
            self.logger.write(str(record_count) + ' records written')

    def create_logger(self):
        """
        TODO:
        this class proabbly needs its own config importer rather than using the composite DBConnector object
        """
        self.logger = ExportLogger(logfile=self.connector.config.logfile)
        self.logging = True


class ExportLogger(object):

    def __init__(self, logfile, loggername='log', loglevel=6, maxBytes=2000, backupCount=10):
        logger = logging.getLogger(loggername)
        logger.setLevel(logging.DEBUG)
        self.logfile = logfile
        handler = RotatingFileHandler(self.logfile, maxBytes=maxBytes, backupCount=backupCount)
        logger.addHandler(handler)
        self.set_loglevel(loglevel)
        self.logger = logger
        self.enabled = True

    def set_enabled(self, enabled):
        """
        enable or disable logging
        """
        self.enabled = enabled

    def write(self, entry, level=7):
        if self.enabled:
            if level == 7:
                self.logger.debug(entry)
            else:
                self.logger.error(entry)

    def set_loglevel(self, level=3):
        """
        The list of severities is defined by RFC 5424:
        See https://en.wikipedia.org/wiki/Syslog
        ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        Value   Severity    Description
        ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        0       Emergency   System is unusable.
        1       Alert 		Action must be taken immediately.
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
