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
        return result

    def select_by_id_range(self, tablename, min_id=0, max_rows=5000, id_colname='ID'):
        query_template = """
        SELECT TOP {{ max_rows }} *
        FROM {{ tablename }}
        WHERE {{ id_colname }} >= {{ min_id }}
        ORDER BY {{ id_colname }} ASC
        """
        query = query_template
        query = query.replace('{{ max_rows }}', str(max_rows))
        query = query.replace('{{ tablename }}', tablename)
        query = query.replace('{{ min_id }}', str(min_id))
        query = query.replace('{{ id_colname }}', str(id_colname))
        result = self.query(query)
        self.last_result = [row for row in self.cursor]
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

    def csv_export_by_id_range(self, tablename, min_id=0, max_rows=5000, id_colname='ID', prefix=''):
        """
        write select query results to csv

        TODO:
        fix result dict

        """
        self.select_by_id_range(tablename=tablename, min_id=min_id, max_rows=max_rows, id_colname=id_colname)
        # get largest id value in query result
        # first get index of ID column
        id_index = [i[0] for i in self.cursor.description].index(id_colname)
        # now get max of  ID column
        max_id = max([i[id_index] for i in self.last_result])
        outfile = str(prefix) + '_' + '_'.join([tablename, str(min_id), str(max_id)]) + '.csv'
        self.export_to_csv(outfile)
        return {
            'result': 'success',
            'min id': min_id,
            'max id': max_id,
            'record count': len(self.last_result)
        }

    def bulk_csv_export_by_id_range(self, tablename, min_id=0, max_files=100, max_rows_per_file=5000, id_colname='ID', prefix=''):
        """
        bulk write select query results to many 'max_files' csv files

        TODO:
        fix result dict

        """
        record_count = 0
        start_id = min_id
        for n in range(max_files):
            print("export starting at " + str(start_id))
            result = self.csv_export_by_id_range(tablename=tablename, min_id=start_id, max_rows=max_rows_per_file, id_colname='SeqNo', prefix=prefix)
            start_id = result['max id']
            record_count += result['record count']
        return {
            'result': 'success',
            'min id': min_id,
            'max id': result['max id'],
            'record count': record_count
        }

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



if __name__ == '__main__':
    config = configparser.ConfigParser()

    # read in config from configfiles
    master_config_file = 'config.ini'
    config.read(master_config_file)
    sql_config_file = config.get('paths', 'SQL_Server_config')
    config.read(sql_config_file)

    # test DBTableExporter class
    exporter = DBTableExporter(config=config)
    exporter.create_logger()
    exporter.create_cursor()
    # result = exporter.select_by_id_range(tablename='tbAlarmsEvents', max_rows=100, id_colname='SeqNo')
    # print(exporter.cursor.fetchone())
    # with open("out.csv", "w", newline='') as csv_file:
    #     csv_writer = csv.writer(csv_file)
    #     csv_writer.writerow([i[0] for i in exporter.cursor.description]) # write headers
    #     csv_writer.writerows(exporter.cursor)
    site_id = '__aefg3b45jfe'
    # result = exporter.csv_export_by_id_range(tablename='tbAlarmsEvents', max_rows=100, id_colname='SeqNo', prefix=site_id)
    # start = time.time()
    # result = exporter.bulk_csv_export_by_id_range(tablename='tbAlarmsEvents', min_id=0, max_files=2, max_rows_per_file=5000, id_colname='SeqNo', prefix=site_id)
    # end = time.time()
    # print(result)
    # print("large file time " + str(end - start) + " sec")
    start = time.time()
    result = exporter.bulk_csv_export_by_id_range(tablename='tbAlarmsEvents', min_id=0, max_files=10, max_rows_per_file=1000, id_colname='SeqNo', prefix=site_id)
    end = time.time()
    print(result)
    print("small file time " + str(end - start) + " sec")

    exporter.close()
