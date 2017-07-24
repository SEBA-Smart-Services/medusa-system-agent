"""
# dbexporter.py

Revision 0.1.1

Clive Gross
Schneider Electric
2017

## License

## Description

## Example

## Todo

 * Write the docstring

"""
import configparser
# import _mssql # need this for cx_Freeze!
import pymssql
import csv
import time
import os


class DBConnector(object):

    def __init__(self, config):
        self.config = DBConfigLoader(config)

    def raw_connect(self):
        """
        create a db connection using pymssql.connect()

        """
        conn = pymssql.connect(
            server = self.config.hostname,
            user = self.config.username,
            password = self.config.password,
            database = self.config.db,
            timeout = self.config.timeout,
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
        self.config_section = 'database'
        self.set_hostname()
        self.set_port()
        self.set_servername()
        self.set_db()
        self.set_driver()
        self.set_username()
        self.set_password()
        # need to fix logging
        # self.set_logfile()
        self.set_datastore()
        self.set_timeout()

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
        self.logfile = self.config.get('logging', 'file')
        
    def set_timeout(self):
        self.timeout = self.config.get(self.config_section, 'timeout')

    def set_datastore(self):
        self.datastore = self.config.get('paths', 'datastore')


class DBTableExporter(object):

    """
    exports SQL queries to CSV

    """

    def __init__(self, config, logger=None):
        self.config = config
        self.connector = DBConnector(config=self.config)
        self.set_outdir()
        # configure logging
        if logger:
            self.logging = True
            self.logger = logger
        else:
            self.logging = False

    def set_outdir(self):
        self.outdir = self.connector.config.datastore

    def connect(self):
        self.conn = self.connector.raw_connect()

    def close(self):
        self.conn.close()
        if self.logging:
            self.logger.write('closed database connection!')

    def create_cursor(self):
        if self.logging:
            self.logger.write('connecting to SQL database ' + self.connector.config.db)
        try:
            self.connect()
            self.cursor = self.conn.cursor()
            if self.logging:
                self.logger.write('connected, cursor created.')
        except:
            if self.logging:
                self.logger.write('failed to connect!')

    def query(self, query):
        """
        executes query
        
        creates connection, creates cursor, executes query, save result, closes connection
        
        TODO:
        - this is inefficient for bulk execution but safe, add an unsafe raw query with no open/close
        """
        self.create_cursor()
        if self.logging:
            self.logger.write('executing query ' + query)
        result = self.cursor.execute(query)
        self.last_result = [row for row in self.cursor]
        self.close()
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
        query = query.replace('{{ start_id }}', self.stringify(str(start_id)))
        query = query.replace('{{ id_colname }}', str(id_colname))
        query = query.replace('{{ operator }}', operators[order])
        query = query.replace('{{ order }}', order)
        result = self.query(query)
        return result
        
    def stringify(self, value):
        if value[0] == "'" and value[-1] == "'":
            return value
        else:
            return "'" + value + "'"

    def get_abspath_programdata(self, path):
        return path.lower().replace('%programdata%', os.environ['ProgramData'])    
        
    def export_to_csv(self, outfile):
        """
        write select query results to csv

        """
        outfile_fullpath = self.get_abspath_programdata(self.connector.config.datastore) + '/' + outfile
        with open(outfile_fullpath, "w", newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([i[0] for i in self.cursor.description]) # write headers
            csv_writer.writerows(self.last_result)
        if self.logging:
            self.logger.write('writing file ' + outfile)

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
        outfile = self.name_csv(tablename, prefix, min_id, max_id)
        self.export_to_csv(outfile)
        return {
            'result': 'success',
            'min id': min_id,
            'max id': max_id,
            'record count': len(self.last_result)
        }
        
    def name_csv(self, tablename, prefix, min_id, max_id):
        """
        create name for csv file
        clean up yucky characters
        """
        filename = str(prefix) + '_' + '_'.join([tablename, str(min_id), str(max_id)])
        filename = filename.replace(' ', '_').replace(':', '').replace('-', '').replace('.', '')
        filename = filename + '.csv'
        return filename

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
            result = self.csv_export_by_id_range(tablename=tablename, start_id=first_id, max_rows=max_rows_per_file, id_colname=id_colname, prefix=prefix, order=order)
            if order == 'ASC':
                first_id = result['max id']
            else:
                first_id = result['min id']
            record_count += result['record count']
            if result['max id'] <= result['min id']:
                print("no more records to pull")
                break
        return {
            'result': 'success',
            'min id': result['min id'],
            'max id': result['max id'],
            'record count': record_count
        }
        if self.logging:
            self.logger.write(str(record_count) + ' records written')
