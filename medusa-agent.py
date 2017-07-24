"""
# medusa-agent.py

Revision 0.1.1

Clive Gross
Schneider Electric
2017

## License


## Description
This program exports data from a site SQL database, such as StruxureWareReportsDB
and dumps into a series of csv files. The most recent data successfully exported 
is saved in a SQLite database so that the next run only dumps newer records. The
export directory can also be purged of files once the data is redundant.

Designed to work in conjuntion with a cloud backup system, such as Chimera. This
application exports the data to a local directory, the backup system then syncs to
a remote store, then this application purges the local data.

## Example
        ```
		# run export db tables as csvs
        $ python medusa-agent.py export
        
		# purge exported csvs
		$ python medusa-agent.py purge
		```


## Configuration file
Read in using configparser. Contains settings for the database connection
as well as the tables that require exporting.

    ```
    [paths]
    datastore: %programdata%\\medusa-agent\\sitedata
    statedb: %programdata%\\medusa-agent\\state.db
    log: %programdata%\\medusa-agent\\log

    [settings]
    site_id: 69

    [database]
    hostname: localhost
    port: 1433
    servername: SQLEXPRESS
    db: StruxureWareReportsDB
    driver: SQL Server Native Client 11.0
    username: medusa
    password: password
    queryrecordlimit: 1000
    timeout: 120

    [tables]
    alarms: tbAlarmsEvents,SeqNo,DateTimeStamp
    logged_variables: tbLoggedEntities,ID,LastMod
    log_samples: tbLogTimeValues,DateTimeStamp,DateTimeStamp
    ```

## Todo

 * Write the class

"""
import sys
import sqlite3
import configparser
from statedb import StateDb
from dbexporter import DBTableExporter
import os
from logger import Logger

# NEED TO SET THESE EXPLICITLY SO FROZEN EXE CAN FIND CONFIG
DATA_DIR = "%programdata%\\medusa-agent\\"
CONFIG_FILE = "config.ini"
# DATA_DIR = "C:\\Schneider Electric\\TESTING\\"
# CONFIG_FILE = "config.ini"

def get_args():
	if len(sys.argv) == 1:
		print('Not enough arguments')
	elif len(sys.argv) == 2:
		if sys.argv[1] == 'export':
			# run export db tables as csvs
			print('exporting data to csv')
			return 'export'
		elif sys.argv[1] == 'purge':
			# purge exported csvs
			print('purging files')
			return 'purge'
		elif sys.argv[1] == 'config':
			# copy config file into programdata
			print('adding config file')
			return 'config'
		else:
			print('Unknown argument')
			return None
	else:
		print('Too many arguments')
		return None

def purge_dir(path):
	"""
	remove all files in a dir
	params:
		path: path to the directory to empty
	TODO: needs to be extended to include subdirs, not just files
	"""
	for file in os.listdir(path):
		file_path = os.path.join(path, file)
		try:
			if os.path.isfile(file_path):
				print('removing', file_path)
				os.remove(file_path)
		except Exception as e:
			print(e)

def make_dir(path):
	abspath = get_abspath_programdata(path)
	if not os.path.exists(abspath):
			os.makedirs(abspath)
			
def get_abspath_programdata(path):
	return path.lower().replace('%programdata%', os.environ['ProgramData'])

if __name__ == '__main__':

    ################################################################
    # decide what to do here based on program command line arguments
    command = get_args()

    ################################################################
    # need to make ProgramData directory if doesn't exist
    make_dir(DATA_DIR)

    if command is not None:
        
        ################################################################
        # read in config from configfiles
        config = configparser.ConfigParser()
        data_dir = get_abspath_programdata(DATA_DIR)
        make_dir(data_dir)
        master_config_file = data_dir + CONFIG_FILE
        config.read(master_config_file)

        ################################################################
        # need to make DATASTORE directory if doesn't exist
        datastore_dir = get_abspath_programdata(config.get('paths', 'datastore'))
        make_dir(datastore_dir)
        
        # need to make LOG directory if doesn't exist
        log_dir = get_abspath_programdata(config.get('paths', 'log'))
        make_dir(log_dir)
        
        # create logger
        logger = Logger(log_dir + '\\' + 'log')

        if command == 'export':
            ################################################################
            # initialise state db
            statedb = StateDb(data_dir, config)

            ################################################################
            # initialise WebReports exporter
            # test DBTableExporter class
            exporter = DBTableExporter(config=config, logger=logger)

            ################################################################
            # need to replace latest ID with 1 for tbLoggedEntities because we want the whole table at initialization
            loggedentities_table = statedb.get_table_by_name("'tbLoggedEntities'")
            statedb.update_last_id(loggedentities_table, exporter, noneonly=True, forceones=True)
            # update latest record IDs from WebReports if state DB has None
            statedb.update_all_last_ids(exporter, noneonly=True)
            #####
            # we dont always want only records from the max ID, on init, we want all of tbLoggedEntities
            #####

            ################################################################
            # loop through all tables and export latest records to csv
            # alarms export
            # id_colname =  config.get('WebReports', 'alarm_uniqueid_colname')
            # tablename = config.get('WebReports', 'alarmtable')
            max_rows = config.get('database', 'queryrecordlimit')
            site_id = config.get('settings', 'site_id')
            max_files = 100
            # get last ID from state db
            tables = statedb.get_tables()
            for table in tables:
                # export csvs starting with max ID
                ####
                # need to insert quotation marks around id in case of datetime ID
                # This is a little yuck and needs some work
                ####
                result = exporter.bulk_csv_export_by_id_range(
                    tablename=table[0],
                    start_id="'"+str(table[2])+"'",
                    max_files=max_files,
                    max_rows_per_file=max_rows,
                    id_colname=table[1],
                    prefix=site_id,
                    order='ASC'
                )
                # update IDs to latest
                ## THIS IS BROKEN AND WILL MISS OCCASIONAL RECORDS, NEED TO FIX
                statedb.update_all_last_ids(exporter)
                # ALWAYS CLOSE CONNECTION
                exporter.close()


        elif command == 'purge':
            ################################################################
            # purge files in datastore
            logger.write('Started purging ' + datastore_dir + '.')
            purge_dir(datastore_dir)
            logger.write('Finished purging ' + datastore_dir + '.')

        elif command == 'config':
            ################################################################
            # copy config file to DATA_DIR
            print('Not supported yet')
