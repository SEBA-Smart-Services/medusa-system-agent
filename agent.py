import configparser
from dbexporter import *

def template_to_query(query_template, tablename, fieldname):
    query = query_template.replace('{{ tablename }}', tablename)
    query = query.replace('{{ fieldname }}', fieldname)
    return query

QUERY_TEMPLATES = {
    'select max': """
        SELECT max({{ fieldname }})
        FROM {{ tablename }}
    """
}

if __name__ == '__main__':
    config = configparser.ConfigParser()

    # read in config from configfiles
    master_config_file = 'config.ini'
    config.read(master_config_file)
    sql_config_file = config.get('paths', 'SQL_Server_config')
    config.read(sql_config_file)

    # test DBTableExporter class
    exporter = DBTableExporter(config=config)
    # create logger
    exporter.create_logger()
    # create cursor
    exporter.create_cursor()
    site_id = '__aefg3b45jfe'

    # alarms export
    id_colname =  config.get('WebReports', 'alarm_uniqueid_colname')
    tablename = config.get('WebReports', 'alarmtable')
    max_rows = config.get('WebReports', 'queryrecordlimit')
    max_files = 50
    start = time.time()
    # get max ID from table
    query = template_to_query(QUERY_TEMPLATES['select max'], tablename, id_colname)
    result = exporter.query(query)
    max_id = exporter.last_result[0][0]
    # REMOVE THIS ONE, USED IN MANUAL RUN ONLY
    max_id = 2433766
    # export csvs starting with max ID
    result = exporter.bulk_csv_export_by_id_range(tablename='tbAlarmsEvents', start_id=max_id, max_files=max_files, max_rows_per_file=max_rows, id_colname='SeqNo', prefix=site_id, order='DESC')
    end = time.time()
    print(result)
    print("small file time " + str(end - start) + " sec")

    exporter.close()
