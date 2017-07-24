"""
# statedb.py

Revision 0.1.1

Clive Gross
Schneider Electric
2017

## License

## Description

## Example

## Todo
    * Write the docstring
    * update_all_last_ids(self, exporter, noneonly=False, forceones=False):
        forceones: set ID to 1 DOESNT WORK YET!!!
    * update_all_last_ids_if_none(self, exporter):
        DELETE THIS METHOD!
        USE update_all_last_ids(..., noneonly=True)

"""
import sqlite3

# stores data related to the current state of exports
STATE_DB = "state.db"

CREATE_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS export (
        id INTEGER PRIMARY KEY ASC,
        table_name text UNIQUE,
        table_primary_key text,
        last_export_record_id blob,
        table_record_dt_field datetime
    );
"""
    
INSERT_RECORDS_QUERY = """
    INSERT INTO export(table_name,table_primary_key,last_export_record_id,table_record_dt_field)
    VALUES (?,?,?,?);
"""

#######
# THIS IS BAD
# DUPLICATE FROM medusa-agent.py, REFACTOR!
#
def template_to_query(query_template, tablename, fieldname, count=1, order='ASC', value='', idfieldname='', idvalue=''):
    query = query_template.replace('{{ tablename }}', tablename)
    query = query.replace('{{ fieldname }}', fieldname)
    query = query.replace('{{ count }}', str(count))
    query = query.replace('{{ order }}', order)
    query = query.replace('{{ value }}', str(value))
    query = query.replace('{{ idfieldname }}', idfieldname)
    query = query.replace('{{ idvalue }}', str(idvalue))
    return query

QUERY_TEMPLATES = {
    'select max': """
        SELECT max({{ fieldname }})
        FROM {{ tablename }};
    """,
    'select top': """
        SELECT top {{ count }}
        {{ fieldname }}
        FROM {{ tablename }}
        ORDER BY {{ fieldname }} {{ order }};
    """,
    'update field': """
        UPDATE {{ tablename }}
        SET {{ fieldname }} = {{ value }}
        WHERE {{ idfieldname }} = {{ idvalue }};
    """
}

class StateDb(object):

    def __init__(self, data_dir, config):
        self.state_db = data_dir + STATE_DB
        self.config = config
        self.initialise_state_db()
        
    def get_table_data_from_config(self):
        """
        TODO:
        Need to import the tables from the config file
        
        
        these are the tables to export
        # the following data for each table is required:
        # 1. table_name
        # 2. table_primary_key: the primary key of the record
        # 3. last_export_record_id: used internally to know which incrementing primary key record to start exporting from
        # 4. table_record_dt_field: the datetime field of the record
        self.TABLE_RECORDS = [
        ('tbAlarmsEvents', 'SeqNo', None, 'DateTimeStamp'),
        ('tbLoggedEntities', 'ID', None, 'LastMod'),
        ('tbLogTimeValues', 'DateTimeStamp', None, 'DateTimeStamp')
        ]
        """
        # table data comes from config file
        table_data = self.config.items('tables')
        TABLE_RECORDS = []
        for table in table_data:
            # config per table is a string containing a comma-sepearted list, convert to python list
            table_config = table[1].split(",")
            # insert the 3rd item as None for the starting field
            table_config.insert(2, None)
            # convert to tuple for easy bulk pymssql execution
            TABLE_RECORDS.append(tuple(table_config))
        # write as attributes of StateDb object
        self.TABLE_RECORDS = TABLE_RECORDS

    def create_connection(self):
        """ create a database connection to the SQLite database
            specified by db_file
        :param db_file: database file
        :return: Connection object or None
        """
        try:
            conn = sqlite3.connect(self.state_db)
            return conn
        except Error as e:
            print(e)
     
        return None
        
    def create_table(self, c, create_table_sql):
        """ create a table from the create_table_sql statement
        :param c: Connection cursor
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            c.execute(create_table_sql)
        except Error as e:
            print(e)
                    
    def initialise_state_db(self):
        """
        initialise state database
         1. open connection to db (create db if not exists)
         2. create export table containing table names to export
         3. insert records into table
         4. commit and close connection
        :param c: Connection cursor
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        state_conn = self.create_connection()
        c = state_conn.cursor()
        self.create_table(c, CREATE_TABLE_QUERY)
        self.get_table_data_from_config()
        try:
            c.executemany(INSERT_RECORDS_QUERY, self.TABLE_RECORDS)
            state_conn.commit()
        except:
            print('data already exists ')
        state_conn.close()
        
    def test_state_db(self):
        state_conn = self.create_connection()    
        c = state_conn.cursor()
        c.execute('SELECT * FROM export')
        print(c.fetchall())
        state_conn.close()
        
    def get_table_by_name(self, table_name):
        state_conn = self.create_connection()    
        c = state_conn.cursor()
        c.execute('SELECT table_name, table_primary_key, last_export_record_id FROM export WHERE table_name = %s;' % (table_name,))
        result = c.fetchone()
        state_conn.close()
        return result
    
    def update_last_id(self, table, exporter, noneonly=False, forceones=False):
        """
        update id of last exported record
        needs exporter object from dbexporter
        for querying webreports tables
        """
        state_conn = self.create_connection()
        c = state_conn.cursor()
        table_name = table[0]
        id_colname = table[1]
        if forceones:
            if noneonly:
                if table[2] is None:
                    max_id = 1
                else:
                    max_id = table[2]
            else:
                max_id = 1
        else:
            # query webreports for latest ID
            try:
                query = template_to_query(
                    QUERY_TEMPLATES['select top'],
                    table_name,
                    id_colname,
                    count=1,
                    order='desc'
                )
                result = exporter.query(query)
                max_id = exporter.last_result[0][0]
            except:
                print('Fail')
                    
        try:
            query = template_to_query(
                QUERY_TEMPLATES['update field'],
                'export',
                'last_export_record_id',
                value="'"+str(max_id)+"'",
                idfieldname='table_name',
                idvalue="'"+str(table_name)+"'"
            )
            c.execute(query)
            state_conn.commit()
        except:
            print("Failed to update last ID")
        # ALWAYS CLOSE CONNECTION
        state_conn.close()
                
    def update_all_last_ids(self, exporter, noneonly=False, forceones=False):
        """
        update id of last exported record
        for all tables
        only if they are currently set to None
        needs exporter object from dbexporter
        for querying webreports tables
        params:
            noneonly: if noneonly, only update if current entry is None
            forceones: set ID to 1 DOESNT WORK YET!!!
        """
        state_conn = self.create_connection()
        c = state_conn.cursor()
        c.execute("""
            SELECT table_name, table_primary_key, last_export_record_id
            FROM export;
        """)
        result = c.fetchall()
        state_conn.close()
        for table in result:
            if noneonly and table[2] is None:
                self.update_last_id(table, exporter, forceones=forceones)
            elif not noneonly:
                self.update_last_id(table, exporter, forceones=forceones)
            else:
                print('table already has record')
        # ALWAYS CLOSE CONNECTION
        state_conn.close()
        
    def get_tables(self):
        state_conn = self.create_connection()
        c = state_conn.cursor()
        c.execute("""
            SELECT table_name, table_primary_key, last_export_record_id
            FROM export;
        """)
        result = c.fetchall()
        # ALWAYS CLOSE CONNECTION
        state_conn.close()
        return result