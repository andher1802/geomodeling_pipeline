import psycopg2
import sys, os
import numpy as np
import pandas as pd
import pandas.io.sql as psql

from psycopg2 import sql

class DB_query(object):
    conn = None
    cursor = None
    db_query_params = None
    def __init__(self, conn, db_query_params):
        self.conn = conn
        self.cursor = self.conn.cursor()
        self.db_query_params = db_query_params
    
    def excecute_query(self, sql_command):
        try:
            self.cursor.execute(sql_command)
            result = self.cursor.fetchall()
            self.conn.commit()
            return (result)
        except Exception as e: print(e)
                
    def get_data(self, columns, condition = None):
        if condition is None:
            sql_command = sql.SQL('SELECT {} FROM {}.{};'.format(columns,\
                                           self.db_query_params['schema'],\
                                           self.db_query_params['table']))
        else:
            sql_command = sql.SQL('SELECT {} FROM {}.{} WHERE {} = {};'.format(\
                                  columns, self.db_query_params['schema'],\
                                  self.db_query_params['table'], condition['variable'], condition['value']))
        return(self.excecute_query(sql_command))

    def insert_data(self, insert_data):
        for datum in insert_data:
            sql_command = 'INSERT INTO {}.{}({}) '.format(self.db_query_params['schema'],\
                                                          self.db_query_params['table'], 'stn, alt, name, lon, lat')
            sql_command += 'VALUES (' + ','.join(datum) + ')'
            self.excecute_query(sql_command)            

class DB_manager(object):
    conn = None
    cursor = None
    db_params = {}
    
    def __init__(self, db_params):
        self.db_params = db_params
        self.conn, self.cursor = self._set_up_db_cursor()
    
    def _set_up_db_cursor(self):
        conn_string = "host=" + self.db_params['PGHOST'] +\
        " port=" + self.db_params['PORT'] +\
        " dbname=" + self.db_params['PGDATABASE'] +\
        " user=" + self.db_params['PGUSER'] +\
        " password="+ self.db_params['PGPASSWORD'] +\
        " connect_timeout=" + str(self.db_params['TIMEOUT'])
        try:
            conn=psycopg2.connect(conn_string)
            conn.autocommit = True
            cursor = conn.cursor()
            print("Connection with the db done")
            return (conn, cursor)
        except Exception as e: print(e)
    
    def get_cursor(self):
        return self.cursor
    
    def get_conn(self):
        return self.conn
    
    def create_data_base(self, db_name):
        sql_command = sql.SQL('CREATE DATABASE {};'.format(db_name))
        try:
            self.cursor.execute(sql_command)
            self.db_params['PGDATABASE'] = db_name
            print('Database created successfully')
        except Exception as e: print(e)
        
    def create_data_base_table(self, table_name, table_fields = []):
        sql_command = 'CREATE TABLE {}'.format(table_name)
        if len(table_fields) > 0:
            sql_command += ' ( '
            list_sql_command = []
            for field in table_fields:
                list_sql_command.append(f'{field["name_field"]} {field["type_field"]}')
            sql_command += ','.join(list_sql_command) + ' )'
        print(sql_command)
        try:
            self.cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
            self.cursor.execute(sql_command)
            print("Database created successfully........")
        except Exception as e: print(e)
                                            
    def close_connection(self):
        conn.close()
