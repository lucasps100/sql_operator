# TODO: ADD WINDOW SELECTION FUNCTIONS, MORE ALTER FUNCTIONS, SIMPLIFIED JOINING FUNCTION

import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np


class SQLOperator:
    def __init__(self, host_name: str, user_name: str, password: str):
        self.host = host_name
        self.user_name = user_name
        self.create_server_connection(host_name, user_name, password)
        self.cursor = self.connection.cursor()
        self.current_db = None
        self.recent_df = None

    def create_server_connection(self, host_name, user_name, user_password):
        self.connection = None
        try:
            self.connection = mysql.connector.connect(
                host=host_name,
                user=user_name,
                passwd=user_password
            )
            print("Connection made.")
        except Error as err:
            print(f"Error: {err}")

    def create_database(self, db: str):
        query = f"CREATE DATABASE {db};"
        try:
            self.cursor.execute(query)
            print(f"Database {db} created.")
        except Error as err:
            print(f"Error: {err}")

    def use_database(self, db: str):
        query = f"USE {db};"
        try:
            self.cursor.execute(query)
            self.current_db = db
            print(f"You are now using the database '{db}'.")
        except Error as err:
            print(f"Error: {err}")

    def table_item(self, name: str, dat: str, primary_key: bool = False, auto_increment: bool = False, foreign_key: bool = False,
                   unique: bool = False, reference_table: str = None, reference_var: str = None, not_null: bool = False, default: int = None):
        if primary_key:
            PRIM = "PRIMARY KEY"
        else:
            PRIM = ""

        if auto_increment:
            AUTO = "AUTO_INCREMENT"
        else:
            AUTO = ""

        if unique:
            UNI = "UNIQUE"
        else:
            UNI = ""

        if foreign_key:
            FORE = f"FOREIGN KEY({name}) REFERENCES {reference_table}({reference_var}), "
        else:
            FORE = ""

        if not_null:
            NOTN = "NOT NULL"
        else:
            NOTN = ""

        if default:
            DEFA = f"DEFAULT {default}"
        else:
            DEFA = ""

        CREATE_ITEM_QUERY = f"{name} {dat} {AUTO} {PRIM} {UNI} {NOTN} {DEFA}, "

        return (CREATE_ITEM_QUERY, FORE)

    def create_table(self, name: str, **args: tuple):
        CREATE_TABLE_QUERY = f"CREATE TABLE {name} ("
        for arg in args:
            CREATE_TABLE_QUERY += arg[0]
        for arg in args:
            CREATE_TABLE_QUERY += arg[1]
        CREATE_TABLE_QUERY = CREATE_TABLE_QUERY.strip(", ")
        CREATE_TABLE_QUERY += ";"
        try:
            self.cursor.execute(CREATE_TABLE_QUERY)
            print(f"Table '{name}' created successfully.")
        except Error as err:
            print(f"Error: {err}")

    def set_auto_start(self, table_name: str, start):
        ALTER_TABLE_QUERY = f"ALTER TABLE {table_name} AUTO_INCREMENT={start};"
        try:
            self.cursor.execute(ALTER_TABLE_QUERY)
            print(f"The auto incremented values of table '{table_name}' now start at {start}.")
        except Error as err:
            print(f"Error: {err}")

    def add_constraint(self, table_name: str, check: str, constraint_name=""):
        ALTER_TABLE_QUERY = f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} CHECK ({check});"
        try:
            self.cursor.execute(ALTER_TABLE_QUERY)
            print(f"Constraint added on table '{table_name}'.")
        except Error as err:
            print(f"Error: {err}")

    def insert_into_table(self, table_name: str, var_tup: tuple, **args: tuple):
        INSERT_QUERY = f"INSERT INTO {table_name} ({var_tup}) VALUES "
        for arg in args:
            INSERT_QUERY += f"({arg}), "
        INSERT_QUERY.strip(", ")
        INSERT_QUERY += ";"
        try:
            self.cursor.execute(INSERT_QUERY)
            self.cursor.commit()
            print(f"Values successfully added to table '{table_name}'.")
        except Error as err:
            print(f"Error: {err}")

    def execute_query(self, query: str):
        try:
            self.cursor.execute(query)
            print(f"Query executed.")
        except Error as err:
            print(f"Error: {err}")

    def selection_to_df(self, table_name: str, vals: str = "*", where: str = None, sort: str=None, desc: bool = False, group: str = None, having: str = None):
        fields = []
        if vals == "*":
            self.cursor.execute(f"DESCRIBE {table_name};")
            for x in self.cursor:
                fields.append(x[0])
        else:
            vals = vals.lower()
            for i in vals.split(","):
                if "as" in i:
                    i = i.split("as")[1]
                fields.append(i.strip())

        WHERE = ""
        if where:
            WHERE = f"WHERE {where}"

        SORT = ""
        if sort:
            DESC = ""
            if desc:
                DESC = "DESC"
            SORT = f"ORDER BY {sort} {DESC}"

        GROUP = ""
        if group:
            HAVING = ""
            if having:
                HAVING = f"HAVING {having}"
            GROUP = f"GROUP BY {group} {HAVING}"


        data = []

        self.execute_query(f"SELECT {vals} FROM {table_name} {WHERE} {GROUP} {SORT};")
        for x in self.cursor:
            data.append(x)
        index = fields[0]
        df = pd.DataFrame(data, columns=fields)
        if len(vals.split(",")) > 1:
            df.set_index([f"{index}"], inplace=True)
        print("Data frame created.")
        self.recent_df = df
        return df

    def enter_df(self, table_name: str, df, vars_tup : tuple =  None):
        if not vars_tup:
            vars_tup = tuple(df.columns)
        arr = np.array(df)
        for i in range(len(arr)):
            arr[i] = tuple(arr[i])
        self.insert_into_table(table_name=table_name, vars_tup=vars_tup,*arr)


