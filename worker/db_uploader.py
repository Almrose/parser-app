from worker.modules.flat_json import flatten
from dotenv import load_dotenv
import psycopg2
from psycopg2.errors import UndefinedTable, DuplicateTable
import os
import datetime
import json
import traceback


class DBUploader():
    #   Local DB variables
    load_dotenv(os.getcwd()+"/.envs/.postgres")
    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    DB_NAME = os.getenv("POSTGRES_DB")
    DB_PORT = os.getenv("POSTGRES_PORT")
    DB_HOST = os.getenv("POSTGRES_HOST")
    db_schema = os.getenv("DB_SCHEMA")
    cursor = None
    conn = None

    def __init__(self, data_table, special_types=dict(), special_parameters=dict(), data_table_update_condition_column=None, data_table_default_insert=dict(), data_table_default_update=dict()) -> None:
        #   SPECIAL VARIABLES FOR SITE
        #   Special types anaalyzed by json
        self.ignored_columns = []
        #   Data table Columns
        self.data_table_columns = dict()
        #   Description table columns names
        self.description_columns_names = dict()
        #   JSON fields names
        self.json_columns = dict()
        self.special_types = special_types
        self.special_parameters = special_parameters
        self.data_table_update_condition_column = data_table_update_condition_column
        self.data_table_default_insert = data_table_default_insert
        self.data_table_default_update = data_table_default_update
        self.data_table_name = data_table
        self.description_table_name = self.data_table_name + "_description"
        self.db_connect()
        self.get_column_names_from_description_table()
        self.get_column_names_from_data_table()
        self.updated_items = 0

    #   Upload cards from response

    def upload_from_response(self, response):
        self.updated_items = 0
        if self.check_db(response):
            self.upload_to_db(response)
            return self.updated_items
        else:
            return "Error while upload"

    #   Upload cards from file

    def upload_from_file(self, json_file):
        self.updated_itemss = 0
        with open(json_file, 'r') as f:
            data = json.load(f)
        if self.check_db(data):
            self.upload_to_db(data)
            return self.updated_items
        else:
            return "Error while upload"

    def upload_to_db(self, data):
        for item in data["responses"]:
            json = flatten(item)
            json_dict_key, json_list_key, default_insert_key, default_update_key, json_dict_value, json_list_value, default_insert_value, default_update_value = self.json_clean(
                json)
            query = f'INSERT INTO {self.data_table_name}\
                    (index, {json_dict_key} {json_list_key} {default_insert_key}) \
                    VALUES ((SELECT COALESCE(MAX(index), 0) + 1 FROM {self.data_table_name}), {json_dict_value} {json_list_value} {default_insert_value}) \
                    ON CONFLICT (id) DO UPDATE SET ({json_dict_key}{json_list_key}{default_update_key}) = \
                    ({json_dict_value} {json_list_value} {default_update_value})'
            if self.data_table_update_condition_column != None:
                condition = f" WHERE {self.data_table_name}.{self.data_table_update_condition_column} <> '{json[self.data_table_update_condition_column]}'"
                query += condition
            try:
                with self.conn:
                    with self.conn.cursor() as cursor:
                        cursor.execute(query)
                        self.conn.commit()
                        self.updated_items += 1
            except Exception as e:
                print(e)

    def json_clean(self, json):
        for key, value in json.items():
            if type(value) is str:
                json[key] = value.replace("'", '"')
            if type(value) is list:
                json[key] = ['' if i is None else i for i in value]
                for element in value:
                    if type(element) is str:
                        element = element.replace("'", '"')
        # with open("exit.json", "a") as file:
        #     json.dump(json, file, ensure_ascii=False)
        json_dict_items = {
            key: value for key, value in json.items() if value is not None and key not in self.ignored_columns and type(value) is not list}
        json_list_items = {
            key: value for key, value in json.items() if value is not None and key not in self.ignored_columns and type(value) is list}
        #   This is a one problem in query. If string contains single quote - it brokes whole query.
        #   This is a problems with repr function that returns string in single quotes 'string'.
        #   So that's why single quotes shoud be replaced to double quotes.
        #   join keys and values to string
        json_dict_keys = ", ".join(map(str, json_dict_items.keys()))
        json_dict_values = ", ".join(repr(x)
                                     for x in json_dict_items.values())
        default_insert_keys = ", ".join(
            map(str, self.data_table_default_insert.keys()))
        default_insert_values = ", ".join(
            map(str, self.data_table_default_insert.values()))
        default_update_keys = ", ".join(
            map(str, self.data_table_default_update.keys()))
        default_update_values = ", ".join(
            map(str, self.data_table_default_update.values()))
        json_list_keys = ", ".join(map(str, json_list_items.keys()))
        json_list_values = ", ARRAY ".join(
            map(str, json_list_items.values()))
        if json_dict_keys != "":
            json_dict_key = f"{json_dict_keys}"
            json_dict_value = f"{json_dict_values}"
        else:
            json_dict_key = ""
            json_dict_value = ""
        if json_list_keys != "":
            json_list_key = f", {json_list_keys}"
            json_list_value = f", ARRAY {json_list_values}"
        else:
            json_list_key = ""
            json_list_value = ""
        if default_insert_keys != "":
            default_insert_key = f", {default_insert_keys}"
            default_insert_value = f", {default_insert_values}"
        else:
            default_insert_key = ""
            default_insert_value = ""
        if default_update_keys != "":
            default_update_key = f", {default_update_keys}"
            default_update_value = f", {default_update_values}"
        else:
            default_update_key = ""
            default_update_value = ""
        return json_dict_key, json_list_key, default_insert_key, default_update_key, json_dict_value, json_list_value, default_insert_value, default_update_value
        # -------------- Check DB and file, creating tables and columns if needed --------------

    def check_db(self, json):
        try:
            self.get_columns_from_json(json)
            if not self.compare_json_and_description_columns():
                self.fill_description_table()
            if not self.compare_data_and_description_columns():
                self.fill_data_table_columns(self.data_table_name)
            return True
        except Exception as e:
            traceback.print_exc()
            # print(e)
            return False

    #   Get columns from json File

    def get_columns_from_json(self, data):
        for item in data["responses"]:
            flat_json = flatten(item)
            for key, value in flat_json.items():
                self.json_columns[key] = self.convert_type(type(value))

    #   ----------------  Mothods for working with data table   -----------------------------------------

    #   Get columns from data table

    def get_column_names_from_data_table(self):
        try:
            query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{self.db_schema}' AND table_name = '{self.data_table_name}';"
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.execute(query)
                    self.conn.commit()
                    data = cursor.fetchall()
                    if data == []:
                        raise UndefinedTable
                    self.data_table_columns = dict(
                        zip(list(val[0] for val in data), list(val[1] for val in data)))
                    for key, value in self.data_table_columns.items():
                        if value == "ARRAY":
                            self.data_table_columns[key] = "text ARRAY"
        except UndefinedTable:
            self.create_data_table(self.data_table_name)

    #   Create table with data if not exist

    def create_data_table(self, data_table_name):
        index = "index bigint PRIMARY KEY UNIQUE NOT NULL"
        query = f"CREATE TABLE IF NOT EXISTS {data_table_name}({index});"
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                self.conn.commit()
        query = f"CREATE INDEX {data_table_name}_index ON {data_table_name}(index)"
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                self.conn.commit()

    def fill_data_table_columns(self, data_table_name):
        self.get_column_names_from_description_table()
        self.get_parameters_from_description_table()
        set_of_data_table_columns = set(
            self.data_table_columns.items())
        difference = dict(x for x in list(
            self.description_columns_names.items()) if x not in set_of_data_table_columns)
        for key, value in difference.items():
            if key in self.special_parameters:
                parameters = self.special_parameters[key]
                self.create_data_column(
                    data_table_name, key, value, parameters)
            else:
                self.create_data_column(data_table_name, key, value)
        self.get_column_names_from_data_table()

    #     #   Create column from table names
    def create_data_column(self, data_table_name, col_name, col_type, parameter=""):
        query = f"ALTER TABLE {data_table_name} ADD COLUMN IF NOT EXISTS {col_name} {col_type} {parameter};"
        self.cursor.execute(query)
        self.conn.commit()

#   ----------------------- Methods for working with description table  --------------------------------

    #   Get column names fom description table
        #   Get Columns from DB
    def get_column_names_from_description_table(self):
        try:
            query = f"SELECT column_name, column_type FROM {self.description_table_name}"
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.execute(query)
                    data = cursor.fetchall()
                    self.description_columns_names = dict(
                        zip(list(val[0] for val in data), list(val[1] for val in data)))
            #   Get Ignored Columns
            query = f"SELECT column_name FROM {self.description_table_name} where ignore_column = True"
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.execute(query)
                    data = cursor.fetchall()
                    self.ignored_columns = [val[0] for val in data]
        except UndefinedTable:
            self.create_description_table(self.description_table_name)
        return self.description_columns_names

    #   Get parameters from description table
    def get_parameters_from_description_table(self):
        try:
            query = f"SELECT column_name, column_parameters FROM {self.description_table_name} where column_parameters != ''"
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.execute(query)
                    data = cursor.fetchall()
                    self.special_parameters = dict(
                        zip(list(val[0] for val in data), list(val[1] for val in data)))
        except:
            pass

    #   Create table with tabble description if not exist
    def create_description_table(self, table_with_column_description):
        query = f"CREATE TABLE IF NOT EXISTS {table_with_column_description} (column_name text PRIMARY KEY UNIQUE, column_type text, column_parameters text, column_changable BOOLEAN DEFAULT FALSE, ignore_column BOOLEAN DEFAULT FALSE)"
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                self.conn.commit()
        for key in self.special_types.keys():
            if key in self.special_parameters:
                self.create_description_row(
                    key, self.special_types[key], self.special_parameters[key])
            else:
                self.create_description_row(key, self.special_types[key])

    #   Fill description table if field doesn't exists in DB

    def fill_description_table(self):
        set_of_description_columns_names = set(
            self.description_columns_names.keys())
        difference = [x for x in list(
            self.json_columns.keys()) if x not in set_of_description_columns_names]
        for column in difference:
            parameters = ""
            if column in self.special_types:
                self.json_columns[column] = self.special_types[column]
            if self.json_columns[column] == "boolean":
                parameters = "DEFAULT false"
            self.create_description_row(
                column, self.json_columns[column], parameters)
        self.get_column_names_from_description_table()

    #   Add row to description table
    def create_description_row(self, column_name, column_type, column_parameters=""):
        with self.conn:
            with self.conn.cursor() as cursor:
                query = f"INSERT INTO {self.description_table_name} (column_name, column_type, column_parameters) VALUES ('{column_name}', '{column_type}', '{column_parameters}') ON CONFLICT DO NOTHING"
                cursor.execute(query)
                self.conn.commit()

    # +  Data type converter while creating description rows
    def convert_type(self, col_type):
        if col_type is type(None):
            return "text"
        if col_type is int:
            return "bigint"
        if col_type is float:
            return "numeric"
        if col_type is str:
            return "text"
        if col_type is list:
            return "text ARRAY"
        if col_type is bool:
            return "boolean"
        if col_type is dict:
            pass

#   -------------------- Additional methods for working with db --------------------------------------

    # +   Connection to DB
    def db_connect(self):
        conn = psycopg2.connect(
            host=self.DB_HOST,
            port=self.DB_PORT,
            dbname=self.DB_NAME,
            user=self.DB_USER,
            password=self.DB_PASSWORD,
            target_session_attrs="read-write",
        )
        self.cursor = conn.cursor()
        self.conn = conn

    #   Compare columns in description and json
    def compare_json_and_description_columns(self):
        difference = set(self.json_columns.keys()) - \
            set(self.description_columns_names.keys())
        if len(difference) != 0:
            return False

    #   Compare columns in data table and description table
    def compare_data_and_description_columns(self):
        set_of_data_table_columns = set(
            self.data_table_columns.items())
        difference = dict(x for x in list(
            self.description_columns_names.items()) if x not in set_of_data_table_columns)
        # for key, value in list(difference.items()):
        #     if value.endswith("ARRAY"):
        #         del difference[key]
        if len(difference) != 0:
            return False

    def select_query(self, query):
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                self.conn.commit()
                data = cursor.fetchall()
                return data

    def insert_or_update_query(self, query):
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                self.conn.commit()

    #   Additional methods

    # def get_unparsed_ids(self, id_field, cards_table_name, condition=None, order_by=None, limit=None):
    #     query = f"select {id_field} from {cards_table_name}"
    #     if order_by != None:
    #         query += f" ORDER BY {order_by} desc "
    #     if condition != None:
    #         query += f" where {condition} = FALSE "
    #     if limit is not None:
    #         query += f" limit {limit} "
    #     with self.conn:
    #         with self.conn.cursor() as cursor:
    #             cursor.execute(query)
    #             self.conn.commit()

    #   Convert column to array type
    # def convert_column_to_text_array(self, table_name, column_name, delimiter="~@#$"):
    #     query = f"alter table {table_name} alter {column_name} type text[] using string_to_array({column_name}, '{delimiter}')"
    #     with self.conn:
    #         with self.conn.cursor() as cursor:
    #             cursor.execute(query)
    #             self.conn.commit()


class RemoteUploader():

    load_dotenv(os.getcwd()+"/.envs/.postgres")
    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    DB_NAME = os.getenv("POSTGRES_DB")
    DB_PORT = os.getenv("POSTGRES_PORT")
    DB_HOST = os.getenv("POSTGRES_HOST")
    db_schema = os.getenv("DB_SCHEMA")
    local_cursor = None
    local_conn = None
    tempt_table_columns = None

    def __init__(self, cards_table_name, data_table_name, temp_table_name, resume_table_ingored_columns_on_upload_to_main_db, cards_table_ignored_columns_on_upload_to_main_db):
        self.cards_table_name = cards_table_name
        self.data_table_name = data_table_name
        self.temp_table_name = temp_table_name
        self.resume_table_ingored_columns_on_upload_to_main_db = resume_table_ingored_columns_on_upload_to_main_db
        self.cards_table_ignored_columns_on_upload_to_main_db = cards_table_ignored_columns_on_upload_to_main_db
        self.db_connect(self.DB_HOST, self.DB_PORT,
                        self.DB_NAME, self.DB_USER, self.DB_PASSWORD)

    #   Creating Local Temp Table

    def join_tables_to_temp(self, join_query):
        columns = self.get_columns_list_for_temp_table()
        self.create_local_temp(join_query, columns)
        self.get_temp_table_columns()

    #   Get columns from data and cards tables

    def get_columns_list_for_temp_table(self):
        try:
            cards_columns_query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{self.db_schema}' AND table_name = '{self.cards_table_name}';"
            cards_columns_response = self.select_query(
                cards_columns_query, self.local_conn)
            cards_columns = list(val[0] for val in cards_columns_response)
            data_columns_query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{self.db_schema}' AND table_name = '{self.data_table_name}';"
            data_columns_response = self.select_query(
                data_columns_query, self.local_conn)
            resume_columns = list(val[0] for val in data_columns_response)
            resume_columns = [
                x for x in resume_columns if x not in self.resume_table_ingored_columns_on_upload_to_main_db]
            cards_columns = [f"{self.cards_table_name}." +
                             x for x in cards_columns if x not in self.cards_table_ignored_columns_on_upload_to_main_db and x not in resume_columns]
            resume_columns = [f"{self.data_table_name}." +
                              x for x in resume_columns]
            total_columns = [i for sublist in [
                resume_columns, cards_columns] for i in sublist]
            columns = ", ".join(x for x in total_columns)
            return columns
        except Exception as e:
            print("Can't merge columns")

    #   Create local temp table

    def create_local_temp(self, join_query, columns):
        try:
            join_query = join_query.format(temp_table=self.temp_table_name, columns=columns,
                                           cards_table=self.cards_table_name, data_table=self.data_table_name)
            self.query_to_db(join_query, self.local_conn)
        except DuplicateTable:
            query = f"drop table {self.temp_table_name}"
            self.query_to_db(query, self.local_conn)
            join_query = join_query.format(temp_table=self.temp_table_name, columns=columns,
                                           cards_table=self.cards_table_name, data_table=self.data_table_name)
            self.query_to_db(join_query, self.local_conn)

    #   Get dict of columns names and data types

    def get_temp_table_columns(self):
        query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{self.db_schema}' AND table_name = '{self.temp_table_name}';"
        data = self.select_query(query, self.local_conn)
        self.temp_table_columns = dict(
            zip(list(val[0] for val in data), list(val[1] for val in data)))
        for key, value in self.temp_table_columns.items():
            if value == "ARRAY":
                self.temp_table_columns[key] = "text ARRAY"

    #
    #   Fill Remote Table
    #

    # def fill_remote_table_dblink(self, temp_table):
    #     self.check_remote_table(temp_table)
    #     cursor, conn = self.db_connect(REMOTE_POSTGRES_HOST, REMOTE_POSTGRES_PORT,
    #                                    REMOTE_POSTGRES_DB, REMOTE_POSTGRES_USER, REMOTE_POSTGRES_PASSWORD, ssl=True)
    #     temp_column_names = ", ".join(key for key in temp_table.keys())
    #     query = f"insert into avito_temp ({temp_column_names}) select {temp_column_names} from \
    #         dblink('hostaddr=158.160.14.213 port=6432 dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}', \
    #         'select {temp_column_names} from avito_temp') as \
    #         linktable({', '.join(' '.join((key, val)) for (key, val) in temp_table.items())})"
    #     # print(query)
    #     cursor.execute(query)
    #     conn.commit()

    #   Check remote table columns

    # def check_remote_table(self, temp_table):
    #     try:
    #         remote_cursor, remote_conn = self.db_connect(REMOTE_POSTGRES_HOST, REMOTE_POSTGRES_PORT,
    #                                                      REMOTE_POSTGRES_DB, REMOTE_POSTGRES_USER, REMOTE_POSTGRES_PASSWORD, ssl=True)
    #         remote_cursor.execute("create table if not exists avito_temp ()")
    #         remote_conn.commit()
    #         query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{db_schema}' AND table_name = 'avito_temp';"
    #         remote_cursor.execute(query)
    #         remote_conn.commit()
    #         remote_column_names = list(val[0] for val in cursor.fetchall())
    #         columns_to_add = {key: value for key, value in temp_table.items(
    #         ) if key not in remote_column_names}
    #         for key, value in columns_to_add.items():
    #             query = f"ALTER TABLE avito_temp ADD COLUMN IF NOT EXISTS {key} {value}"
    #             remote_cursor.execute(query)
    #             remote_conn.commit()
    #         return True
    #     except Exception as e:
    #         print(e)
    #         print("No Connection or error while columns insert")
    #         return False

    #   Additional Methods

    #   Select query with return

    def select_query(self, query, conn):
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()
                data = cursor.fetchall()
                return data

    def query_to_db(self, query, conn):
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()

    #   Connect to database
    def db_connect(self, host, port, dbname, user, password, ssl=False):
        if ssl == True:
            sslmode = "verify-full"
        else:
            sslmode = "allow"
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            target_session_attrs="read-write",
            sslmode=sslmode
        )
        self.local_conn = conn
        self.local_cursor = conn.cursor()
