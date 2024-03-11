from worker.token_checker import Token
from worker.db_uploader import DBUploader, RemoteUploader
import os
import requests
import json
import datetime
import time
from worker.modules.flat_json import flatten


class ResumeParser():
    #   SPECIAL VARIABLES FOR CARDS UPLOAD
    #   Cards table
    cards_table = "avito_resume_cards"
    #   Special types for columns, analyzed by json (Used for table creating)
    cards_special_types = {"id": "bigint",
                           "parsed": "boolean",
                           "added_to_parse": "timestamp without time zone",
                           "parsed_at": "timestamp without time zone"}
    #   Special parameters for columns analyzed by json (Used for table crating)
    cards_special_prameters = {"id": "UNIQUE",
                               "parsed": "default false"}
    #   Condition column to update data DB where different value in response and in json
    #   For exemple if "updated_at" in json have a different value - update column
    cards_data_table_update_contidion_column = "updated_at"
    #   If data row updates - set default values in to columns from dict.
    cards_data_table_default_insert = {"parsed": "FALSE",
                                       "added_to_parse": "CURRENT_TIMESTAMP"}
    cards_data_table_default_update = {"parsed": "FALSE",
                                       "added_to_parse": "CURRENT_TIMESTAMP",
                                       "parsed_at": "NULL"}

    #   SPECIAL VARIABLES FOR RESUME UPLOAD
    resume_table = "avito_resume"
    resume_special_types = {"id": "bigint",
                            "source_file": "text",
                            "source_file_first": "text"}
    resume_special_parameters = {"id": "UNIQUE"}

    tempt_table = "avito_temp"

    def __init__(self):
        self.token = Token()
        self.cards_uploader = DBUploader(self.cards_table, self.cards_special_types, self.cards_special_prameters, self.cards_data_table_update_contidion_column,
                                         data_table_default_insert=self.cards_data_table_default_insert, data_table_default_update=self.cards_data_table_default_update)
        self.resume_uploader = DBUploader(self.resume_table, self.resume_special_types,
                                          self.resume_special_parameters)

    #   Support method to wrtie json with admin righs
    def opener(self, path, flags):
        return os.open(path, flags, 0o777)

    #   Get lase resume cards
    def get_last_resumes_cards(self):
        headers = {'Authorization': 'Bearer ' + self.token.token}
        params = {'per_page': "100"}
        url = 'https://api.avito.ru/job/v1/resumes/'
        try:
            response = requests.get(
                url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                response_json = response.json()
                # with open(os.getcwd() + '/raw/'+str(datetime.date.today()) + "/list_of_resumes.json", "w") as file:
                #     json.dump(response_json, file, ensure_ascii=False)
                self.write_file(os.getcwd() + '/raw/'+str(datetime.date.today()) +
                                '/resume_list_H' + str(datetime.datetime.now().hour)+'.json', response_json['resumes'])
                #   Upload to DB
                data = {"responses": response_json['resumes']}
                parsed = self.cards_uploader.upload_from_response(data)
                # self.get_unparsed_resumes()
                return "Parsed " + str(parsed)
        #         with open("smth2.json", "w") as file:
        #             json.dump(response_json, file,
        #                       indent=4, ensure_ascii=False)
        except Exception as e:
            print(e)
            return "No response"

    def get_unparsed_resumes(self):
        headers = {'Authorization': 'Bearer ' + self.token.token}
        single_vacancy_url = 'https://api.avito.ru/job/v2/resumes/'
        query = f"select id from {self.cards_table} where parsed = FALSE AND parsed_at IS NULL order by added_to_parse desc limit 100"
        table_data = self.resume_uploader.select_query(query)
        ids = [val[0] for val in table_data]
        parsed_ids = []
        filename = str(datetime.date.today()) + '/resume_H' + \
            str(datetime.datetime.now().hour)+'.json'
        tempfile = str(datetime.date.today()) + '/resume' + \
            str(datetime.datetime.now())+'.json'
        if len(ids) > 0:
            for id in ids:
                try:
                    response = requests.get(single_vacancy_url+str(id),
                                            headers=headers, timeout=10)
                    if response.status_code == 200:
                        response_json = response.json()
                        #   Save to file
                        self.write_file(os.getcwd() + '/raw/' +
                                        filename, response_json)
                        self.write_file(os.getcwd() + '/raw/' +
                                        tempfile, response_json)
                        parsed_ids.append(response_json["id"])
                    else:
                        query = f"update {self.cards_table} set parsed = FALSE, parsed_at = CURRENT_TIMESTAMP where id = {id}"
                        self.resume_uploader.insert_or_update_query(query)
                except Exception as e:
                    print("No Response from id " + str(id))
            self.resume_uploader.data_table_default_insert = {
                "source_file": repr(filename),
                "source_file_first": repr(filename)}
            self.resume_uploader.data_table_default_update = {
                "source_file": repr(filename)}
            self.resume_uploader.upload_from_file(
                os.getcwd() + '/raw/' + tempfile)
            #   Remove temp file after upload
            os.remove(os.getcwd() + '/raw/' + tempfile)
            query = f"update {self.cards_table} set parsed = True, parsed_at = CURRENT_TIMESTAMP where id in ({', '.join(str(id) for id in parsed_ids)})"
            self.resume_uploader.insert_or_update_query(query)
            return ("Parsed " + str(len(parsed_ids)) + " Full Resumes")
        else:
            return "No resume for parse"

    def write_file(self, file, response):
        if not os.path.exists(os.getcwd() + '/raw/'+str(datetime.date.today())):
            os.mkdir(os.getcwd() + '/raw/' +
                     str(datetime.date.today()), mode=0o777)
        try:
            with open(file, 'r+', opener=self.opener, encoding='utf8') as f:
                data = json.load(f)
                if type(response) is list:
                    for item in response:
                        data['responses'].append(item)
                else:
                    data['responses'].append(response)
                f.seek(0)
                json.dump(data, f, indent=4, ensure_ascii=False)
        except:
            with open(file, "w", opener=self.opener, encoding='utf8') as f:
                if type(response) is list:
                    data = {"responses": response}
                else:
                    data = {"responses": [response]}
                json.dump(data, f, indent=4, ensure_ascii=False)

    def upload_to_main_db(self):
        resume_table_ingored_columns_on_upload_to_main_db = [
            "index", "start_time", "update_time", "params_address", "params_business_area", "params_nationality"]
        cards_table_ignored_columns_on_upload_to_main_db = [
            "parsed", "added_to_parse", "parsed_at"]

        uploader = RemoteUploader(self.cards_table, self.resume_table, self.tempt_table,
                                  resume_table_ingored_columns_on_upload_to_main_db, cards_table_ignored_columns_on_upload_to_main_db)
        uploader.join_tables_to_temp()
        columns = uploader.temp_table_columns
        join_query = "create table {temp_table} as select {columns}, {cards_table}.created_at::timestamp AS first_pub_date from {data_table} join {cards_table} Using(id)"
        # query = f"select {self.resume_table}.created_at, {self.resume_table}.updated_at, {self.cards_table}.start_time, {self.cards_table}.update_time from avito_resume JOIN {self.cards_table} USING(id)"
        # data = self.resume_uploader.select_query(query)
        # with open(os.getcwd() + "/raw/"+"data.txt", "w") as file:
        #     file.write(data)
        #   Join tables
        #   Add first_pub_date
        #   Connect to main table
        #   Insert or update data from current table
        #   If everything fine - remove uploaded data from local table

        pass
