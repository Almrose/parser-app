import os
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(os.getcwd() + "/.envs/.env")

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(
                Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Token(metaclass=Singleton):
    token_path = os.getcwd() + "/config/token"
    token = ''
    update_time = datetime.now()

    def __init__(self) -> None:
        self.get_token()
        pass

    #   Get Token
    def get_token(self):
        self.load_token()
        if self.token == '':
            updated = self.update_token()
        else:
            updated = self.check_token_expires()
        return updated

    #   Check expired token
    def check_token_expires(self):
        if (datetime.now() - self.update_time).total_seconds() < 84600:
            print("No need to update token")
            return "No need to update"
        else:
            updated = self.update_token()
            return updated

    #   Load Token from avito
    def update_token(self):
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        params = {'grant_type': 'client_credentials',
                  'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET}
        url = 'https://api.avito.ru/token/'
        try:
            response = requests.post(
                url, headers=headers, params=params).json()
            self.token = response['access_token']
            self.update_time = datetime.strftime(
                datetime.now(), "%Y-%m-%d %H:%M:%S")
            self.save_token()
        except:
            print("Error while token update")
            return "Update Token Error"
        return "Token Updated"

    #   Save Token to txt file

    def save_token(self):
        with open(self.token_path, 'w+') as file:
            file.write(str(self.update_time) + "\n" + str(self.token))
            print("Token Updated")
        return

    #   Load Token from txt file
    def load_token(self):
        if os.path.exists(self.token_path):
            with open(self.token_path, 'r+') as file:
                data = file.read().splitlines()
                if data != []:
                    time = data[0]
                    self.token = data[1]
                    self.update_time = datetime.strptime(
                        time, "%Y-%m-%d %H:%M:%S")
        return
