import config
import os
import re
import psycopg2
from datetime import datetime, date
import datetime


class Log:
    def __init__(self):
        self.ip = 'Нет данных'
        self.date = date.today()
        self.method = 'Нет данных'
        self.url = 'Нет данных'
        self.status = 'Нет данных'
        self.user_agent = 'Нет данных'

    def __repr__(self):
        return f'ip: {self.ip}, date: {self.date}, method: {self.method}, url: {self.url}, status: {self.status}, user_agent: {self.user_agent} '

    def to_tuple(self):
        return (self.ip, self.date, self.method, self.url, self.status, self.user_agent)


data_patterns = {
    '%h': r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
    '%t': r'(\[\d{2}\/[A-Za-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4}\])',
    '%r': r'"([^ ]*) ([^ ]*) HTTP/1\.[01]"',
    '%>s': r'(\b\d{3}\b)',
    '%b': r'(\b\d+\b)'

}


class LogManager:
    def __init__(self, file, data_patterns):
        self.file = file
        self.data_patterns = data_patterns

    def read_logs(self):
        logs = []
        for file_path, file_pattern in self.file:
            if not os.path.exists(file_path):
                print(f'File not found: {file_path}')
                continue

            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    log = self.create_log(line, file_pattern)
                    if log:
                        logs.append(log)
        return logs

    def read_and_send_logs(self, db_manager):
        logs = self.read_logs()
        logs_tuples = [log.to_tuple() for log in logs]
        db_manager.send_data(logs_tuples)

    def fetch_logs(self, db_manager, answer):
        query, columns = self.create_query(answer)
        result = db_manager.fetch_data(query)
        logs = []
        for row in result:
            log = {}
            for i, column in enumerate(columns):
                log[column] = row[i]
            logs.append(log)
        return logs

    def create_query(self, answer):
        # Разбор ввода пользователя и создание SQL-запроса
        parts = answer.split()
        if parts[0].lower() != 'select':
            raise ValueError("Invalid command format. Expected 'select'.")

        # Получаем список столбцов для выборки
        columns = []
        i = 1
        while i < len(parts) and parts[i] != 'from':
            columns.append(parts[i])
            i += 1

        # Проверяем, что столбцы были указаны
        if not columns:
            raise ValueError("No columns specified for selection.")

        # Получаем таблицу и условия фильтрации
        if i < len(parts) - 1:  # Убеждаемся, что после 'from' есть еще элементы
            table = parts[i + 1]
            conditions = []
            i += 2  # Пропускаем 'from' и таблицу
            while i < len(parts) and parts[i] != ';':
                conditions.append(parts[i])
                i += 1
        else:
            raise ValueError("Invalid command format. Expected 'from' and table name.")

        # Формируем SQL-запрос
        query = f"SELECT {', '.join(columns)} FROM {table}"
        if conditions:
            query += f" WHERE {' AND '.join(conditions)}"
        query += ";"

        return query, columns

    def create_log(self, line, file_pattern):
        log = Log()
        for pattern in file_pattern:
            if pattern in self.data_patterns:  # Проверяем, существует ли ключ в словаре data_patterns
                match = re.search(self.data_patterns[pattern], line)
                if not match:
                    continue
                if pattern == '%h':
                    log.ip = match.group()
                elif pattern == '%t':
                    # Извлекаем строку времени без квадратных скобок
                    date_str = match.group(1).strip('[]')
                    date_object = datetime.datetime.strptime(date_str, '%d/%b/%Y:%H:%M:%S %z')
                    log.date = date_object.strftime('%Y-%m-%d')
                elif pattern == '%r':
                    match = re.search(self.data_patterns['%r'], line)
                    if match:
                        request = match.group(1)
                        # Теперь вызовите re.search() с request
                        match = re.search(self.data_patterns['%r'], line)
                        if match:
                            request = match.group(1)
                            # Теперь вызовите re.search() с request
                            match = re.search(r'(.*?) (.*?) (.*?) HTTP/1\.[01]" (\d{3}) (.*)', request)
                            if match:
                                log.method, log.url, _, log.status, log.user_agent = match.groups()
                elif pattern == '%>s':
                    log.status = match.group()
                elif pattern == '%b':
                    log.user_agent = match.group(1)
        return log if any(getattr(log, attr) != 'Нет данных' for attr in log.__dict__) else None


class DatabaseManager:
    def __init__(self, db_info):
        self.connection = None
        self.db_info = db_info

    def connect(self):
        self.connection = init_connection(self.db_info)

    def fetch_data(self, query):
        if not self.connection:
            self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def send_data(self, data):
        if not self.connection:
            self.connect()
        pull_data(self.connection, data)


def init_connection(db_info):
    try:
        return psycopg2.connect(**db_info)
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        return None


def pull_data(connection, logs):
    if not connection:
        return
    cursor = connection.cursor()
    for log_tuple in logs:
        if len(log_tuple) != 6:
            print(f"Invalid tuple length: {log_tuple}. Skipping...")
            continue
        if not all(isinstance(val, (str, int, datetime.date)) for val in log_tuple):
            print(f"Invalid data types in tuple: {log_tuple}. Skipping...")
            continue
        cursor.execute(
            "INSERT INTO logs (ip, timestamp, method, url, status, user_agent) VALUES (%s, %s, %s, %s, %s, %s)",
            log_tuple)
    connection.commit()


def main():
    db_manager = DatabaseManager(config.db_info)
    log_manager = LogManager(config.file, data_patterns)
    while True:
        answer = input('# ')
        if 'load_logs' in answer:
            log_manager.read_and_send_logs(db_manager)
        elif 'select' in answer:
            try:
                logs = log_manager.fetch_logs(db_manager, answer)
                for log in logs:
                    print(log)
            except ValueError as e:
                print(e)
        else:
            print('Unknown command')

if __name__ == '__main__':
    main()
