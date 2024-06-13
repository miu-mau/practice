from datetime import datetime
import sqlite3
import re

conn = sqlite3.connect('apache_logs.db')
cursor = conn.cursor()

def create_table():
    try:
        cursor.execute('''CREATE TABLE IF NOT EXISTS apache_logs (
                            id INTEGER PRIMARY KEY,
                            ip TEXT,
                            date DATE,
                            method TEXT,
                            url TEXT,
                            status_code INTEGER,
                            response_size INTEGER,
                            UNIQUE(ip, date, method, url, status_code, response_size) ON CONFLICT IGNORE
                          )''')
    except sqlite3.Error as e:
        print(f"Ошибка при создании базы данных: {e}")

def insert_log_data(log_file):
    try:
        with open(log_file, 'r') as file:
            logs = file.readlines()

        for log in logs:
            patt_re = r'^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(\d{1,2}/[A-Za-z]{3}/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4})\] "(GET|POST|PUT|DELETE) (\S+) HTTP/\d\.\d" (\d{3}) (\d+)$'
            match = re.match(patt_re, log)
            if match:
                ip = match.group(1)
                date_s = match.group(2)

                date_format = '%d/%b/%Y:%H:%M:%S %z'
                date_obj = datetime.strptime(date_s, date_format)
                date = date_obj.strftime('%Y-%m-%d')
                
                method = match.group(3)
                url = match.group(4)
                status_code = int(match.group(5))
                response_size = int(match.group(6))
                
                log_data = (ip, date, method, url, status_code, response_size)
                cursor.execute('''INSERT OR IGNORE INTO apache_logs (ip, date, method, url, status_code, response_size) 
                                      VALUES (?,?,?,?,?,?)''', log_data)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Ошибка при внесении данных: {e}")
    except FileNotFoundError:
        print(f"Файл не был найден: {log_file}")
    except Exception as e:
        print(f"Ошибка: {e}")

def view(date=None, ip=None, method=None, status_code=None):
    try:
        query = "SELECT * FROM apache_logs"
        param = []
        if date:
            if len(date) == 1:
                query += " WHERE date = ?"
                param.append(date[0])
            else:
                query += " WHERE date BETWEEN ? AND ?"
                param.extend(date)
        
        if ip:
            if 'WHERE' in query:
                query += " AND ip = ?"
            else:
                query += " WHERE ip = ?"
            param.append(ip)
        
        if method:
            if 'WHERE' in query:
                query += " AND method = ?"
            else:
                query += " WHERE method = ?"
            param.append(method)
        
        if status_code:
            if 'WHERE' in query:
                query += " AND status_code = ?"
            else:
                query += " WHERE status_code = ?"
            param.append(status_code)
        
        cursor.execute(query, param)
        results = cursor.fetchall()
        
        for row in results:
            print(row)
    except sqlite3.Error as e:
        print(f"Ошибка при чтении базы данных: {e}")

create_table()

while True:
    user_write = input("Выберите команду(parse, view или ext): ")
    if user_write == "parse":
        log_file = input("Введите файл с логинами Apache: ")
        insert_log_data(log_file)
    elif user_write == "view":
        date_input = input("Введите дату или диапазон дат (YYYY-MM-DD или YYYY-MM-DD YYYY-MM-DD): ")
        dates = date_input.split()
        if len(dates) == 1:
            date = [dates[0]]
        else:
            date = dates 
        ip = input("Введите IP: ")
        method = input("Введите метод (GET, POST, PUT, DELETE): ")
        status_code = input("Введите статус кода: ")
        if ip:
            view(date, ip)
        elif method:
            view(date, ip, method)
        elif status_code:
            view(date, ip, method, status_code)
        else:
            view(date)
    elif user_write == "ext":
        break
    else:
        print("Неправильная команда, попробуйте снова!")

conn.close()