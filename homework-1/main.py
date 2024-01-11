"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv

import psycopg2

FILE_CUSTOMERS_DATA = 'north_data/customers_data.csv'
FILE_EMPLOYEES_DATA = 'north_data/employees_data.csv'
FILE_ORDERS_DATA = 'north_data/orders_data.csv'


def get_db_connection():
    return psycopg2.connect(host='localhost', database='north', user='postgres', password='23061980')


def insert_data_to_sql(filename, tablename):
    try:
        with open(filename, 'r', encoding='UTF-8') as file:
            data = csv.DictReader(file)
            with get_db_connection() as conn:
                with conn.cursor() as curs:
                    for row in data:
                        columns = ', '.join(row.keys())
                        values = ', '.join(['%s'] * len(row))
                        query = f"INSERT INTO {tablename} ({columns}) VALUES ({values})"
                        curs.execute(query, tuple(row.values()))
    except Exception as error:
        print(f" Произошла ошибка: {error}")
    finally:
        conn.close()


# Пример использования:
insert_data_to_sql(FILE_CUSTOMERS_DATA, 'customers')
insert_data_to_sql(FILE_EMPLOYEES_DATA, 'employees')
insert_data_to_sql(FILE_ORDERS_DATA, 'orders')
