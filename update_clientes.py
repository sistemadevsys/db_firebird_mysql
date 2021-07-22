# https://pypi.org/project/firebirdsql/
# pip install firebirdsql

import firebirdsql
from decouple import config
# pip install mysql-connector-python
import mysql.connector
import re
import os


try:
    # Mysql Local
    # con_mysql = mysql.connector.connect(
    #     host=config("host"),
    #     user=config("user"),
    #     password=config("password"),
    #     database=config("database"))
    # MYSQL Site
    con_mysql = mysql.connector.connect(
        host=config("host_"),
        user=config("user_"),
        password=config("password_"),
        database=config("database_"))

    print("Database connection Mysql made!")

    cursor_mysql = con_mysql.cursor()
    # site
    cursor_mysql.execute("""SELECT cpf_cnpj, data_uso
                            FROM core_cliente""")
    t_cli = cursor_mysql.fetchall()
    dt_new = input('Por favor digite a data de vencimento(aaaa-mm-dd): ') # aaaa-mm-dd
    for cpf_cnpj, dtus in t_cli:
        print(cpf_cnpj, "Data Uso: ", dtus)
        dtus = '2021-09-10'  # aaaa-mm-dd
        value_column = 'data_uso'
        value_where = 'cpf_cnpj'
        comando_sql = f"""UPDATE core_cliente
                    SET {value_column}=('{dt_new}')
                    WHERE {value_where}=('{cpf_cnpj}')"""
        print('Atualizando: ', value_column)
        cursor_mysql.execute(comando_sql)
        con_mysql.commit()

    con_mysql.close()
    # fecha terminal?
    os._exit(1)

except ValueError:
    print('Error database')
else:
    con_mysql.close()
    con_fire.close()
    os._exit(1)
