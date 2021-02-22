# https://pypi.org/project/firebirdsql/
# pip install firebirdsql

import firebirdsql
from decouple import config
# pip install mysql-connector-python
import mysql.connector


# try:
# Mysql Local
def mysql_con():
    con_mysql = mysql.connector.connect(
        host=config("host"), user=config("user"),
        password=config("password"),
        database=config("database"))
    # MYSQL
    # con_mysql = mysql.connector.connect(
    #     host=config("host_"), user=config("user_"),
    #     password=config("password_"),
    #     database=config("database_"))

    print("Database connection Mysql made!")

    return con_mysql


def mysql_cursor():
    con = mysql_con()
    cursor_mysql = con.cursor()
    return cursor_mysql


# FIREBIRD
def fire_bird_con():
    con_fire = firebirdsql.connect(
        host=config("host_f"),
        database=config("database_f"),
        port=config("port_f"),
        user=config("user_f"),
        password=config("password_f"),
        charset=config("charset_f")
    )
    print("Database Local Firebird connection made!")

    return con_fire


def fire_cursor():
    con = fire_bird_con()
    cursor_fire = con.cursor()
    return cursor_fire


def t_ser_servicos():
    c = fire_cursor()
    c.execute("""SELECT REFERENCIAL, REF_FUN, DESCRICAO,
                        RESPONSAVEL, DT_AGENDA, DT_PAGAMENTO, VALOR,
                        EQUIPAMENTO FROM SER_SERVICOS""")
    t_ser = c.fetchall()
    return t_ser


# Ordem Servicos
def t_ordem_servico():
    c = mysql_cursor()
    c.execute("""SELECT referencial, ref_fun2
                FROM core_ordem_servico
                """)
    t_os = c.fetchall()
    return t_os


def mysql_close():
    con = mysql_con()
    return con.close()


def fire_bird_close():
    con = fire_bird_con()
    return con.close()


t_os = t_ordem_servico()
c_mysql = mysql_cursor()
con = mysql_con()
for ref, rfun in t_os:
    # delete (não usar porque é backup)
    if ref:
        sql_Delete_query = """DELETE from core_ordem_servico
                                WHERE referencial = %s"""
        c_mysql.execute(sql_Delete_query, (ref,))
        con.commit()


# except ValueError:
#     print('Error database')
# else:
#     mysql_close()

#     fire_bird_close()
