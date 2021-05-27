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
    cursor_mysql.execute("""SELECT referencial, cpf_cnpj, data_uso
                            FROM core_cliente""")
    t_us = cursor_mysql.fetchall()

    # FIREBIRD
    con_fire = firebirdsql.connect(
        host=config("host_f"),
        database=config("database_f"),
        port=config("port_f"),
        user=config("user_f"),
        password=config("password_f"),
        charset=config("charset_f")
    )
    print("Database Local Firebird connection made!")
    cursor_fire = con_fire.cursor()
    # local
    cursor_fire.execute("""SELECT referencial, data_uso
                            FROM CON_CONFIG""")
    t_ul = cursor_fire.fetchall()

    # pegar dados do arquivo
    arq = open("cod_cliente.ini", "r")
    lst_cod = []
    # Efetua a leitura das linhas do arquivo
    for line in arq.readlines():
        cpf_cnpj_empresa = str(line)
        # Quebra a linha lida em uma lista, a cada espaço encontrado
        # é gerado um item na lista
        # line.strip().split(' ')

        # retira espaços em branco do inicio e fim
        cpf_cnpj_empresa = re.sub(r"^\s+|\s+$", "", cpf_cnpj_empresa)
        lst_cod.append(cpf_cnpj_empresa)
    cpf_cnpj_empresa = str(lst_cod[0])

    # Comparar e Fazer UPDATE - LOCAL data_uso
    for rfire, dtu in t_ul:
        for rfms, cpf_cnpj, dtus in t_us:
            # Cliente site - DevSys cpf_cnpj
            if cpf_cnpj == cpf_cnpj_empresa:
                # print("Site: ", cpf_cnpj)
                # print("Cod: ", cpf_cnpj_empresa)
                if dtus != dtu:
                    print(rfms, " Uso Site: ", dtus)
                    print(rfire, " Uso Local: ", dtu)
                    value_column = 'data_uso'
                    comando_sql = f"""UPDATE CON_CONFIG
                                SET {value_column}=('{dtus}')
                                WHERE {rfire}=({1})"""
                    print('Atualizando: ', value_column)
                    cursor_fire.execute(comando_sql)
                    con_fire.commit()

    con_mysql.close()
    con_fire.close()
    # fecha terminal?
    os._exit(1)

except ValueError:
    print('Error database')
else:
    con_mysql.close()
    con_fire.close()
    os._exit(1)
