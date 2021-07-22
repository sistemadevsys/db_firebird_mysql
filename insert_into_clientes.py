# https://pypi.org/project/firebirdsql/
# pip install firebirdsql
import firebirdsql
from decouple import config
# pip install mysql-connector-python
import mysql.connector
import re
from datetime import datetime

try:
    # Mysql Local
    # con_mysql = mysql.connector.connect(
    #     host=config("host"), user=config("user"),
    #     password=config("password"),
    #     database=config("database"))
    # MYSQL
    con_mysql = mysql.connector.connect(
        host=config("host_"), user=config("user_"),
        password=config("password_"),
        database=config("database_"))

    print("Database connection Mysql made!")

    cursor_mysql = con_mysql.cursor()

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

    # fin_clientes local
    cursor_fire = con_fire.cursor()
    cursor_fire.execute("""SELECT NOME, RAZAO_SOCIAL, CPF_CNPJ
                            FROM FIN_CLIENTES""")
    t_fclientes = cursor_fire.fetchall()

    # cliente site
    cursor_mysql.execute("""SELECT nome, razao_social,
                            cpf_cnpj, data_uso 
                            FROM core_cliente""")
    t_cliente = cursor_mysql.fetchall()
 #######################################################
    list_rfire = []  # lista do banco local POR NOME
    for nome, razao, cpf_cnpj in t_fclientes:
        list_rfire.append(nome)

    list_rfms = []  # lista do banco site POR NOME
    for name, rs, cpfcnpj, dtuso in t_cliente:
        list_rfms.append(name)

    dif_list = []  # compara e insere na lista
    for element in list_rfire:
        if element not in list_rfms:
            dif_list.append(element)

    print(f'Lista clientes diferentes dos bancos '
          f'(falta inserir no site): {dif_list}\n')
    # list_rfos[-2] -1 nula...
    print(f'Nome clientes no site: {list_rfms}\n')

    # for name, rs, cpfcnpj, dtuso in t_cliente:
    i = 0
    for nome, razao, cpf_cnpj in t_fclientes:
        if nome == dif_list[i]:
            if razao is None:
                razao = 'Sem Razão Social'
            if cpf_cnpj is None:
                new_cpf_cnpj = '00.000.000.0000/00'
            if cpf_cnpj is not None:
                new_cpf_cnpj = ''.join(e for e in cpf_cnpj if e.isalnum())
                print(new_cpf_cnpj)

            sql_i = """INSERT INTO core_cliente(
                nome, razao_social, cpf_cnpj, data_uso, date_added
                )
                VALUES(%s, %s, %s, %s, %s)"""
            dt = '2021-08-10'
            date_added = datetime.now()
            val = (
                nome, razao, new_cpf_cnpj, dt, date_added
                )
            cursor_mysql.executemany(sql_i, (val,))
            con_mysql.commit()
            print('Inserido cliente: ', dif_list[i])
            i += 1

    con_mysql.close()
    con_fire.close()
except ValueError:
    print('Error database')
else:
    con_mysql.close()
    con_fire.close()
