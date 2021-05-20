# https://pypi.org/project/firebirdsql/
# pip install firebirdsql

import firebirdsql
from decouple import config
# pip install mysql-connector-python
import mysql.connector
# import re
import os


try:
    # Mysql Local
    con_mysql = mysql.connector.connect(
        host=config("host"),
        user=config("user"),
        password=config("password"),
        database=config("database"))
    # MYSQL Site
    # con_mysql = mysql.connector.connect(
    #     host=config("host_"),
    #     user=config("user_"),
    #     password=config("password_"),
    #     database=config("database_"))

    print("Database connection Mysql made!")

    cursor_mysql = con_mysql.cursor()
    # Ven_Caixa site
    cursor_mysql.execute("""SELECT referencial, nome, tipo, prazo,
                                  parcelas, taxa, ref_conta,
                                  ref_subconta, contas_receber,
                                  vl_minimo, codigo_fiscal, controle_cheque,
                                  ref_banco, ref_sic, emissor, valor, maquina,
                                  parcelado
                                   FROM core_ven_formas""")
    t_cs = cursor_mysql.fetchall()

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
    # VEN_CAIXA local
    cursor_fire.execute("""SELECT referencial, nome, tipo, prazo,
                                  parcelas, taxa, ref_conta,
                                  ref_subconta, contas_receber,
                                  vl_minimo, codigo_fiscal, controle_cheque,
                                  ref_banco, ref_sic, emissor, valor, maquina,
                                  parcelado FROM VEN_FORMAS""")
    t_c = cursor_fire.fetchall()

    list_rfire = []  # lista do banco local (referencial)
    for a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r in t_c:
        list_rfire.append(a)

    list_rfms = []  # lista do banco site (referencial)
    for a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r in t_cs:
        list_rfms.append(a)

    dif_list = []  # compara e insere na lista (referencial)
    for element in list_rfire:
        if element not in list_rfms:
            dif_list.append(element)

    print(f'Lista de referenciais diferentes dos bancos '
          f'(ven_formas - falta inserir no site): {dif_list}\n')
    # list_rfos[-2] -1 nula...
    print(f'Referenciais site: {list_rfms}\n')

    count = 0
    for a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r in t_c:

        if len(dif_list) != 0:
            if a == dif_list[count]:
                # if dtf is None:
                #     dtf = " "
                # dtf = re.sub(r"^\s+|\s+$", "", dtf)

                sql_i = """INSERT INTO core_ven_formas(
                            referencial, nome, tipo, prazo,
                            parcelas, taxa, ref_conta,
                            ref_subconta, contas_receber,
                            vl_minimo, codigo_fiscal, controle_cheque,
                            ref_banco, ref_sic, emissor, valor, maquina,
                            parcelado)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                val = (
                    a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r
                    )
                cursor_mysql.executemany(sql_i, (val,))
                con_mysql.commit()
                print('Inserido referencial: ', dif_list[count])
                count += 1

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
