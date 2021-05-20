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
    cursor_mysql.execute("""SELECT referencial, data, troco, ref_fun,
                                  fechado, hora_fechamento, nome_caixa,
                                  saldo_final FROM core_ven_caixa""")
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
    cursor_fire.execute("""SELECT referencial, data, troco, ref_fun,
                                  fechado, hora_fechamento, nome_caixa,
                                  saldo_final FROM VEN_CAIXA""")
    t_c = cursor_fire.fetchall()

    list_rfire = []  # lista do banco local (referencial)
    for rfire, dt, tc, rff, fc, hrf, nc, sf in t_c:
        list_rfire.append(rfire)

    list_rfms = []  # lista do banco site (referencial)
    for rfms, dts, tcs, rffs, fcs, hrfs, ncs, sfs in t_cs:
        list_rfms.append(rfms)

    dif_list = []  # compara e insere na lista (referencial)
    for element in list_rfire:
        if element not in list_rfms:
            dif_list.append(element)

    print(f'Lista de referenciais diferentes dos bancos '
          f'(ven_caixa - falta inserir no site): {dif_list}\n')
    # list_rfos[-2] -1 nula...
    print(f'Referenciais caixa site: {list_rfms}\n')

    # Comparar e Fazer UPDATE
    for rfire, dt, tc, rff, fc, hrf, nc, sf in t_c:
        for rfms, dts, tcs, rffs, fcs, hrfs, ncs, sfs in t_cs:
            if rfire == rfms:
                if dt != dts:
                    value_column = 'data'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_caixa
                                    SET {value_column}=('{dt}')
                                    WHERE {referencial}=({rfire})"""
                    print('Atualizando: ', value_column)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if tc != tcs:
                    value_column = 'troco'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_caixa
                                    SET {value_column}=({tc})
                                    WHERE {referencial}=({rfire})"""
                    print('Atualizando: ', value_column)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rff != rffs:
                    value_column = 'ref_fun'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_caixa
                                    SET {value_column}=({rff})
                                    WHERE {referencial}=({rfire})"""
                    print('Atualizando: ', value_column)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if fc != fcs:
                    value_column = 'fechado'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_caixa
                                    SET {value_column}=('{fc}')
                                    WHERE {referencial}=({rfire})"""
                    print('Atualizando: ', value_column)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if hrf != hrfs:
                    value_column = 'hora_fechamento'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_caixa
                                    SET {value_column}=('{hrf}')
                                    WHERE {referencial}=({rfire})"""
                    print('Atualizando: ', value_column)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if nc != ncs:
                    value_column = 'nome_caixa'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_caixa
                                    SET {value_column}=('{nc}')
                                    WHERE {referencial}=({rfire})"""
                    print('Atualizando: ', value_column)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if sf != sfs:
                    value_column = 'saldo_final'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_caixa
                                    SET {value_column}=('{sf}')
                                    WHERE {referencial}=({rfire})"""
                    print('Atualizando: ', value_column)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()

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
