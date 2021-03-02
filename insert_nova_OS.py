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
    # Servicos local
    cursor_fire = con_fire.cursor()
    cursor_fire.execute("""SELECT REFERENCIAL, REF_FUN, DESCRICAO,
                           RESPONSAVEL, DT_AGENDA, DT_PAGAMENTO, VALOR,
                           EQUIPAMENTO FROM SER_SERVICOS""")
    t_ser = cursor_fire.fetchall()

    # Ordem Servicos
    cursor_mysql.execute("""SELECT referencial, ref_fun2, descricao,
                    dt_agenda, valor, usuario_os_id
                    FROM core_ordem_servico
                    """)
    t_os = cursor_mysql.fetchall()

    sql_fun = ("""SELECT referencial, nome, usuario_fun_id
                    FROM core_funcionario""")
    cursor_mysql.execute(sql_fun)
    # Precisa ter pelo menos dois campos para passar para string
    t_mysql_fun = cursor_mysql.fetchall()

    for rfu, nome, u_fun_id in t_mysql_fun:
        print(f'{u_fun_id} - {nome}')

    list_rfire = []
    for rfire, rfun, desc, respo, dt_a, dt_p,  vl, eq in t_ser:
        if rfun == 1 or rfun == 2 or rfun == 5 or rfun == 10 or rfun == 22:
            list_rfire.append(rfire)

    list_rfos = []
    for rfos, rfun_os, descri, dt, vl_os, user_id in t_os:
        # print(rfun_os, user_id)
        list_rfos.append(rfos)

    dif_list = []
    for element in list_rfire:
        if element not in list_rfos:
            dif_list.append(element)

    print(f'Lista de referenciais diferentes dos bancos'
          f'(falta inserir no site): {dif_list}\n')
    # list_rfos[-2] -1 nula...
    print(f'Referenciais serviços (OS) site: {list_rfos}\n')

    # s = ['a', 'b', 'c']
    # f = ['a', 'b', 'c', 'd']
    # ss = set(s)
    # fs = set(f)
    # print(s)
    # print(f)
    # print('Interseção: ', ss.intersection(fs))
    # print('União: ', ss.union(fs))
    # print('Diferença (União-Interseção): ', ss.union(fs)-ss.intersection(fs))

    # comparar e fazer insert dos referenciais diferentes...
    dt_ = datetime.now().strftime('%Y-%m-%d %H:%M')
    for rfire, rfun, desc, respo, dt_a, dt_p, vl, eq in t_ser:
        if rfun == 1:
            user_id = 2  # maurilio
        if rfun == 2:
            user_id = 3  # fernando
        if rfun == 5:
            user_id = 4  # flavia
        if rfun == 10:
            user_id = 5  # rogerio
        if rfun == 22:
            user_id = 7  # julia
        if len(dif_list) != 0:
            if rfire == dif_list[-1]:  # depois da primeiro
                if desc is None:
                    desc = "Sem descrição."
                desc = re.sub(r"^\s+|\s+$", "", desc)
                if eq is None:
                    eq = "Sem equipamento."
                eq = re.sub(r"^\s+|\s+$", "", eq)
                if respo is None:
                    respo = "Sem responsavel."
                respo = re.sub(r"^\s+|\s+$", "", respo)
                if dt_a is None or dt_a == "":
                    dt_a = '1111-11-11 11:11:11 1.1'
                if dt_p is None or dt_p == "":
                    dt_p = '1111-11-11 11:11:11'

                sql_i = """INSERT INTO core_ordem_servico(
                    referencial, ref_fun2, descricao, responsavel,
                    dt_agenda, dt_pagamento, dt_entrada,
                    dt_atualizada, valor, equipamento,
                    confirmar, finalizar, usuario_os_id)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s)"""
                val = (
                    rfire, rfun, desc, respo, dt_a, dt_p, dt_, dt_,
                    vl, eq, 0, 0, user_id
                    )
                cursor_mysql.executemany(sql_i, (val,))
                con_mysql.commit()

    con_mysql.close()
    con_fire.close()

except ValueError:
    print('Error database')
else:
    con_mysql.close()
    con_fire.close()
