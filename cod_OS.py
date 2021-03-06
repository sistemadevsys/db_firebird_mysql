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
    # Servicos
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

    dt_ = datetime.now().strftime('%Y-%m-%d %H:%M')

    for rfire, rfun, desc, respo, dt_a, dt_p,  vl, eq in t_ser:
        for rfos, rfun_os, descri, dta, vl_os, user_id in t_os:
            # Se Tabela OS (t_os) estiver vazia não entra aqui.
            # Primeiro fazer somente com t_ser e t_mysql_fun
            for rfu, nome, u_fun_id in t_mysql_fun:  # Funcionario
                if rfire == rfos and vl != vl_os:
                    print(rfire, rfun, rfun_os, rfos, vl, vl_os)
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
                    if vl is None:
                        vl = 0.0

                    value_column = 'valor'
                    # ALTERAR UM POR UM
                    comando_sql = f"""UPDATE core_ordem_servico
                                    SET {value_column}={vl}
                                    WHERE referencial={rfire}"""
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                # Primeiro para inserir todas OS - mudar if rfu == rfun:
                elif rfu == rfun:  # user e OS

                    if desc is None:
                        desc = "Sem descrição."
                    desc = re.sub(r"^\s+|\s+$", "", desc)
                    if eq is None:
                        eq = "Sem equipamento."
                    eq = re.sub(r"^\s+|\s+$", "", eq)
                    if respo is None:
                        respo = "Sem responsavel."
                    if dt_a is None or dt_a == "":
                        dt_a = '2021-01-14 11:11:11 1.1'
                    if dt_p is None or dt_p == "":
                        dt_p = '2021-01-14 11:11:11'

                    sql_i = """INSERT INTO core_ordem_servico(
                        referencial, ref_fun2, descricao, responsavel,
                        dt_agenda, dt_pagamento, dt_entrada,
                        dt_atualizada, valor, equipamento,
                        confirmar, finalizar, usuario_os_id)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s)"""
                    val = (
                        rfire, rfun, desc, respo, dt_a, dt_p, dt_, dt_,
                        vl, eq, 0, 0, u_fun_id
                        )
                    cursor_mysql.executemany(sql_i, (val,))
                    con_mysql.commit()

except ValueError:
    print('Error database')
else:
    con_mysql.close()
    con_fire.close()
