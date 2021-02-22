# https://pypi.org/project/firebirdsql/
# pip install firebirdsql

import firebirdsql
from decouple import config

# pip install mysql-connector-python
import mysql.connector
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
                    dt_agenda, valor, equipamento, usuario_os_id
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
        for rfos, rfun_os, descri, dta, vl_os, eqp, user_id in t_os:
            # Se Tabela OS (t_os) estiver vazia não entra aqui.
            # Primeiro fazer somente com t_ser e t_mysql_fun
            for rfu, nome, u_fun_id in t_mysql_fun:  # Funcionario
                if rfire == rfos:
                    if vl != vl_os:
                        # print(rfire, rfun, rfos, rfun_os, vl, vl_os)
                        value_column = 'valor'
                        referencial = 'referencial'
                        # ALTERAR UM POR UM
                        comando_sql = f"""UPDATE core_ordem_servico
                                        SET {value_column}=({vl})
                                        WHERE {referencial}=({rfire})"""
                        cursor_mysql.execute(comando_sql)
                        con_mysql.commit()
                    if descri != desc:
                        value_column = 'descricao'
                        referencial = 'referencial'
                        if desc is not None:
                            # se descrição não nulo substitui onde tem aspas
                            # simples porque ao fazer update ocorre erro
                            # com '{desc}' pois ja tem aspas simples...
                            desc = desc.replace("'", "")
                        # ALTERAR UM POR UM
                        comando_sql = f"""UPDATE core_ordem_servico
                                        SET {value_column}=('{desc}')
                                        WHERE {referencial}={rfire}"""
                        cursor_mysql.execute(comando_sql)
                        con_mysql.commit()
                    if eqp != eq:
                        value_column = 'equipamento'
                        referencial = 'referencial'
                        if eq is not None:
                            eq = eq.replace("'", "")
                        # ALTERAR UM POR UM
                        comando_sql = f"""UPDATE core_ordem_servico
                                        SET {value_column}=('{eq}')
                                        WHERE {referencial}=({rfire})"""
                        cursor_mysql.execute(comando_sql)
                        con_mysql.commit()

    con_mysql.close()
    con_fire.close()
except ValueError:
    print('Error database')
else:
    con_mysql.close()
    con_fire.close()
