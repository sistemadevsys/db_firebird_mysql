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
    sql_os = ("""SELECT ref_fun2, descricao, responsavel, valor, equipamento,
                usuario_os_id FROM core_ordem_servico""")
    cursor_mysql.execute(sql_os)
    # Precisa ter pelo menos dois campos para passar para string
    t_mysql = cursor_mysql.fetchall()

    sql_fun = ("""SELECT referencial, nome, usuario_fun_id
                    FROM core_funcionario""")
    cursor_mysql.execute(sql_fun)
    # Precisa ter pelo menos dois campos para passar para string
    t_mysql_fun = cursor_mysql.fetchall()

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
    # funcionarios
    cursor_fire = con_fire.cursor()
    cursor_fire.execute("""SELECT REF_FUN, DESCRICAO, RESPONSAVEL,
                           DT_AGENDA, VALOR, EQUIPAMENTO
                           FROM SER_SERVICOS""")
    t_fire = cursor_fire.fetchall()

    # Para saber formado de data
    cursor_mysql.execute("""SELECT ref_fun2, descricao, dt_agenda,
                    usuario_os_id FROM core_ordem_servico""")
    t_os = cursor_mysql.fetchall()
    # for rf_os, descri, dt, u in t_os:
    #     print(rf_os, descri, dt, u)

    # print(t_fire)
    # for c in cursor_fire.fetchall():
    #     print(c)

    dt_ = datetime.now().strftime('%Y-%m-%d %H:%M')

    for rfun, desc, respo, dt_a, vl, eq in t_fire:
        for ref, nome, user_id in t_mysql_fun:  # funcionario
            if rfun == ref:
                if desc is None:
                    desc = "Sem descrição."
                desc = re.sub(r"^\s+|\s+$", "", desc)
                if eq is None:
                    eq = "Sem equipamento."
                if respo is None:
                    respo = "Sem responsavel."
                if dt_a is None or dt_a == "":
                    # formato django: 2020-12-30 10:59:00
                    dt_a = '2021-01-14 11:11:11 1.1'

                # print(
                #     type(rfun), type(desc), type(respo), type(dt_a),
                #     type(vl))
                # print(rfun, desc, respo, dt_a, vl, eq)

                # cursor_mysql.execute(
                #                     f'''UPDATE core_ordem_servico
                #                     SET
                #                     descricao=("{desc}"),
                #                     responsavel=("{respo}"),
                #                     dt_agenda=({dt_a}),
                #                     valor=(({vl})),
                #                     equipamento=("({eq})")
                #                     WHERE ref_fun2=({rfun})''')

                # Insert ordem_servico OK mas nao mostra na pagina
                sql_i = """INSERT INTO core_ordem_servico(
                    ref_fun2, descricao, responsavel, dt_agenda,
                    dt_entrada, dt_atualizada, valor,
                    equipamento, usuario_os_id)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                val = (rfun, desc, respo, dt_a, dt_, dt_, vl, eq, user_id)
                cursor_mysql.executemany(sql_i, (val,))
                con_mysql.commit()

                # cursor_mysql.executemany(
                #         f'''INSERT INTO core_ordem_servico (
                #             ref_fun2, descricao, responsavel,
                #             dt_agenda, valor, equipamento,
                #             usuario_os_id
                #         ) VALUES (
                #             ({rfun}),("{desc}"),("{respo}"),
                #             ("{dt_a}"),({vl}),("{eq}"),({user_id})
                #         )''')
                # con_mysql.commit()

    # for rfun, desc, respo, vl, eq in t_fire:
    #     for ref, nome, user_id in t_mysql_fun:
    #         if rfun == ref:
    #             # print(user_id)
    #             sql_i = '''INSERT INTO `core_ordem_servico` (
    #                                     `referencial`,`descricao`,
    #                                     `responsavel`,`valor`,`equipamento`,
    #                                     `usuario_os_id`
    #                                 ) VALUES (%d,%s,%s,%d,%s,%d)'''
    #             cursor_mysql.execute(
    #                 sql_i, (rfun, desc, respo, vl, eq, user_id)
    #                 )
    # con_mysql.commit()

    # for rfun2, descricao, rpl, vlr, equip, usuario_os in t_mysql:
    #     for rfun, desc, respo, vl, eq in t_fire:
    #         if descricao != desc and rfun2 == rfun:
    #             # print(referencial, nome, rf, desc, respo, vl, eq)
    #             # Atualiza se DESCRIÇÃO diferente senão cria
    #             # INSERT INTO depois atualizar
    #             cursor_mysql.execute(
    #                                 f'''UPDATE core_ordem_servico
    #                                 SET
    #                                 descricao=("{desc}"),
    #                                 responsavel=("{respo}"),
    #                                 valor=({vl}),
    #                                 equipamento=("{eq}")
    #                                 from core_ordem_servico
    #                                 INNER JOIN core_funcionario ON
    #                                 core_funcionario.referencial=({rfun})''')

except ValueError:
    print('Error database')
else:
    con_mysql.close()
    con_fire.close()
