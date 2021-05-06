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
    # VEN_FECHA_CAIXA local
    cursor_fire.execute("""SELECT REFERENCIAL, REF_SAIDA, DATA, REF_CAIXA,
    REF_FORMA, VALOR, DEBITO, COMPLEMENTO, REF_BANCO, DESCONTO
    FROM VEN_FECHA_CAIXA""")
    t_fc = cursor_fire.fetchall()

    # Ven_Fecha_Caixa site
    cursor_mysql.execute("""SELECT referencial, ref_saida, data, ref_caixa,
                    ref_forma, valor, debito, complemento, ref_banco,
                    desconto
                    FROM core_ven_fecha_caixa
                    """)
    t_fcs = cursor_mysql.fetchall()

    # Funcionarios Site
    sql_fun = ("""SELECT referencial, nome, usuario_fun_id
                    FROM core_funcionario""")
    cursor_mysql.execute(sql_fun)
    # Precisa ter pelo menos dois campos para passar para string
    # nao precisa de funcionarios para subir fecha caixa
    t_mysql_fun = cursor_mysql.fetchall()

    for rfu, nome, u_fun_id in t_mysql_fun:
        # Funcionários do site - nao precisa
        print(f'{u_fun_id} - {nome}')

    list_rfire = []
    for rfire, rfs, dtf, rfc, rff, vl, dbt,  comp, rfb, dsc in t_fc:
        # if rfun == 1 or rfun == 2 or rfun == 5 or rfun == 10 or rfun == 22:
        list_rfire.append(rfire)

    list_rfms = []
    for rfms, rfss, dts, rfcs, rffs, vls, dbts, comps, rfbs, dscs in t_fcs:
        # print()
        list_rfms.append(rfms)

    dif_list = []
    for element in list_rfire:
        if element not in list_rfms:
            dif_list.append(element)

    print(f'Lista de referenciais diferentes dos bancos'
          f'(ven_fecha_caixa - falta inserir no site): {dif_list}\n')
    # list_rfos[-2] -1 nula...
    print(f'Referenciais fecha caixa site: {list_rfms}\n')

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

    i = 0
    for rfire, rfs, dtf, rfc, rff, vl, dbt, comp, rfb, dsc in t_fc:

        if len(dif_list) != 0:
            if rfire == dif_list[i]:
                # if dtf is None:
                #     dtf = " "
                # dtf = re.sub(r"^\s+|\s+$", "", dtf)

                sql_i = """INSERT INTO core_ven_fecha_caixa(
                    referencial, ref_saida, data, ref_caixa,
                    ref_forma, valor, debito, complemento,
                    ref_banco, desconto)
                    VALUES(%s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s)"""
                val = (
                    rfire, rfs, dtf, rfc, rff, vl, dbt, comp, rfb, dsc
                    )
                cursor_mysql.executemany(sql_i, (val,))
                con_mysql.commit()
                print('Inserido referencial: ', dif_list[i])
                i += 1

    # atualizar data - ERRO mas atualizou algum
    # Fazer Com Listas e Comparar e Fazer UPDATE
    # for rfire, rfs, dtf, rfc, rff, vl, dbt, comp, rfb, dsc in t_fc:
    #     for rfms, rfss, dts, rfcs, rffs, vls, dbs, coms, rfbs, dscs in t_fcs:
    #         if dtf != dts:
    #             value_column = 'data'
    #             referencial = 'referencial'
    #             comando_sql = f"""UPDATE core_ven_fecha_caixa
    #                             SET {value_column}=('{dtf}')
    #                             WHERE {referencial}=({rfire})"""
    #             cursor_mysql.execute(comando_sql)
    #             con_mysql.commit()

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
