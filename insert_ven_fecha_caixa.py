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
    cursor_fire.execute("""SELECT referencial, ref_saida, data, ref_forma,
                        valor, complemento, ref_caixa, debito, saldo,
                        ref_entrada, cheque, n_documento, ref_servicos,
                        ref_banco, ref_conta, ref_subconta, arquivo_morto,
                        codigo_fiscal, ref_pagar, val_servicos, val_peca,
                        desconto, data_compensado, ref_cliente, troco, local,
                        ref_subconta2, ref_subconta3, comissao, vl_comissao,
                        ref_receber, ref_fun, ref_cheque, hora, ref_pagamento,
                        ref_transf_caixa, ref_setor, ref_locacao, ref_pag,
                        ref_mesa, ref_garcom, ref_empresa
                        FROM VEN_FECHA_CAIXA""")
    t_fc = cursor_fire.fetchall()

    # Ven_Fecha_Caixa site
    cursor_mysql.execute("""SELECT referencial, ref_saida, data, ref_forma,
                        valor, complemento, ref_caixa, debito, saldo,
                        ref_entrada, cheque, n_documento, ref_servicos,
                        ref_banco, ref_conta, ref_subconta, arquivo_morto,
                        codigo_fiscal, ref_pagar, val_servicos, val_peca,
                        desconto, data_compensado, ref_cliente, troco, local,
                        ref_subconta2, ref_subconta3, comissao, vl_comissao,
                        ref_receber, ref_fun, ref_cheque, hora, ref_pagamento,
                        ref_transf_caixa, ref_setor, ref_locacao, ref_pag,
                        ref_mesa, ref_garcom, ref_empresa
                        FROM core_ven_fecha_caixa
                        """)
    t_fcs = cursor_mysql.fetchall()

    list_rfire = []
    for (rf, rs, dt, rfm, vl, c, rc, db, s, re, ch, nd, rse, rb, rco,
         rsco, arq, cfc, rfp, vlse, vlpe, dsc, dtc, recl, tc, lc, rs2, rs3,
         cms, vlcms, rfcb, refn, rfch, hr, refpa, rtsf, rfst, rlcc, rpg,
         rfma, rfga, rfem) in t_fc:
        list_rfire.append(rf)

    list_rfms = []
    for (refs, rs, dt, rfm, vl, c, rc, db, s, re, ch, nd, rse, rb, rco,
         rsco, arq, cfc, rfp, vlse, vlpe, dsc, dtc, recl, tc, lc, rs2, rs3,
         cms, vlcms, rfcb, refn, rfch, hr, refpa, rtsf, rfst, rlcc, rpg,
         rfma, rfga, rfem) in t_fcs:
        # print()
        list_rfms.append(refs)

    dif_list = []
    for element in list_rfire:
        if element not in list_rfms:
            dif_list.append(element)

    print(f'Lista de referenciais diferentes dos bancos'
          f'(ven_fecha_caixa - falta inserir no site): {dif_list}\n')
    # list_rfos[-2] -1 nula...
    # print(f'Referenciais fecha caixa site: {list_rfms}\n')

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
    for (rfl, rs, dt, rfm, vl, c, rc, db, s, re, ch, nd, rse, rb, rco, rsco,
         arq, cfc, rfp, vlse, vlpe, dsc, dtc, recl, tc, lc, rs2, rs3, cms,
         vlcms, rfcb, refn, rfch, hr, refpa, rtsf, rfst, rlcc, rpg, rfma,
         rfga, rfem) in t_fc:
        if len(dif_list) != 0:
            if rfl == dif_list[i]:
                # if dtf is None:
                #     dtf = " "
                # dtf = re.sub(r"^\s+|\s+$", "", dtf)
                sql_i = """INSERT INTO core_ven_fecha_caixa(
                        referencial, ref_saida, data, ref_forma,
                        valor, complemento, ref_caixa, debito, saldo,
                        ref_entrada, cheque, n_documento, ref_servicos,
                        ref_banco, ref_conta, ref_subconta, arquivo_morto,
                        codigo_fiscal, ref_pagar, val_servicos, val_peca,
                        desconto, data_compensado, ref_cliente, troco, local,
                        ref_subconta2, ref_subconta3, comissao, vl_comissao,
                        ref_receber, ref_fun, ref_cheque, hora, ref_pagamento,
                        ref_transf_caixa, ref_setor, ref_locacao, ref_pag,
                        ref_mesa, ref_garcom, ref_empresa)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s)"""
                val = (
                    rfl, rs, dt, rfm, vl, c, rc, db, s, re,
                    ch, nd, rse, rb, rco, rsco, arq, cfc, rfp, vlse,
                    vlpe, dsc, dtc, recl, tc, lc, rs2, rs3, cms, vlcms,
                    rfcb, refn, rfch, hr, refpa, rtsf, rfst, rlcc, rpg, rfma,
                    rfga, rfem
                    )
                cursor_mysql.executemany(sql_i, (val,))
                con_mysql.commit()
                print('Inserido referencial: ', dif_list[i])
                i += 1

    # Comparar e Fazer UPDATE (Após o Insert)
    for (rfl, rs, dt, rfm, vl, c, rc, db, s, re, ch, nd, rse, rb, rco, rsco,
         arq, cfc, rfp, vlse, vlpe, dsc, dtc, recl, tc, lc, rs2, rs3, cms,
         vlcms, rfcb, refn, rfch, hr, refpa, rtsf, rfst, rlcc, rpg, rfma,
         rfga, rfem) in t_fc:
        for (rfsi, rsi, dti, rfmi, vli, ci, rci, dbi, si, rei, chi, ndi,
             rsei, rbi, rcoi, rscoi, arqi, cfci, rfpi, vlsei, vlpei, dsci,
             dtci, recli, tci, lci, rs2i, rs3i, cmsi, vlcmsi, rfcbi, refni,
             rfchi, hri, refpai, rtsfi, rfsti, rlcci, rpgi, rfmai, rfgai,
             rfemi) in t_fcs:
            if rfl == rfsi:
                if rs != rsi:
                    value_column = 'ref_saida'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rs})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if dt != dti:
                    value_column = 'data'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=('{dt}')
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rfm != rfmi:
                    value_column = 'ref_forma'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rfm})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if vl != vli:
                    print(vl, ' - ', vli)
                    value_column = 'valor'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({vl})
                                    WHERE {referencial}=({rfl})"""
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                    print('Atualizando: ', value_column, rfl)
                if c != ci:
                    value_column = 'complemento'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=('{c}')
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rc != rci:
                    value_column = 'ref_caixa'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rc})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if db != dbi:
                    print(db, ' - ', dbi)
                    value_column = 'debito'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({db})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if s != si:
                    value_column = 'saldo'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({s})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if re != rei:
                    value_column = 'ref_entrada'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({re})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if ch != chi:
                    value_column = 'cheque'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=('{ch}')
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if nd != ndi:
                    value_column = 'n_documento'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({nd})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rse != rsei:
                    value_column = 'ref_servicos'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rse})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rb != rbi:
                    value_column = 'ref_banco'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rb})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rco != rcoi:
                    value_column = 'ref_conta'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rco})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rsco != rscoi:
                    value_column = 'ref_subconta'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rsco})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if arq != arqi:
                    value_column = 'arquivo_morto'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({arq})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if cfc != cfci:
                    value_column = 'codigo_fiscal'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({cfc})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rfp != rfpi:
                    value_column = 'ref_pagar'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rfp})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if vlse != vlsei:
                    value_column = 'val_servicos'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({vlse})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if vlpe != vlpei:
                    value_column = 'val_peca'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({vlpe})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if vlpe != vlpei:
                    value_column = 'val_peca'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({vlpe})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if dsc != dsci:
                    value_column = 'desconto'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({dsc})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if dtc != dtci:
                    value_column = 'data_compensado'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=('{dtc}')
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if recl != recli:
                    value_column = 'ref_cliente'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({recl})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if tc != tci:
                    value_column = 'troco'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=('{tc}')
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if lc != lci:
                    value_column = 'local'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=('{lc}')
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rs2 != rs2i:
                    value_column = 'ref_subconta2'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rs2})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rs3 != rs3i:
                    value_column = 'ref_subconta3'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rs3})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if cms != cmsi:
                    value_column = 'comissao'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({cms})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if vlcms != vlcmsi:
                    value_column = 'vl_comissao'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({vlcms})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rfcb != rfcbi:
                    value_column = 'ref_receber'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rfcb})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if refn != refni:
                    value_column = 'ref_fun'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({refn})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rfch != rfchi:
                    value_column = 'ref_cheque'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rfch})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if hr != hri:
                    value_column = 'hora'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=('{hr}')
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if refpa != refpai:
                    value_column = 'ref_pagamento'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({refpa})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rtsf != rtsfi:
                    value_column = 'ref_transf_caixa'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rtsf})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rfst != rfsti:
                    value_column = 'ref_setor'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rfst})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rlcc != rlcci:
                    value_column = 'ref_locacao'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rlcc})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rpg != rpgi:
                    value_column = 'ref_pag'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rpg})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rfma != rfmai:
                    value_column = 'ref_mesa'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rfma})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rfga != rfgai:
                    value_column = 'ref_garcom'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rfga})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
                    cursor_mysql.execute(comando_sql)
                    con_mysql.commit()
                if rfem != rfemi:
                    value_column = 'ref_empresa'
                    referencial = 'referencial'
                    comando_sql = f"""UPDATE core_ven_fecha_caixa
                                    SET {value_column}=({rfem})
                                    WHERE {referencial}=({rfl})"""
                    print('Atualizando: ', value_column, rfl)
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
