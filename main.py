# import insert_into_OS
# import update_OS
from funcoes import t_ordem_servico, mysql_cursor, mysql_con

# Erro
t_os = t_ordem_servico
c_mysql = mysql_cursor
con = mysql_con()
for ref, rfun in t_os:
    # delete (não usar porque é backup)
    if ref:
        sql_Delete_query = """DELETE from core_ordem_servico
                                WHERE referencial = %s"""
        c_mysql.execute(sql_Delete_query, (ref,))
        con.commit()
