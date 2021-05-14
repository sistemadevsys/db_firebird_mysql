from decouple import config
# pip install mysql-connector-python
import mysql.connector


try:
    # MYSQL site
    con_mysql = mysql.connector.connect(
        host=config("host_"),
        user=config("user_"),
        password=config("password_"),
        database=config("database_"))

    print("Database connection Mysql made!")

    cursor_mysql = con_mysql.cursor()

    cursor_mysql.execute("""SELECT referencial, ref_forma
                    FROM core_ven_fecha_caixa
                    """)
    t_fc = cursor_mysql.fetchall()

    sql_f = ("""SELECT referencial, nome, usuario_fun_id
                    FROM core_funcionario""")
    cursor_mysql.execute(sql_f)
    # Precisa ter pelo menos dois campos para passar para string
    t_mysql_fun = cursor_mysql.fetchall()

    for ref, rforma in t_fc:
        # delete (não usar porque é backup)
        print(ref)
        if ref:
            sql_Delete_query = """DELETE from core_ven_fecha_caixa
                                    WHERE referencial = %s"""
            cursor_mysql.execute(sql_Delete_query, (ref,))
            con_mysql.commit()

    con_mysql.close()

except ValueError:
    print('Error database')
else:
    con_mysql.close()
