# https://pypi.org/project/firebirdsql/
# pip install firebirdsql

import firebirdsql
from decouple import config
# pip install mysql-connector-python
import mysql.connector

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
    # users
    cursor_mysql = con_mysql.cursor()
    sql_user = ("SELECT id, username FROM auth_user")
    cursor_mysql.execute(sql_user)
    t_mysql = cursor_mysql.fetchall()

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
    cursor_fire.execute("""SELECT REFERENCIAL, NOME, FONE1, ENDERECO
                           FROM FIN_FUNCIONARIOS """)
    t_fire = cursor_fire.fetchall()

    for id, username in t_mysql:
        # print(id, username)
        for referencial, nome, fone, endereco in t_fire:
            # só Atualiza
            if username == nome:
                # print(referencial, nome, fone, endereco)
                cursor_mysql.execute(
                                    f'''UPDATE core_funcionario
                                    SET referencial=("{referencial}"),
                                    nome=("{nome}"),
                                    fone1=("{fone}"),
                                    endereco=("{endereco}")
                                    WHERE referencial=("{referencial}")''')

except ValueError:
    print('Error database')
else:
    con_mysql.close()
    con_fire.close()
