import psycopg2

#configurações
host = "localhost"
dbname = "SRAG"
user = "postgres"
password = "123"


#inicia a conexão com o banco
# conn = psycopg2.connect(con_string)
#inicializa o cursor
#cursor = conn.cursor()

#encerra o cursor e a conexão
#conn.commit()
#cursor.close()
#conn.close()

def CreatStringConnection():
    con_string = "host={0} user={1} dbname={2} password={3}".format(host, user, dbname, password)
    return con_string
def InitConnection():
    conn = psycopg2.connect(CreatStringConnection())
    print("Connection established")
    return conn

