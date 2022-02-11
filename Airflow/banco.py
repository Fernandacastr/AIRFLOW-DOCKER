from mysql.connector import Error
import mysql.connector as msql

def criando_banco():
    try:
        conn = msql.connect(host='192.168.0.170', user='root', password='mysql_321654')
        if conn.is_connected():
         cursor = conn.cursor()
         cursor.execute("CREATE DATABASE db")
         print("Banco de dados criado com sucesso")
    except Error as e:
        print("Erro ao conectar ao MySQL", e)
