import mysql.connector

cnx = mysql.connector.connect(user='root', password='Rasheeda@123',
                              host='127.0.0.1',
                              database='supply_chaindb')

# cursor = cnx.cursor()

cnx.close()

