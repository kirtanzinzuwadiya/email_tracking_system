import pymysql
import pandas as pd


conn = pymysql.connect(
    host='localhost',
    user='root',         
    password='kz#1212',  
    database='linkedin'
)


query = "SELECT * FROM data2"
df = pd.read_sql(query, conn)

df.to_excel("linkedin_data.xlsx", index=False)


conn.close()

print("Data exported to linkedin_data.xlsx successfully!")
