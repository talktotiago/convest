import sqlite3 as sq
from sqlite3.dbapi2 import Cursor, connect
from sqlite3 import Error
from typing import Text
from alpha_vantage.fundamentaldata import FundamentalData
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
from pandas.core import indexing

key = 'J3S5AJTYSX8GQCUO'

#Coletar os dados pela API Alpha Vantage
fd = FundamentalData(key, output_format = 'pandas')
ts = TimeSeries (key, output_format='pandas')
tickers = ["B3SA3.SAO", "PETR4.SAO"]
closing_values = []
dates_last_week = []
dates_most_recent = []


def Coletar_Dados():
    for i in range (0, len(tickers)):
        data_daily, meta_data = ts.get_daily_adjusted(symbol=tickers[i], outputsize ='compact')
        dates_most_recent.append(data_daily.index[0])
        closing_values.append(data_daily['4. close'][6])
        #Sétimo Dia
        dates_last_week.append(data_daily.index[6])
        print("Dados Coletados")
        print(tickers,closing_values,dates_last_week,dates_most_recent)
###############################
Coletar_Dados()
###############################
#Tabular Dados Coletados em um banco de dados em sql
def Tabular_Dados():
    Table = {'Tickers': tickers,
        'Mais_Recentes' : dates_most_recent,
        'Ultima_Semana' :dates_last_week,
        'Closing': closing_values
        }
    df = pd.DataFrame(Table, columns = ['Tickers', 'Mais_Recentes', 'Ultima_Semana', 'Closing'])
    print(df)
    print("Criando banco de dados")
    con = sq.connect("consulta_api.sqlite")
    df.to_sql('consulta_api', con, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None, dtype=None, method=None)
    con.close()
#########################3
Tabular_Dados()
###########################
#Comparar os dados do 'consulta_api.sqlite' com os dados do banco de dados 'servidor_bd.sqlite'
def Comparar_Dados():
    print("Comparando Banco de Dados")
    con = sq.connect("consulta_api.sqlite")
    print("Conectado ao BD consulta_api.sqlite")
    df_api = pd.read_sql_query("SELECT Tickers, Ultima_Semana, Closing from consulta_api", con)
    print(df_api)
    con.close()
    con = sq.connect("servidor_bd.sqlite")
    df_servidor = pd.read_sql_query("SELECT Tickers, Ultima_Semana, Closing from consulta_api", con)
    print(df_servidor)
    con.close()
    print(df_servidor['Closing'][0])
    print(df_api['Closing'][0])
    for i in range(0,len(df_api['Closing'])):
        if df_api['Closing'][i]==df_servidor['Closing'][i]:
            print("match")
        else:
            print("don't match")
            con = sq.connect("servidor_bd.sqlite")
            with con:
                cur = con.cursor()
                cur.execute("UPDATE consulta_api SET Closing= "+repr(df_api['Closing'][i])+" WHERE Closing= "+repr(df_servidor['Closing'][i]))
                print('servidor.sqlite updated to match')

##############################################################
Comparar_Dados()

