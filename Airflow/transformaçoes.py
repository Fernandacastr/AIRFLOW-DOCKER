import pandas as pd
import numpy as np
from sqlalchemy import create_engine

def tratamento_dados():

    conexao = create_engine("mysql://root:mysql_321654@192.168.0.170:3306", echo=False)

    df = pd.read_csv("bs140513_032310.csv")
    dados = df

    ##TRANSFORMANDO COLUNA AGE EM INT(REMOVENDO VALOR U)##
    dados["age"] = dados["age"].map(lambda x: x.replace("'",""))
    dados["age"] = dados["age"].replace(["U"], None)
    dados["age"] = dados["age"].astype(int)

    ##DEIXAR COLUNA GENDER APENAS COM M E F(REMOVENDO VALOR U e E)##
    dados["gender"] = dados["gender"].map(lambda x: x.replace("'",""))
    dados["gender"] = dados["gender"].replace(["U", "E"], np.nan)

    ##DEIXAR NULL ONDE COLUNA AMOUNT FOR ZERO##
    dados["amount"].replace(0, np.nan, inplace=True)

    ##CRIAR COLUNA MÊS BASEADO NA COLUNA STEP##
    dados.loc[(dados["step"] <= 30), "month"] = "01"
    dados.loc[(dados["step"] > 30) & (dados["step"] <= 60), "month"] = "02"
    dados.loc[(dados["step"] > 60) & (dados["step"] <= 90), "month"] = "03"
    dados.loc[(dados["step"] > 90) & (dados["step"] <= 120), "month"] = "04"
    dados.loc[(dados["step"] > 120) & (dados["step"] <= 150), "month"] = "05"
    dados.loc[(dados["step"] > 150) & (dados["step"] <= 180), "month"] = "06"

    ##CRIAR COLUNA ANO##
    dados["year"] = "2022"

    ##CRIAR COLUNA DIA##
    dados['day'] = "01"

    ##RETIRANDO ASPAS DAS COLUNAS##
    dados["customer"] = dados["customer"].map(lambda x: x.replace("'",""))
    dados["zipcodeOri"] = dados["zipcodeOri"].map(lambda x: x.replace("'",""))
    dados["merchant"] = dados["merchant"].map(lambda x: x.replace("'",""))
    dados["zipMerchant"] = dados["zipMerchant"].map(lambda x: x.replace("'",""))
    dados["category"] = dados["category"].map(lambda x: x.replace("'",""))


    ##SALVANDO DADOS EM TABELA TRANSACTIONS##
    dados.to_sql(con=conexao, schema="db", name="transactions", if_exists="replace", index=True, index_label="id")


