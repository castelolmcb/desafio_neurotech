import os
from re import M
import pandas as pd
import zipfile
import requests as rq
from io import BytesIO
from bs4 import BeautifulSoup

url = 'http://ftp.dadosabertos.ans.gov.br/FTP/PDA/TISS/HOSPITALAR/2019/'

with open('htmldoftp', 'wb') as file:
    file.write(rq.get(url).content)

dado = open('htmldoftp', 'r')    
sopa = BeautifulSoup(dado, 'html.parser')

estados = []
for link in sopa.find_all('a'):
    estados += [link.get('href')]

estados_fim = estados[5:]

for n in range(len(estados_fim)):
    link_por_estado = 'http://ftp.dadosabertos.ans.gov.br/FTP/PDA/TISS/HOSPITALAR/2019/{}'.format(estados_fim[n])
    print(link_por_estado)
    os.makedirs(estados_fim[n], exist_ok=True)
    rq.get(link_por_estado,).content
    with open('htmldecadaestado', 'wb') as file:
        file.write(rq.get(link_por_estado).content)
    sopa_por_estado = BeautifulSoup(open('htmldecadaestado', 'r'), 'html.parser')
    estados_pasta = []
    for link in sopa_por_estado.find_all('a'):
        estados_pasta += [link.get('href')]
    estados_pasta_fim = estados_pasta[5:]
    dataframes = []
    for j in range(len(estados_pasta_fim)):
        link_arquivo_pasta = link_por_estado+estados_pasta_fim[j]
        print(link_arquivo_pasta)
        arquivo_zip = BytesIO(rq.get(link_arquivo_pasta).content)
        vamo_extrair = zipfile.ZipFile(arquivo_zip)
        vamo_extrair.extractall("./{}".format(estados_fim[n]))
        abrindodf = pd.read_csv('./{}{}csv'.format(estados_fim[n], estados_pasta_fim[j][:-3]), sep=';')
        dataframes += [abrindodf]
    final = pd.concat(dataframes)
    nome_file_estado = str("./DADOS_AGRUPADOS/"+estados_fim[n][:-1]+"_GERAL.csv")
    final.to_csv(nome_file_estado)
    
    dataframe_con1 = []
    for h in range(0,len(dataframes),2):
        dataframe_con1 += [dataframes[h]]
        df_merged = pd.concat(dataframe_con1)
        nome_file = str("./DADOS_AGRUPADOS/"+estados_fim[n][:-1]+"_CONS.csv")
        df_merged.to_csv(nome_file)
    
    dataframe_con2 = []
    for k in range(1,len(dataframes),2):
        dataframe_con2 += [dataframes[k]]
        df_merged = pd.concat(dataframe_con2)
        nome_file = str("./DADOS_AGRUPADOS/"+estados_fim[n][:-1]+"_DET.csv")
        df_merged.to_csv(nome_file)    

dataframesestados = []
for i in range(len(estados_fim)):
    dadodecadaestado = ["./DADOS_AGRUPADOS/"+estados_fim[i][:-1]+"_GERAL.csv"]
    for m in range(len(dadodecadaestado)):
        dataframesestados += [pd.read_csv(dadodecadaestado[m])]
    

dadofinal = pd.concat(dadodecadaestado)
dadofinal.to_csv("./DADOS_AGRUPADOS/GERAL.csv")
