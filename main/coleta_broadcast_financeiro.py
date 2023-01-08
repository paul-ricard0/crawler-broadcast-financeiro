import chromedriver_binary # Abrir chromedriver sem o .exe
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from datetime import datetime
from time import sleep
import pandas as pd

#VARIAVEIS AMBIENTE
from dotenv import load_dotenv, find_dotenv
import os
load_dotenv(find_dotenv())
key_id = os.getenv('AWS_ACCESS_KEY_ID')
secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# CONFIGURATION AWS
import boto3
import awswrangler as wr
session = boto3.Session(
    region_name = "us-east-1",
    aws_access_key_id=key_id,
    aws_secret_access_key=secret_key
)


link = 'http://broadcast.com.br/cadernos/financeiro/'

df = pd.DataFrame(columns=['categoria', 'data', 'titulo', 'texto'])

df_more_news = pd.DataFrame(columns=['categoria', 'data', 'titulo'])


def hora_atual():
    return datetime.now().strftime('%d/%m/%y %I:%M:%S')


def upload_s3(df, final_path, table):
    try:
        wr.s3.to_csv(
            df = df,
            path = 's3://crawlers123/raw-zone/broadcast/financeiro/'+final_path,
            dataset = True, 
            mode = 'append', 
            encoding="UTF-8",
            database = 'broadcast',
            table=table,
            boto3_session = session,
            schema_evolution=True
        ) 
        print(f'SUCESSO NA AWS: {hora_atual()}!')
    except:
        print('ERRO AO SUBIR PARA AWS')

def click(xpath): # click pelo xpath
    browser.find_element(By.XPATH, xpath).click()
    sleep(1)
    
def get_text(xpath): # pegando o texto pelo xpath
    return browser.find_element(By.XPATH, xpath).text


options = Options() # Options para o navegador
#options.add_argument('--headless')
options.add_argument('window-size=800,800')
browser = webdriver.Chrome(options=options)


print('\n'+hora_atual()+'\n')

browser.get(link)
sleep(2)

#Aceitando cookies
click('/html/body/div[1]/div/a[1]')
sleep(1)

# Pegandos a 1 primeira materia
titulo = get_text('/html/body/div[3]/div[1]/div/div[1]')
texto = get_text('/html/body/div[3]/div[1]/div/div[2]')
conteudo = titulo+'\n'+texto



for i in range(1,4): 
    # Pegandos primeira filera de materias
    titulo = get_text(f'/html/body/div[3]/div[2]/div[{i}]/div[1]')
    texto = get_text(f'/html/body/div[3]/div[2]/div[{i}]/div[2]')
    conteudo = conteudo + titulo+'\n'+texto
    
    # Pegandos segunda filera de materias
    titulo = get_text(f'/html/body/div[3]/div[3]/div[{i}]/div[1]')
    texto = get_text(f'/html/body/div[3]/div[3]/div[{i}]/div[2]')
    conteudo = conteudo + titulo+'\n'+texto  
    
    sleep(0.5) 
    print(f'COLETA {i} FEITA!!!')

# Limpando e transformqando e lista o conteudo
conteudo = conteudo.replace('continuar lendo', '')
conteudo = conteudo.split('\n')


while len(conteudo)!=1: # Passando conteudo para o dataframe
    df.loc[len(df)] = [conteudo[0].strip(), conteudo[1].strip(), conteudo[2].strip(), conteudo[3].strip()]
    del[conteudo[0]]
    del[conteudo[0]]
    del[conteudo[0]]
    del[conteudo[0]]

# Adicionando data da coleta
df=df.assign(data_atualizacao=hora_atual()) 
print(df)  

#Salvando em .csv
df.to_csv(r'C:\Users\paulo\OneDrive\Documentos\MEGAsync\P-Ricardo\Projetos\crawler\broadcast_financeiro\coletas\bdc_financeiro.csv', encoding='UTF-8', index=False) 



''' PARTE 2 - COLETANDO MAIS NOTICIAS  '''
conteudo=''
# Coletando mais noticias
click('/html/body/div[6]/div/div/div/div[10]/button')
for j in range(1,18): 
    # Pegandos primeira filera de materias
    titulo = get_text(f'/html/body/div[6]/div/div/div/div[{j}]/div[1]')
    texto = get_text(f'/html/body/div[6]/div/div/div/div[{j}]/div[2]')
    conteudo = titulo+'\n'+texto +'\n'+conteudo 
    
    sleep(0.5)
    print(f'COLETA {j+3} FEITA!!!')

# Limpando e transformqando e lista o conteudo
conteudo = conteudo.replace('continuar lendo', '')
conteudo = conteudo.split('\n')


df_more_news = pd.DataFrame(columns=['categoria', 'data', 'titulo'])

while len(conteudo)!=1: # Passando conteudo para o dataframe
    df_more_news.loc[len(df_more_news)] = [conteudo[0].strip(), conteudo[1].strip(), conteudo[2].strip()]
    del[conteudo[0]]
    del[conteudo[0]]
    del[conteudo[0]]

df_more_news=df_more_news.assign(data_atualizacao=hora_atual()) 

#Salvando em .csv

print(df_more_news)
df_more_news.to_csv(r'C:\Users\paulo\OneDrive\Documentos\MEGAsync\P-Ricardo\Projetos\crawler\broadcast_financeiro\coletas\bdc_financeiro_more_news.csv', encoding='UTF-8', index=False) 


browser.close()# Fechando navegador


# Subindo para AWS
upload_s3(df, 'noticias/', 'noticias')
upload_s3(df_more_news, 'more_news/', 'more_news')

print('\n'+hora_atual()+'\n')