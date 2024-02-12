# importando as bibliotecas
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

sys.path.append(r'C:\Users\SALA443\Desktop\Projetos\josecarlos-dataengineer\WEB_Analise_de_carteira\web_analise\Lib\site-packages')
# from utils.analytics import run_analytics
import streamlit as st 
import matplotlib
import pandas as pd
import uuid
import json
import pymongo
import mysql.connector
import datetime
import pymysql
from sqlalchemy import create_engine
# ----------------------------------------------------------------------------------
# ---------------------- Cria conexão com mongo ------------------------------------
class env_builder():
    '''
    Classe desenvolvida para criação do ambiente, estrutura de pastas e
    conexão com o client.
    Métodos: \n
    * uri_scan() Verifica o contexto de execução \n
    * client_mongo() *Estabelace a conexão \n
    '''

    def uri_scan() -> str:
        '''
        Função utilizada para verificar se o script está sendo executado em 
        container ou localmente. Se for uma execução local, a função retorna
        a string "local", se não "container".    
        '''
        pwd = os.getcwd()
        if pwd == '/workspaces/app':

            return 'container'
        else:

            return 'local'
    def path_scan() -> str:
        '''
        Função utilizada para trazer automaticamente o caminho dos arquivos fonte".    
        '''
        pwd = str(os.getcwd()).replace("\\","/")+"/"
        
        return pwd
    def client_mysql():
        host = {
            'local':'localhost',
            'container':'host.docker.internal'            
            }
        host_str = host[env_builder.uri_scan()]
        
        try:
            mydb = mysql.connector.connect(
            host = host_str,
            user = 'root',
            password = 'root',
            database = 'db')

        except Exception as e:

            pass
        
        return mydb
        
        
    def client_mongo():
        '''
        Esta função estabelece a conexão com o client mongo da biblioteca
        pymongo.
        Para selecionar a uri, é feita a chamada da função *uri_scan(), que 
        indica se o script está sendo executado em container ou localmente.
        '''
        
        uri_selector = {
            'local':'mongodb://localhost:27017/',
            'container':'mongodb://host.docker.internal:27017/',
            'container_SCRAM-SHA-1':'mongodb://user:example@host.docker.internal:27017/?authSource=the_database&authMechanism=SCRAM-SHA-1'
            }

        uri = uri_selector[env_builder.uri_scan()]
        try:
            myclient = pymongo.MongoClient(uri)

        except Exception as e:

            pass
        
        return myclient
# ----------------------------------------------------------------------------------
# ---------------------- Cria conexão com MySQL ------------------------------------
class mysql_etl():
    '''
    Classe que cria a conexão com mysql usando o método *criar. \n
    Parâmetros: \n
        host \n 
        user \n
        password \n
        database \n
    *host*:Para execução local do python usar host=localhost e host=host.docker.internal para execução em container.
    *user*:Usar o mesmo user mencionado no compose.yaml
    *password*:Usar o mesmo password mencionado no compose.yaml
    *database*:Usar o mesmo database mencionado no compose.yaml
    '''
    def __init__(self,host:str,user:str,password:str,database:str) -> None:
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        
    def criar(self):    
        '''
        Método utiliza as bibliotecas sqlalchemy e pymysql para criar a conexão, utilizando os parametros passados como atributos da classe.        
        '''          
            
        # MySQL parametros da conexão com Mysql
        host = self.host
        user = self.user
        password = self.password
        database = self.database
        try:
            # Criação da conexão usando pymysql
            connection = pymysql.connect(host=host, user=user, password=password, database=database)

            # Criação da engine sqlalchemy
            engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        except Exception as e:
            raise Exception('Acesso negado ao Mysql.')

        return engine  
class mysql_connect():
    def consulta_mysql(query:str):

        mydb = env_builder.client_mysql()

        mycursor = mydb.cursor()
        mycursor.execute("""select Papel, Cotação from fundamentus""")

        result = mycursor.fetchall()
        print(type(result))
        # for x in result:
        #     print(x)
        df = pd.DataFrame(result,columns=['Papel',
        'Cotação'])
        
        return df

# ----------------------------------------------------------------------------------
# ---------------------- Consulta operacoes ----------------------------------------
class mongo_etl():
    
    '''
    Realiza a extração, transformação e carga de collections do mongodb
    através do uso dos métodos: \n
    transform: carrega uma lista de collections para uma lista de dicionarios.
    insert_into_mongo: carrega uma lista de arquivos json para o mongodb. \n
    
    '''        

    def mongo_to_dict_list(database:str,collection:str):
        '''
        Função que carrega documentos do mongodb para lista de dicionarios.
        database: Nome do database no mongodb;
        collection: Collection a ser extraída.    
        
        exemplo:
        mongo_to_dict_list(database='plataforma',collection='operacoes')\n
        Retornará uma lista de documentos da coleção operacoes.
        '''    
        myclient = env_builder.client_mongo()
        mydb = myclient[database]
        mycol = mydb[collection]
        doc_list = []
        n = 0
        for col in mycol.find({},{'_id':False}):
            
            doc_list.append(col)
            n += 1

        return doc_list


    def mongo_list_to_list_dict(database:str,collections:list):
        '''
        Função que retorna uma lista de dicionarios de lista de documentos.
        database: Nome do Database
        collections: lista de collections a buscar 
        
        exemplo:
        mongo_list_to_list_dict(
            database='plataforma',
            collections=['usuarios','carteiras','operacoes'])
            
        Retornará uma lista de dicionários de coleções contendo uma lista de documentos.
        '''
        dict_list = []
        param_dict = dict()
        n = 0
        for col in collections:
            param_dict = {col:mongo_etl.mongo_to_dict_list(database,col)}
            dict_list.append(param_dict)
            n += 1

        return dict_list
    
    def carga_mongodb_one(path:str,database:str,file:str):
        '''
        Função que carrega arquivo json de ambiente local para mongodb.
        path: caminho onde estão salvos os arquivos.
        database: nome do banco que receberá os arquivos.
        files: Nome do arquivo com a extensão.
        '''
        
        
        myclient = env_builder.client_mongo()
        path = path
        database = database
        filename = file.replace('.json','')
            
        with open(path+file, 'r+',encoding='UTF-8') as arquivo:
                data = json.load(arquivo)   

                mydb = myclient[database]
                
                string_insert = f"mydb.{filename}.insert_many(data)"
                print(string_insert)
                exec(string_insert)
        return None
    
    def carga_mongodb_many(path:str,database:str,files:list):
        '''
        Função que carrega uma lista de arquivos json de ambiente local para mongodb.
        path: caminho onde estão salvos os arquivos.
        database: nome do banco que receberá os arquivos.
        files: lista de arquivos a serem carregados com suas extensões.
        '''
        for file in files:
            mongo_etl.carga_mongodb_one(path=path,database=database,file=file)
        return None
# ----------------------------------------------------------------------------------
# ----------------------  ----------------------------------------
class run_analytics():
    
    def comparacao_preco_compra_preco_atual():
        # Buscando os dados das compras no mongodb
        mongo = mongo_etl.mongo_to_dict_list(database='plataforma',collection='operacoes')

        # transformando o resultado da consulta em um dataframe
        list_ = []
        for m in mongo:
            ticker = list(m.keys()).pop()
            m = list(m.values())
            qt_vl = m.pop()
            qt = str(list(qt_vl.keys())[0])
            qt_vl.values()
            vl =  str(list(qt_vl.values())[0])
            m.append(ticker)
            m.append(qt)
            m.append(vl)
            print(m)    
            list_.append(m)
        df_mongo = pd.DataFrame(list_,columns=['id_usuario','id_carteira','id','tipo','data','ticker','quantidade','valor'])

        # estabelecendo a conexão
        engine = mysql_etl(
            host = 'localhost',
            user = 'root',
            password = 'root',
            database = 'db'
        ).criar()

        # definindo a consulta
        query = mysql_connect.consulta_mysql('select Papel, Cotação from fundamentus;')

        # atribuindo ao dataframe
        df_fundamentus = query

        # selecionando algumas colunas para analisar
        df_fundamentus = df_fundamentus[['Papel','Cotação']].reset_index()

        # Fazendo join entre os dataframes mongo e mwsql
        df_analytics = df_mongo.merge(df_fundamentus,left_on='ticker',right_on='Papel')

        # Adicionando as colunas total_compra, total_hoje e variacao
        df_analytics['total_compra'] = df_analytics['quantidade'].astype('int64') * df_analytics['valor'].astype('float64')
        df_analytics['total_hoje'] = df_analytics['quantidade'].astype('int64') * df_analytics['Cotação'].astype('float64')
        df_analytics['variacao'] = ((df_analytics['total_hoje'] / df_analytics['total_compra']) - 1) * 100

        # plotando o primeiro gráfico para comparar compra e atual
        fig = df_analytics.plot.bar(x='ticker',y=['total_compra','total_hoje'])


st.title('Comparação preço compra x preço atual')
st.set_option('deprecation.showPyplotGlobalUse', False)
grafico = run_analytics.comparacao_preco_compra_preco_atual()
st.pyplot(grafico)

