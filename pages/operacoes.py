# importando as bibliotecas
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
sys.path.append(r'C:\Users\SALA443\Desktop\Projetos\josecarlos-dataengineer\WEB_Analise_de_carteira\streamlit_analise_de_carteira\web_env\Lib\site-packages')
# from utils.analytics import operacao,env_builder
import streamlit as st 
import pandas as pd
import json
import datetime
import uuid
import pymongo

# ----------------------------------------------------------------------------------
# ---------------------- Conecta Mongo e SQL ---------------------------------------
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
# ---------------------- Consulta cadastro -----------------------------------------
    
class consulta_cadastro():
    
    def dicionario_usuarios():
        with open(r'C:\Users\SALA443\Desktop\Projetos\josecarlos-dataengineer\WEB_Analise_de_carteira\streamlit_analise_de_carteira\usuarios.json', 'r+',encoding='UTF-8') as arquivo:
                data = json.load(arquivo)
        return data
    
    
    def retorna_lista_de_usuarios():   
        datata = consulta_cadastro.dicionario_usuarios()
        lista = len(datata)
        n = 0
        lista_usuarios = []
        for u in range(lista):
            id = list(datata[n].values())[0]
        
            n += 1
            lista_usuarios.append(id)
            
        return lista_usuarios
# ----------------------------------------------------------------------------------
# ---------------------- Gera arquivos das operações--------------------------------

class operacao():
    '''
    operacao(id_usuario='156',id_carteira='51561',ticker='bbdc4',qtde=50,preco=38.6,operacao='venda').criar_json()       


    '''
    
    def __init__(self,id_usuario,id_carteira,ticker:str,qtde:int,preco:float,operacao:str):

        self.id_usuario = id_usuario
        self.id_carteira = id_carteira
        self.ticker = ticker
        self.qtde = qtde
        self.preco = preco
        self.operacao = operacao
    
    def criar_dict(self):
        
        document = {
            'id_usuario':self.id_usuario,
            'id_carteira':self.id_carteira,
            'id':uuid.uuid4().hex[:8],
            'tipo':self.operacao,
            'data':str(datetime.date.today()),
            self.ticker:{self.qtde:self.preco}}
        return document
    
    def criar_json(self) -> dict:    
            
        json_file = operacao(self.id_usuario,self.id_carteira,self.ticker,self.qtde,self.preco,self.operacao).criar_dict()
        # json_file = json.dumps(json_file) 
        # print(json_file)
        
        if os.path.isfile('operacoes.json') == False:
            with open('operacoes.json', 'w', encoding='utf-8') as arquivo:
                arquivo.write("[]")
                
        with open("operacoes.json", 'r') as arquivo:
            data = json.load(arquivo)            
            data.append(json_file)
            
        with open("operacoes.json", mode='w', encoding='utf-8') as arquivo:
            json.dump(data, arquivo,indent=4)


# ----------------------------------------------------------------------------------
# ---------------------- Carrega dados no mongo ------------------------------------       
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
# ---------------------- Script de registro ----------------------------------------

# carregando lista de tickers

# st.text("Carregando")
df = pd.read_csv('C:/Users/SALA443/Desktop/Projetos/josecarlos-dataengineer/WEB_Analise_de_carteira/streamlit_analise_de_carteira/pages/fundamentus.csv',sep=';',encoding='utf-8')

lista_usuarios = consulta_cadastro.retorna_lista_de_usuarios()

st.sidebar.title("menu")
st.title('Análise de carteira')


form = st.form(key="Ticker", clear_on_submit=True)
tickers = list(df['Papel'])

with form:
    input_id_usuario = st.text_input("id_usuario:", placeholder="id_usuario")    
    input_id_carteira = st.text_input("id_carteira:", placeholder="Insira o nome da carteira")
    input_ticker = st.selectbox("Selecione o ticker:", tickers)
    input_quantidade = st.text_input("Quantidade:", placeholder="Quantidade comprada")
    input_valor = st.text_input("Preço:", placeholder="Preço pago")
    input_data = st.date_input("Data da compra")
    botao_submit = st.form_submit_button("Enviar dados")

    
    if botao_submit:
            operacao(id_usuario=input_id_usuario,
                     id_carteira=input_id_carteira,
                     ticker=input_ticker,
                     qtde=input_quantidade,
                     preco=input_valor,
                     operacao='compra').criar_json()  
            
            
            filesl = ['operacoes.json'] 
            # path = r'C:\Users\SALA443\Desktop\Projetos\josecarlos-dataengineer\Analise_de_carteira\testes\python\\'
            path = env_builder.path_scan()

            database = 'plataforma'

            mongo_etl.carga_mongodb_many(path=path,files=filesl,database=database)
            
            
