# importando as bibliotecas
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
sys.path.append(r'C:\Users\SALA443\Desktop\Projetos\josecarlos-dataengineer\WEB_Analise_de_carteira\streamlit_analise_de_carteira\web_env\Lib\site-packages')
# from utils.analytics import usuario,carteiras
import streamlit as st 
import pandas as pd
import uuid
import json
import pymongo

# ----------------------------------------------------------------------------------
# ---------------------- Consulta MONGO opetações ----------------------------------
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
# ---------------------- Gera arquivos das carteiras--------------------------------
class usuario():
    '''
    operacao(id_usuario='156',id_carteira='51561',ticker='bbdc4',qtde=50,preco=38.6,operacao='venda').criar_json()       
    '''
    
    def __init__(self,nome,perfis:list,carteiras:list):


        self.nome = nome
        self.perfis = perfis
        self.carteiras = carteiras
    
    def criar_dict(self):
        
        document = {
            'id_usuario':uuid.uuid4().hex[:8],
            'nome':self.nome,
            'perfis':self.perfis,
            'carteiras':self.carteiras
        }

        return document
    
    def criar_json(self) -> dict:    
            
        json_file = usuario(self.nome,self.perfis,self.carteiras).criar_dict()
        
        if os.path.isfile('usuarios.json') == False:
            with open('usuarios.json', 'w', encoding='utf-8') as arquivo:
                arquivo.write("[]")
            
        with open("usuarios.json", 'r+',encoding='UTF-8') as arquivo:
            data = json.load(arquivo)            
            data.append(json_file)
            # json.dump(data, arquivo,indent=4)        

        with open("usuarios.json", mode='w', encoding='UTF-8') as arquivo:
            json.dump(data, arquivo,indent=4)
            
        return json_file
    
# ----------------------------------------------------------------------------------
# ---------------------- Gera arquivos das carteiras--------------------------------

class carteiras():
    def __init__(self,nome:str,perfil:list,tags:list,ticker:dict):
        self.nome = nome
        self.perfil = perfil
        self.tags = tags
        self.ticker = ticker

        
    
    def criar_dict(self) -> dict:
        if self.tags == None:
            self.tags = ['padrao']
        if self.perfil == None:
            self.perfil = ['padrao']
            
        document = {
            'id':uuid.uuid4().hex[:8],
            'nome':self.nome,
            'perfil':self.perfil,
            'tags':self.tags,
            'tickers':self.ticker
            
        }
        
        return document
    
    def criar_json(self) -> dict:    
            
        json_file = carteiras(self.nome,self.perfil,self.tags,self.ticker).criar_dict()         
        # json_file = json.dumps(json_file,indent=2)  
        
        if os.path.isfile('carteiras.json') == False:
            with open('carteiras.json', 'w', encoding='utf-8') as arquivo:
                arquivo.write("[]")
                
        with open("carteiras.json", 'r+',encoding='UTF-8') as arquivo:
            data = json.load(arquivo)            
            data.append(json_file)
            
        with open("carteiras.json", "w") as arquivo:
            json.dump(data, arquivo,indent=4)
            
        return json_file           

# carregando lista de tickers

# st.text("Carregando")
df = pd.read_csv('C:/Users/SALA443/Desktop/Projetos/josecarlos-dataengineer/WEB_Analise_de_carteira/streamlit_analise_de_carteira/pages/fundamentus.csv',sep=';',encoding='utf-8')


st.sidebar.title("Menu")
st.title('Cadastro de usuário')

perfis = ['conservador','moderado','arrojado','indefinido']
form = st.form(key="Ticker", clear_on_submit=True)
tickers = list(df['Papel'])

with form:
    input_id_nome = st.text_input("Nome:", placeholder="Insira seu nome de usuário")
    input_id_perfil = st.selectbox("Selecione o perfil:", perfis)
    input_id_carteira = st.text_input("carteira:", placeholder="Insira o nome da carteira")

    botao_submit = st.form_submit_button("Enviar dados")
            
    if botao_submit:
            
            commit = usuario(nome=input_id_nome,
                    perfis=input_id_perfil,
                    carteiras=input_id_carteira).criar_json()
            id = commit['id_usuario']
           
            st.write(f"Anote seu id de usuário: {id}")
            
            carteiras(
                nome=commit['carteiras'],
                perfil=commit['perfis'],
                tags=['fake'],
                ticker={'':{'':''}

                }).criar_json()

            filesl = ['operacoes.json'] 
            # path = r'C:\Users\SALA443\Desktop\Projetos\josecarlos-dataengineer\Analise_de_carteira\testes\python\\'
            path = env_builder.path_scan()

            database = 'plataforma'

            mongo_etl.carga_mongodb_many(path=path,files=filesl,database=database)


     