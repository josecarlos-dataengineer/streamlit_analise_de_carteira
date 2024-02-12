# Web_Analise_de_carteira
Neste repositório crio uma aplicação Streamlit para criar um cenário parecido com o que foi feito no repositório ['Analise_de_carteira'](https://github.com/josecarlos-dataengineer/Analise_de_carteira). Mas nesse caso, utilizo o streamlit para que os dados sejam inseridos pelo usuário. O Streamlit aqui é usado como inteface para que o usuário insira dados no MongoDB, e depois esses dados sejam comparados com os dados disponíveis no MySQL.
Para realizar localmente as operações, pode-se utilizar a estrutura criada no repositório ['Analise_de_carteira'](https://github.com/josecarlos-dataengineer/Analise_de_carteira).

# Tecnologias usadas:
* Mongodb
* Streamlit
* MySQL
* Python

## Status do projeto:
| Etapa | Status |
| ------| ------ |
| Streamlit | Feito |
| Gráfico | Feito |
| Conexão com MongoDB | Feito |
| Conexão com MySQL | Feito |
| Gráficos no Plotly | Pendente |

# Execução do script
streamlit run home.py --server.port=8501 --server.address=0.0.0.0

# Próximos passos:
A criação de gráficos que mostrem mais informações das carteiras de ações, como média de P/L, perfil (participação do Governo nas ações) e outras.