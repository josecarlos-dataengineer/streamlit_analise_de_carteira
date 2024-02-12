# importando as bibliotecas
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import streamlit as st 

st.set_page_config(
    page_title="Análise de carteiras de investimento",
    page_icon=":earth:"
)   
st.sidebar.success("Opções")