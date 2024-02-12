FROM python:3.9-bullseye

# RUN  pip install --upgrade pip \ pymongo \ pandas \ pyarrow \ mysql-connector-python
RUN mkdir -p /workspaces/app
RUN apt update 
RUN apt install nano

COPY /pages /workspaces/app/pages
COPY /home.py /workspaces/app/home.py

WORKDIR /workspaces/app
RUN pip install -r requirements.txt
RUN pip install --no-cache-dir cryptography

ENV PYTHONPATH=/usr/local/lib/python3.9/dist-packages
ENV ME_CONFIG_MONGODB_URL=mongodb://root:example@mongo:27017/
ENV ME_CONFIG_MONGODB_ADMINUSERNAME=root
ENV ME_CONFIG_MONGODB_ADMINPASSWORD=example

EXPOSE 8501
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health
ENTRYPOINT ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]