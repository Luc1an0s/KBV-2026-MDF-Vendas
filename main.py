import os
import json
import pandas as pd
import gspread
import mysql.connector
from datetime import datetime
from sshtunnel import SSHTunnelForwarder
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

load_dotenv()

# Configurações de Ambiente
ssh_host = os.getenv("SSH_HOST")
ssh_port = int(os.getenv("SSH_PORT", 22))
ssh_user = os.getenv("SSH_USER")
ssh_password = os.getenv("SSH_PASSWORD")
mysql_host = os.getenv("DB_HOST")
mysql_user = os.getenv("DB_USER")
mysql_password = os.getenv("DB_PASS")
mysql_db = os.getenv("DB_NAME")
mysql_port = int(os.getenv("DB_PORT", 3306))
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
ABA_NOME = os.getenv("ABA_NOME")

ARQUIVO_CONTROLE = os.path.abspath("controle_incremental.json")

def salvar_controle(date, time, nfno):
    # Formata para YYYYMMDD para o JSON
    data_formatada = str(date).replace("-", "").split(" ")[0]
    
    # Proteção contra datas inválidas ou nulas
    if len(data_formatada) < 8 or "1970" in data_formatada or "NaT" in data_formatada:
        return
        
    with open(ARQUIVO_CONTROLE, "w") as f:
        json.dump({"date": data_formatada, "time": int(time), "nfno": int(nfno)}, f, indent=4)

def inicializar_controle():
    if not os.path.exists(ARQUIVO_CONTROLE):
        inicio = datetime.now().replace(day=1).strftime('%Y%m%d')
        salvar_controle(inicio, 0, 0)

def ler_controle():
    with open(ARQUIVO_CONTROLE, "r") as f:
        return json.load(f)

def conectar_banco():
    server = SSHTunnelForwarder(
        (ssh_host, ssh_port), 
        ssh_username=ssh_user, 
        ssh_password=ssh_password, 
        remote_bind_address=(mysql_host, mysql_port)
    )
    server.start()
    conn = mysql.connector.connect(
        host="127.0.0.1", 
        port=server.local_bind_port, 
        user=mysql_user, 
        password=mysql_password, 
        database=mysql_db
    )
    return conn, server

def conectar_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credenciais_google.json", scope)
    return gspread.authorize(creds)

def main():
    inicializar_controle()
    controle = ler_controle()

    hoje = datetime.now()
    inicio_mes_str = hoje.replace(day=1).strftime('%Y%m%d')
    data_json = str(controle["date"]).replace("-", "")

    # Define ponto de partida (mês atual ou último registro)
    if data_json < inicio_mes_str:
        data_busca, hora_busca, nota_busca = inicio_mes_str, 0, 0
    else:
        data_busca, hora_busca, nota_busca = data_json, controle["time"], controle["nfno"]

    print(f"Buscando após: {data_busca} | {hora_busca}")

    conn, server = conectar_banco()

    query = """
        SELECT
            m.time AS col_time,
            m.nfno AS col_nfno,
            CAST(m.date AS CHAR) AS col_date,
            custp.cpf_cgc AS document,
            m.custno AS client_internal_code,
            custp.name AS client_name,
            CONCAT(m.nfno, '/', m.nfse) AS order_code,
            CAST(m.date AS CHAR) AS payment_date,
            trim(f.prdno) AS sku,
            prd.name AS product_description,
            type.name AS category,
            f.qtty AS quantity,
            f.sl_price * f.qtty AS total_value
        FROM pxaprd f
        JOIN pxa m ON f.xano = m.xano
        JOIN custp ON m.custno = custp.no
        JOIN prd ON f.prdno = prd.no
        JOIN type ON prd.typeno = type.no
        WHERE m.storeno = 1
        AND prd.typeno = 5
        AND (
              (m.date > %s)
           OR (m.date = %s AND m.time > %s)
           OR (m.date = %s AND m.time = %s AND m.nfno > %s)
        )
        ORDER BY m.date, m.time, m.nfno
    """

    params = (data_busca, data_busca, hora_busca, data_busca, hora_busca, nota_busca)

    try:
        df = pd.read_sql(query, conn, params=params)
    finally:
        conn.close()
        server.stop()

    if df.empty:
        print("Nenhum dado novo.")
        return

    # --- AJUSTE DE VALORES NUMÉRICOS ---
    # quantity / 1000 e total_value / 100000
    df['quantity'] = (df['quantity'] / 1000).round(3)
    df['total_value'] = (df['total_value'] / 100000).round(2)

    # --- TRATAMENTO DE DATAS PARA O GOOGLE SHEETS ---
    df['col_date'] = pd.to_datetime(df['col_date'], errors='coerce')
    df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')

    ultima_linha = df.iloc[-1]
    
    client = conectar_sheets()
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(ABA_NOME)
    
    # Se a planilha estiver vazia, adiciona o cabeçalho
    if not sheet.get_all_values():
        sheet.append_row(df.columns.tolist())

    # Prepara o upload formatando como string para o Sheets reconhecer
    df_upload = df.copy()
    df_upload['col_date'] = df_upload['col_date'].dt.strftime('%Y-%m-%d')
    df_upload['payment_date'] = df_upload['payment_date'].dt.strftime('%Y-%m-%d')
    
    # Substitui valores nulos por vazio
    df_upload = df_upload.fillna('')
    
    dados = df_upload.values.tolist()
    
    # Upload em blocos de 100 linhas
    for i in range(0, len(dados), 100):
        sheet.append_rows(
            dados[i:i + 100], 
            value_input_option="USER_ENTERED"
        )

    # Pega a data da última linha para o controle
    if pd.notnull(ultima_linha['col_date']):
        nova_data_json = ultima_linha['col_date'].strftime('%Y%m%d')
    else:
        nova_data_json = data_busca

    salvar_controle(
        date=nova_data_json,
        time=ultima_linha['col_time'],
        nfno=ultima_linha['col_nfno']
    )
    
    print(f"Sucesso! {len(df)} linhas enviadas. Controle atualizado para: {nova_data_json}")

if __name__ == "__main__":
    main()