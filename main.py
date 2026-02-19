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

def get_env_int(name, default):
    value = os.environ.get(name, "")
    return int(value) if value.strip().isdigit() else default

ssh_host = os.environ.get("SSH_HOST")
ssh_port = get_env_int("SSH_PORT", 22)
ssh_user = os.environ.get("SSH_USER")
ssh_password = os.environ.get("SSH_PASSWORD")

mysql_host = os.environ.get("DB_HOST")
mysql_user = os.environ.get("DB_USER")
mysql_password = os.environ.get("DB_PASS")
mysql_db = os.environ.get("DB_NAME")
mysql_port = get_env_int("DB_PORT", 3306)

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
ABA_NOME = "TESTE"

ARQUIVO_CONTROLE = os.path.abspath("controle_incremental.json")

MODO_TESTE = False  # True para puxar período retroativo, False para incremental normal
DATA_INICIO_TESTE = "20260101"
DATA_FIM_TESTE = "20260131"

def inicializar_controle():
    if not os.path.exists(ARQUIVO_CONTROLE):
        controle = {
            "pxa": {"date": "", "time": 0, "nfno": 0},
            "xalog2": {"date": "", "time": 0, "nfno": 0}
        }
        with open(ARQUIVO_CONTROLE, "w") as f:
            json.dump(controle, f, indent=4)

def ler_controle():
    with open(ARQUIVO_CONTROLE, "r") as f:
        return json.load(f)

def salvar_controle(origem, date, time, nfno):
    controle = ler_controle()
    controle[origem] = {"date": date, "time": int(time), "nfno": int(nfno)}
    with open(ARQUIVO_CONTROLE, "w") as f:
        json.dump(controle, f, indent=4)

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
        database=mysql_db,
        connect_timeout=30
    )
    return conn, server

def conectar_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credenciais_google.json", scope)
    return gspread.authorize(creds)


def buscar_dados_pxa(controle=None):
    if MODO_TESTE:
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
                TRIM(f.prdno) AS sku,
                prd.name AS product_description,
                type.name AS category,
                f.qtty AS quantity,
                xaprd2.precoUnitario AS total_value
            FROM pxaprd f
            JOIN pxa m ON f.xano = m.xano
            JOIN xaprd2 ON f.xano = xaprd2.xano AND f.prdno = xaprd2.prdno AND f.storeno = xaprd2.storeno
            JOIN custp ON m.custno = custp.no
            JOIN prd ON f.prdno = prd.no
            JOIN type ON prd.typeno = type.no
            WHERE m.storeno = 1
              AND prd.no IN (94533, 94538, 96122, 97782, 97831, 94519, 94523, 94524, 94525, 94526, 94527, 94529, 94530, 94532, 94537, 94539, 94542, 94543, 94927, 96004, 96005, 96006, 96007, 96108, 96109, 96287, 96288, 96289, 96401, 96402, 96407, 96718, 96719, 96720, 96721, 96722, 96723, 96724, 96883, 96887, 96889, 96895, 97421, 97423, 97424, 97425, 97426, 97427, 97428, 97429, 97651, 98164)
              AND m.date BETWEEN %s AND %s
            ORDER BY m.date, m.time, m.nfno
        """
        params = (DATA_INICIO_TESTE, DATA_FIM_TESTE)
    else:
        data_busca, hora_busca, nota_busca = controle["pxa"]["date"], controle["pxa"]["time"], controle["pxa"]["nfno"]
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
                TRIM(f.prdno) AS sku,
                prd.name AS product_description,
                type.name AS category,
                f.qtty AS quantity,
                xaprd2.precoUnitario AS total_value
            FROM pxaprd f
            JOIN pxa m ON f.xano = m.xano
            JOIN xaprd2 ON f.xano = xaprd2.xano AND f.prdno = xaprd2.prdno AND f.storeno = xaprd2.storeno
            JOIN custp ON m.custno = custp.no
            JOIN prd ON f.prdno = prd.no
            JOIN type ON prd.typeno = type.no
            WHERE m.storeno = 1
              AND prd.no IN (94533, 94538, 96122, 97782, 97831, 94519, 94523, 94524, 94525, 94526, 94527, 94529, 94530, 94532, 94537, 94539, 94542, 94543, 94927, 96004, 96005, 96006, 96007, 96108, 96109, 96287, 96288, 96289, 96401, 96402, 96407, 96718, 96719, 96720, 96721, 96722, 96723, 96724, 96883, 96887, 96889, 96895, 97421, 97423, 97424, 97425, 97426, 97427, 97428, 97429, 97651, 98164)
              AND ((m.date > %s) OR (m.date = %s AND m.time > %s) OR (m.date = %s AND m.time = %s AND m.nfno > %s))
            ORDER BY m.date, m.time, m.nfno
        """
        params = (data_busca, data_busca, hora_busca, data_busca, hora_busca, nota_busca)
    return query, params

def buscar_dados_xalog2(controle=None):
    if MODO_TESTE:
        query = """
            SELECT
                x.time AS col_time,
                inv.nfNfno AS col_nfno,
                CAST(x.date AS CHAR) AS col_date,
                custp.cpf_cgc AS document,
                x.custno AS client_internal_code,
                custp.name AS client_name,
                CONCAT(MID(x.doc,1,LOCATE('/',x.doc)-1),'/',MID(x.doc,LOCATE('/',x.doc)+1,2)) AS order_code,
                CAST(x.date AS CHAR) AS payment_date,
                TRIM(x.prdno) AS sku,
                prd.name AS product_description,
                type.name AS category,
                x.qtty AS quantity,
                -1 * (((x.qtty / 1000) * (x.price / 100)) - ABS(x.discount / 100)) AS total_value
            FROM xalog2 x
            JOIN custp ON x.custno = custp.no
            JOIN prd ON x.prdno = prd.no
            JOIN type ON prd.typeno = type.no
            JOIN inv ON x.storeno = inv.storeno AND x.xano = inv.auxLong1
            WHERE x.storeno = 1
              AND prd.no IN (94533, 94538, 96122, 97782, 97831, 94519, 94523, 94524, 94525, 94526, 94527, 94529, 94530, 94532, 94537, 94539, 94542, 94543, 94927, 96004, 96005, 96006, 96007, 96108, 96109, 96287, 96288, 96289, 96401, 96402, 96407, 96718, 96719, 96720, 96721, 96722, 96723, 96724, 96883, 96887, 96889, 96895, 97421, 97423, 97424, 97425, 97426, 97427, 97428, 97429, 97651, 98164)
              AND x.qtty < 0
              AND x.date BETWEEN %s AND %s
            ORDER BY x.date, x.time, inv.nfNfno
        """
        params = (DATA_INICIO_TESTE, DATA_FIM_TESTE)
    else:
        data_busca, hora_busca, nota_busca = controle["xalog2"]["date"], controle["xalog2"]["time"], controle["xalog2"]["nfno"]
        query = """
            SELECT
                x.time AS col_time,
                inv.nfNfno AS col_nfno,
                CAST(x.date AS CHAR) AS col_date,
                custp.cpf_cgc AS document,
                x.custno AS client_internal_code,
                custp.name AS client_name,
                CONCAT(MID(x.doc,1,LOCATE('/',x.doc)-1),'/',MID(x.doc,LOCATE('/',x.doc)+1,2)) AS order_code,
                CAST(x.date AS CHAR) AS payment_date,
                TRIM(x.prdno) AS sku,
                prd.name AS product_description,
                type.name AS category,
                x.qtty AS quantity,
                --1 * (((x.qtty / 1000) * (x.price / 100)) - ABS(x.discount / 100)) AS total_value
            FROM xalog2 x
            JOIN custp ON x.custno = custp.no
            JOIN prd ON x.prdno = prd.no
            JOIN type ON prd.typeno = type.no
            JOIN inv ON x.storeno = inv.storeno AND x.xano = inv.auxLong1
            WHERE x.storeno = 1
              AND prd.no IN (94533, 94538, 96122, 97782, 97831, 94519, 94523, 94524, 94525, 94526, 94527, 94529, 94530, 94532, 94537, 94539, 94542, 94543, 94927, 96004, 96005, 96006, 96007, 96108, 96109, 96287, 96288, 96289, 96401, 96402, 96407, 96718, 96719, 96720, 96721, 96722, 96723, 96724, 96883, 96887, 96889, 96895, 97421, 97423, 97424, 97425, 97426, 97427, 97428, 97429, 97651, 98164)
              AND x.qtty < 0
              AND ((x.date > %s) OR (x.date = %s AND x.time > %s) OR (x.date = %s AND x.time = %s AND inv.nfNfno > %s))
            ORDER BY x.date, x.time, inv.nfNfno
        """
        params = (data_busca, data_busca, hora_busca, data_busca, hora_busca, nota_busca)
    return query, params


def processar_e_salvar(df, origem, sheet):
    if df.empty:
        print(f"Nenhum dado novo de {origem}.")
        return

    df['quantity'] = (df['quantity'] / 1000).round(3)
    if origem == "pxa":
        df['total_value'] = (df['total_value'] / 100).round(2)
    elif origem == "xalog2":
        df['total_value'] = df['total_value'].round(2)  

    df['col_date'] = pd.to_datetime(df['col_date'], errors='coerce')
    df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')

    
    ultima_linha = df.iloc[-1]
    df_upload = df.copy()
    df_upload['col_date'] = df_upload['col_date'].dt.strftime('%Y-%m-%d')
    df_upload['payment_date'] = df_upload['payment_date'].dt.strftime('%Y-%m-%d')
    df_upload = df_upload.fillna('')

    
    if not sheet.get_all_values():
        sheet.append_row(df_upload.columns.tolist())

    dados = df_upload.values.tolist()
    for i in range(0, len(dados), 100):
        sheet.append_rows(dados[i:i+100], value_input_option="USER_ENTERED")

    
    if not MODO_TESTE:
        nova_data = ultima_linha['col_date'].strftime('%Y%m%d')
        salvar_controle(origem, nova_data, ultima_linha['col_time'], ultima_linha['col_nfno'])

    print(f"{len(df)} linhas de {origem} enviadas.")


def main():
    inicializar_controle()
    controle = ler_controle()

    conn, server = conectar_banco()
    client = conectar_sheets()
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(ABA_NOME)

    try:
        # PXA
        query, params = buscar_dados_pxa(controle)
        df_pxa = pd.read_sql(query, conn, params=params)
        processar_e_salvar(df_pxa, "pxa", sheet)

        # XALOG2
        query, params = buscar_dados_xalog2(controle)
        df_xalog2 = pd.read_sql(query, conn, params=params)
        processar_e_salvar(df_xalog2, "xalog2", sheet)

    finally:
        conn.close()
        server.stop()

if __name__ == "__main__":
    main()
