from flask import Flask, render_template, request, jsonify, Response
import pandas as pd
import psycopg2
from psycopg2 import IntegrityError
import io
import time
from datetime import datetime, timedelta, timezone
import os 
from dotenv import load_dotenv  

# Carrega o "cofre" se estiver rodando no seu computador
load_dotenv()

app = Flask(__name__)

# Agora o código pega os links direto das Variáveis de Ambiente!
DATABASE_URL = os.getenv("DATABASE_URL")
LINK_GOOGLE_SHEETS = os.getenv("LINK_GOOGLE_SHEETS")


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


# === SISTEMA DE CACHE ===
df_cache = None
ultima_atualizacao = 0
TEMPO_CACHE = 3600


def obter_dados_planilha():
    global df_cache, ultima_atualizacao
    agora = time.time()

    if df_cache is None or (agora - ultima_atualizacao) > TEMPO_CACHE:
        print("Baixando dados do Google Sheets...")
        df = pd.read_csv(LINK_GOOGLE_SHEETS, on_bad_lines='skip')
        df.columns = df.columns.str.strip()

        coluna_cpf = 'p.num_cpf_pessoa'
        if coluna_cpf in df.columns:
            df[coluna_cpf] = df[coluna_cpf].astype(str).str.replace(
                r'\.0$', '', regex=True).str.zfill(11)

        df_cache = df
        ultima_atualizacao = agora

    return df_cache

# --- ROTAS DA APLICAÇÃO ---


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/cadastrar_admin', methods=['POST'])
def cadastrar_admin():
    dados = request.get_json()
    nome = dados.get('nome')
    email = dados.get('email', '').strip().lower()
    senha = dados.get('senha')

    try:
        conexao = get_db_connection()
        cursor = conexao.cursor()
        cursor.execute(
            "INSERT INTO administrador (nome, email, senha) VALUES (%s, %s, %s)", (nome, email, senha))
        conexao.commit()
        conexao.close()
        return jsonify({"status": "sucesso", "mensagem": "Administrador cadastrado com sucesso!"})
    except IntegrityError:
        return jsonify({"status": "erro", "mensagem": "Este e-mail já está cadastrado!"})


@app.route('/login', methods=['POST'])
def login():
    dados = request.get_json()
    email = dados.get('email', '').strip().lower()
    senha = dados.get('senha')

    conexao = get_db_connection()
    cursor = conexao.cursor()
    cursor.execute(
        "SELECT * FROM administrador WHERE email = %s AND senha = %s", (email, senha))
    admin = cursor.fetchone()
    conexao.close()

    if admin:
        return jsonify({"status": "sucesso", "email": email})
    else:
        return jsonify({"status": "erro", "mensagem": "E-mail ou senha incorretos!"})


@app.route('/registrar_peixe', methods=['POST'])
def registrar_peixe():
    dados = request.get_json()
    cpf_digitado = dados.get('cpf')
    local_cadastro = dados.get('local_cadastro')
    local_retirada = dados.get('local_retirada')
    admin_logado = dados.get('admin')

    if not cpf_digitado or not local_cadastro or not local_retirada:
        return jsonify({"status": "erro", "mensagem": "Preencha todos os campos!"})

    try:
        df = obter_dados_planilha()

        coluna_cpf = 'p.num_cpf_pessoa'
        coluna_familia = 'd.cod_familiar_fam'
        coluna_renda = 'd.vlr_renda_media_fam'

        resultado = df[df[coluna_cpf] == cpf_digitado]

        if resultado.empty:
            return jsonify({"status": "erro", "mensagem": "❌ Bloqueado: CPF não encontrado na base oficial."})

        renda_bruta = resultado.iloc[0][coluna_renda]
        if pd.isna(renda_bruta):
            renda_media = 0.0
        else:
            renda_media = float(str(renda_bruta).replace(',', '.'))

        if renda_media > 500:
            return jsonify({"status": "erro", "mensagem": f"❌ Bloqueado: A renda familiar (R$ {renda_media:.2f}) ultrapassa o limite permitido de R$ 500,00."})

        cod_familiar = str(resultado.iloc[0][coluna_familia])

        conexao = get_db_connection()
        cursor = conexao.cursor()
        cursor.execute(
            "SELECT * FROM cadastro_Peixe WHERE cadastroFamilia = %s", (cod_familiar,))
        familia_existente = cursor.fetchone()

        if familia_existente:
            conexao.close()
            return jsonify({"status": "erro", "mensagem": "❌ Bloqueado: Uma pessoa desta mesma família já retirou o peixe."})

        fuso_br = timezone(timedelta(hours=-3))
        # CORREÇÃO 1: Salva no banco no formato universal AAAA-MM-DD
        hora_exata_br = datetime.now(fuso_br).strftime('%Y-%m-%d %H:%M:%S')

        cursor.execute("""
            INSERT INTO cadastro_Peixe (cpf, cadastroFamilia, local_cadastro, local_retirada, admin_responsavel, data_hora)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (cpf_digitado, cod_familiar, local_cadastro, local_retirada, admin_logado, hora_exata_br))

        conexao.commit()
        conexao.close()

        return jsonify({"status": "sucesso", "mensagem": f"✅ Sucesso! Peixe liberado.\nFamília: {cod_familiar}\nRenda validada: R$ {renda_media:.2f}"})

    except Exception as e:
        print(f"ERRO DETALHADO: {e}")
        return jsonify({"status": "erro", "mensagem": f"Erro interno: Verifique se as colunas da planilha estão corretas. Detalhe: {str(e)}"})


@app.route('/dados_dashboard', methods=['GET'])
def dados_dashboard():
    try:
        conexao = get_db_connection()
        cursor = conexao.cursor()

        # CORREÇÃO 2: Usa o TO_CHAR para o banco formatar como DD/MM/AAAA e enviar para o Dashboard
        cursor.execute('''
            SELECT cpf, cadastroFamilia AS "cadastroFamilia", local_cadastro, local_retirada, admin_responsavel, 
                   TO_CHAR(data_hora, 'DD/MM/YYYY HH24:MI:SS') AS data_hora 
            FROM cadastro_Peixe 
            ORDER BY cadastro_Peixe.data_hora DESC
        ''')

        colunas = [desc[0] for desc in cursor.description]
        registros = [dict(zip(colunas, linha)) for linha in cursor.fetchall()]
        conexao.close()

        total = len(registros)
        cadastro_counts = {}
        retirada_counts = {}

        for r in registros:
            cad = r['local_cadastro']
            ret = r['local_retirada']
            cadastro_counts[cad] = cadastro_counts.get(cad, 0) + 1
            retirada_counts[ret] = retirada_counts.get(ret, 0) + 1

        return jsonify({
            "status": "sucesso",
            "total": total,
            "cadastro_counts": cadastro_counts,
            "retirada_counts": retirada_counts,
            "registros": registros[:50]
        })
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": str(e)})


@app.route('/exportar_csv', methods=['GET'])
def exportar_csv():
    try:
        conexao = get_db_connection()
        cursor = conexao.cursor()

        # CORREÇÃO 3: Aplica o TO_CHAR também na exportação do CSV
        cursor.execute("""
            SELECT cpf as "CPF", 
                   cadastroFamilia as "Código Familiar", 
                   local_cadastro as "Local do Cadastro", 
                   local_retirada as "Local de Retirada",
                   admin_responsavel as "Atendente (Admin)",
                   TO_CHAR(data_hora, 'DD/MM/YYYY HH24:MI:SS') as "Data e Hora" 
            FROM cadastro_Peixe
            ORDER BY cadastro_Peixe.data_hora DESC
        """)

        colunas = [desc[0] for desc in cursor.description]
        dados = cursor.fetchall()
        conexao.close()

        df = pd.DataFrame(dados, columns=colunas)
        output = io.StringIO()
        df.to_csv(output, index=False, sep=';', encoding='utf-8-sig')

        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-disposition": "attachment; filename=relatorio_entregas.csv"}
        )
    except Exception as e:
        return f"Erro ao gerar relatório: {str(e)}"


if __name__ == '__main__':
    app.run(debug=False, port=5000)
