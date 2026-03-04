from flask import Flask, jsonify, request, g, send_file
from flask_cors import CORS
from flasgger import Swagger, swag_from
import os
from datetime import datetime
import secrets
from functools import wraps
from dotenv import load_dotenv
from supabase import create_client, Client
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from envio import enviar_email_boas_vindas, reenviar_email_token
import time
import io
import pandas as pd
import psycopg2


API_KEY_CACHE = {}

TTL_VALIDACAO_SEGUNDOS = 300
INTERVALO_UPDATE_BD = 3600

# ============================================
# CONFIGURAÇÕES INICIAIS
# ============================================
load_dotenv()

app = Flask(__name__)

# Configuração de CORS
CORS(app, resources={
    r"/*": {
        "origins": ["http://localhost:3000", "http://localhost:5173", "http://localhost:8080", "http://127.0.0.1:8001", "https://frontend-xi-eight-69.vercel.app"],
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# Inicialização do Supabase
supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)
supabase_uri = os.environ.get("SUPABASE_URI")

# ============================================
# FUNÇÕES AUXILIARES
# ============================================

# Limitando a 20 requisições por minuto por usuário (identificado por API Key ou IP)
def identificar_usuario():
    token = request.headers.get("Authorization")
    if token:
        return token
    
    return get_remote_address()

limiter = Limiter(
    key_func=identificar_usuario,
    app=app,
    default_limits=["20 per minute"]
)

@app.errorhandler(429)
def limite_excedido(e):
    return jsonify({
        "erro": "Limite de requisições excedido.",
        "detalhes": "Você só pode fazer 20 requisições por minuto. Aguarde um momento."
    }), 429

def generate_unique_api_key():
    while True:
        key = secrets.token_urlsafe(32)
        # Verifica se a chave já existe no Supabase
        response = supabase.table('user').select('*').eq('api_key', key).execute()
        if not response.data:
            return key

def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = None
        auth_header = request.headers.get('Authorization')
        
        if auth_header and auth_header.startswith('Bearer '):
            parts = auth_header.split(' ')
            if len(parts) == 2:
                api_key = parts[1]
        
        if not api_key:
            api_key = request.args.get('api_key')

        if not api_key:
            return jsonify({'error': 'Chave de API ausente'}), 401

        agora = time.time()
        usuario = None
        precisa_validar_banco = True
        
        # 1. TENTA PEGAR DO CACHE EM MEMÓRIA
        cache_entry = API_KEY_CACHE.get(api_key)
        if cache_entry and (agora - cache_entry['ultima_validacao'] < TTL_VALIDACAO_SEGUNDOS):
            usuario = cache_entry['user']
            precisa_validar_banco = False

        # 2. SE NÃO ESTÁ NO CACHE OU EXPIROU, VAI NO SUPABASE (SELECT)
        if precisa_validar_banco:
            response = supabase.table('user').select('*').eq('api_key', api_key).execute()
            
            if not response.data:
                return jsonify({'error': 'Chave de API inválida'}), 401
            
            usuario = response.data[0]
            
            # Salva ou atualiza a chave no cache
            API_KEY_CACHE[api_key] = {
                'user': usuario,
                'ultima_validacao': agora,
                # Mantém o histórico do último update se já existir no cache, senão zera
                'ultimo_update_bd': cache_entry['ultimo_update_bd'] if cache_entry else 0
            }
            cache_entry = API_KEY_CACHE[api_key]

        # 3. ATUALIZA O 'last_used_at' NO BANCO (UPDATE COM THROTTLING)
        # Só faz o update se passou mais de 1 hora (3600s) desde o último
        if agora - cache_entry['ultimo_update_bd'] > INTERVALO_UPDATE_BD:
            try:
                data_iso = datetime.utcnow().isoformat()
                supabase.table('user').update({'last_used_at': data_iso}).eq('id', usuario['id']).execute()
                
                # Registra no cache que acabamos de atualizar o banco
                API_KEY_CACHE[api_key]['ultimo_update_bd'] = agora
            except Exception as e:
                print(f"Erro ao atualizar uso: {e}")
        
        g.current_user = usuario
        return f(*args, **kwargs)
    return decorated

def get_pagination_params(page, per_page):
    """Converte page/per_page para o formato start/end do Supabase (0-indexed)"""
    start = (page - 1) * per_page
    end = start + per_page - 1
    return start, end

def format_paginated_response(response, page, per_page):
    """Formata a saída padrão de paginação"""
    total = response.count if response.count is not None else 0
    return {
        'data': response.data,
        'total': total,
        'page': page,
        'per_page': per_page,
        'pages': (total + per_page - 1) // per_page if total > 0 else 0
    }

# ============================================
# ROTAS DA API
# ============================================

# Puxar dados completos da api
@app.route('/oiatuarial_api/<tabela>')
@require_api_key
def exportar_parquet_dinamico(tabela):

    tabelas_permitidas = ['tabua_original', 'tabuas_previsoes', 'metricas_erro', 'nacoes_unidas']
    if tabela not in tabelas_permitidas:
        return jsonify({"erro": "Tabela inválida"}), 400

    try:
        conn = psycopg2.connect(supabase_uri)

        query = f"SELECT * FROM {tabela}"

        df = pd.read_sql_query(query, conn)
        conn.close()

        buffer = io.BytesIO()
        
        df.to_parquet(buffer, index=False, engine='pyarrow')

        buffer.seek(0)

        return send_file(
            buffer,
            as_attachment=True,
            download_name=f"{tabela}.parquet",
            mimetype='application/octet-stream'
        )
        
    except Exception as e:
        return jsonify({"erro": f"Falha ao gerar Parquet: {str(e)}"}), 500

@app.route('/cadastro', methods=['POST'])
def cadastro_usuario():
    data = request.get_json()
    nome = data.get('nome')
    email = data.get('email').lower()
    usualidade = data.get('uso')
    descricao = data.get('descricao')

    if not nome or not email:
        return jsonify({"mensagem": "Nome e email são obrigatórios!", "status": "error"}), 400
    
    # Verifica email duplicado
    existente = supabase.table('user').select('*').eq('email', email).execute()
    if existente.data:
        token_antigo = existente.data[0]['api_key']

        email_enviado = reenviar_email_token(email, token_antigo,nome.split()[0].capitalize())
        
        if email_enviado:
            return jsonify({"mensagem": "E-mail já cadastrado. Reenviamos o seu token para a caixa de entrada!"}), 200
        else:
            return jsonify({"erro": "Erro ao reenviar o e-mail de recuperação."}), 500
    
    agora = datetime.utcnow().isoformat()
    novo_usuario = {
        'name': nome,
        'email': email,
        'usualidade': usualidade,
        'api_key': generate_unique_api_key(),
        'created_at': agora,
        'last_used_at': agora,
        'instituicao_descricao': descricao
    }
    
    supabase.table('user').insert(novo_usuario).execute()
    email_enviado = enviar_email_boas_vindas(email, novo_usuario['api_key'], nome.split()[0].capitalize())

    if email_enviado:
        return jsonify({"mensagem": "Cadastro realizado! Verifique seu e-mail para pegar o token."}), 201
    
    return jsonify({"erro": "Cadastro feito, mas houve um erro ao enviar o e-mail."}), 500

@app.route('/')
def list_routes():
    output = []
    for rule in app.url_map.iter_rules():
        if rule.endpoint != 'static':
            methods = ', '.join(rule.methods - {'OPTIONS', 'HEAD'})
            output.append(f"Endpoint: {rule.endpoint} | Métodos: [{methods}] | Caminho: {rule}")
    return "<br>".join(output)

# --- DIMENSÕES ---

@app.route('/dimensoes/anos_original')
def get_anos_original():
    # Supabase não tem um comando nativo .distinct() fácil via API, então agrupamos no Python
    response = supabase.rpc('get_anos_originais_unicos').execute()
    return jsonify([item['ano'] for item in response.data])

@app.route('/dimensoes/anos_projecoes')
def get_anos_projecoes():
    response = supabase.rpc('get_anos_previsoes_unicos').execute()
    return jsonify([item['ano'] for item in response.data])

@app.route('/dimensoes/locais')
def get_locais():
    response = supabase.table('dim_locais').select('*').execute()
    return jsonify(response.data)

@app.route('/dimensoes/faixas')
def get_faixas():
    response = supabase.table('dim_faixas').select('*').execute()
    return jsonify(response.data)

@app.route('/dimensoes/sexos')
def get_sexos():
    response = supabase.table('dim_sexo').select('*').execute()
    return jsonify(response.data)

@app.route('/dimensoes/modelos')
def get_modelos():
    response = supabase.table('dim_modelo').select('*').execute()
    return jsonify(response.data)

# --- DADOS PRINCIPAIS ---

@app.route('/original')
@require_api_key
def get_original():
    query = supabase.table('tabua_original').select('*', count='estimated')

    ano = request.args.get('ano', type=int)
    sexo = request.args.get('sexo', type=int)
    local = request.args.get('local', type=int)
    faixa = request.args.get('faixa', type=int)

    if ano: query = query.eq('ano', ano)
    if sexo: query = query.eq('id_sexo', sexo)
    if local: query = query.eq('id_local', local)
    if faixa: query = query.eq('id_faixa', faixa)
    
    query = query.order('ano').order('id_faixa')

    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 1000, type=int)
    start, end = get_pagination_params(page, per_page)
    
    response = query.range(start, end).execute()
    return jsonify(format_paginated_response(response, page, per_page))

@app.route('/previsoes')
@require_api_key
def get_tabua_projecoes():
    query = supabase.table('tabuas_previsoes').select('*', count='estimated')

    ano = request.args.get('ano', type=int)
    sexo = request.args.get('sexo', type=int)
    local = request.args.get('local', type=int)
    faixa = request.args.get('faixa', type=int)
    modelo = request.args.get('modelo', type=int)

    if ano: query = query.eq('ano', ano)
    if sexo: query = query.eq('id_sexo', sexo)
    if local: query = query.eq('id_local', local)
    if faixa: query = query.eq('id_faixa', faixa)
    if modelo: query = query.eq('id_modelo', modelo)
    
    query = query.order('ano')

    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 1000, type=int)
    start, end = get_pagination_params(page, per_page)
    
    response = query.range(start, end).execute()
    return jsonify(format_paginated_response(response, page, per_page))

@app.route('/metricas')
@require_api_key
def get_metricas_erro():
    query = supabase.table('metricas_erro').select('*', count='estimated')
    
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 1000, type=int)
    start, end = get_pagination_params(page, per_page)
    
    response = query.range(start, end).execute()
    return jsonify(format_paginated_response(response, page, per_page))

@app.route('/nacoes_unidas')
@require_api_key
def get_nacoes_unidas():
    query = supabase.table('nacoes_unidas').select('*', count='estimated')

    ano = request.args.get('ano', type=int)
    sexo = request.args.get('sexo', type=str)
    local = request.args.get('local', type=str)
    faixa_etaria = request.args.get('faixa_etaria', type=int)

    if ano: query = query.eq('ano', ano)
    if sexo: query = query.eq('sexo', sexo)
    if local: query = query.eq('local', local)
    if faixa_etaria: query = query.eq('faixa_etaria', faixa_etaria)
    
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 1000, type=int)
    start, end = get_pagination_params(page, per_page)
    
    response = query.range(start, end).execute()
    return jsonify(format_paginated_response(response, page, per_page))

# ============================================
# EXECUÇÃO
# ============================================

if __name__ == '__main__':
    print(f"API rodando em: http://0.0.0.0:8001")
    print(f"Documentação Swagger: http://localhost:8001/docs/")
    app.run(host='0.0.0.0', port=8001, debug=True)