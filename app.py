from flask import Flask, jsonify, request, g
from flask_cors import CORS
from flasgger import Swagger
import os
from datetime import datetime
import secrets
from functools import wraps
from dotenv import load_dotenv
from supabase import create_client, Client

# ============================================
# CONFIGURAÇÕES INICIAIS
# ============================================
load_dotenv()

app = Flask(__name__)

# Configuração do Swagger
swagger_config = {
    "headers": [],
    "specs": [{"endpoint": 'apispec', "route": '/apispec.json', "rule_filter": lambda rule: True, "model_filter": lambda tag: True}],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/docs/",
    "securityDefinitions": {
        "Bearer": {"type": "apiKey", "name": "Authorization", "in": "header", "description": "Insira o token: Bearer SUA_CHAVE"}
    }
}
swagger = Swagger(app, config=swagger_config)

# Configuração de CORS
CORS(app, resources={
    r"/*": {
        "origins": ["http://localhost:3000", "http://localhost:5173", "http://localhost:8080", "http://127.0.0.1:8001"],
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# Inicialização do Supabase
supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)

# ============================================
# FUNÇÕES AUXILIARES
# ============================================

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

        # Busca o usuário no Supabase
        response = supabase.table('user').select('*').eq('api_key', api_key).execute()
        
        if not response.data:
            return jsonify({'error': 'Chave de API inválida'}), 401

        user = response.data[0]

        # Atualiza o último uso
        try:
            supabase.table('user').update({'last_used_at': datetime.utcnow().isoformat()}).eq('id', user['id']).execute()
        except Exception as e:
            print(f"Erro ao atualizar uso: {e}")
        
        g.current_user = user
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

@app.route('/cadastro', methods=['POST'])
def cadastro_usuario():
    data = request.get_json()
    nome = data.get('nome')
    email = data.get('email')
    usualidade = data.get('uso')

    if not nome or not email:
        return jsonify({"mensagem": "Nome e email são obrigatórios!", "status": "error"}), 400
    
    # Verifica email duplicado
    existente = supabase.table('user').select('*').eq('email', email).execute()
    if existente.data:
        return jsonify({"mensagem": "Email já cadastrado!", "status": "error"}), 400
    
    novo_usuario = {
        'name': nome,
        'email': email,
        'usualidade': usualidade,
        'api_key': generate_unique_api_key()
    }
    
    supabase.table('user').insert(novo_usuario).execute()
    return jsonify({"mensagem": "Usuário cadastrado com sucesso!", "status": "sucesso"}), 201

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
    response = supabase.table('tabua_original').select('ano').execute()
    anos = sorted(list(set([item['ano'] for item in response.data if item['ano'] is not None])))
    return jsonify(anos)

@app.route('/dimensoes/anos_projecoes')
def get_anos_projecoes():
    response = supabase.table('tabuas_previsoes').select('ano').execute()
    anos = sorted(list(set([item['ano'] for item in response.data if item['ano'] is not None])))
    return jsonify(anos)

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
def get_original():
    query = supabase.table('tabua_original').select('*', count='exact')

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
    per_page = request.args.get('per_page', 100, type=int)
    start, end = get_pagination_params(page, per_page)
    
    response = query.range(start, end).execute()
    return jsonify(format_paginated_response(response, page, per_page))

@app.route('/previsoes')
@require_api_key
def get_tabua_projecoes():
    query = supabase.table('tabuas_previsoes').select('*', count='exact')

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
    per_page = request.args.get('per_page', 100, type=int)
    start, end = get_pagination_params(page, per_page)
    
    response = query.range(start, end).execute()
    return jsonify(format_paginated_response(response, page, per_page))

@app.route('/metricas')
def get_metricas_erro():
    query = supabase.table('metricas_erro').select('*', count='exact')
    
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 100, type=int)
    start, end = get_pagination_params(page, per_page)
    
    response = query.range(start, end).execute()
    return jsonify(format_paginated_response(response, page, per_page))

@app.route('/sigerip/tabua-mortalidade', methods=['GET'])
def get_tabua_mortalidade_join():
    # Usando inner join do Supabase para filtrar pelas tabelas relacionadas
    query = supabase.table('tabua_original').select(
        '*, dim_locais!inner(*), dim_sexo!inner(*), dim_faixas!inner(*)', 
        count='exact'
    )

    filtro_local = request.args.get('local')
    filtro_sexo = request.args.get('sexo')
    filtro_faixa = request.args.get('faixa')
    filtro_ano = request.args.get('ano', type=int)

    if filtro_local:
        query = query.eq('dim_locais.nome_local', filtro_local)
    if filtro_sexo:
        query = query.eq('dim_sexo.descricao', filtro_sexo)
    if filtro_faixa:
        query = query.eq('dim_faixas.descricao', filtro_faixa)
    if filtro_ano:
        query = query.eq('ano', filtro_ano)

    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 100, type=int)
    start, end = get_pagination_params(page, per_page)
    
    response = query.range(start, end).execute()
    
    # Limpando a resposta para não enviar as tabelas aninhadas inteiras no JSON final
    dados_limpos = []
    for item in response.data:
        # Remove os nós extras criados pelo join
        item.pop('dim_locais', None)
        item.pop('dim_sexo', None)
        item.pop('dim_faixas', None)
        dados_limpos.append(item)

    return jsonify({
        'data': dados_limpos,
        'total': response.count,
        'page': page,
        'per_page': per_page,
        'pages': (response.count + per_page - 1) // per_page if response.count else 0
    })

@app.route('/nacoes_unidas')
def get_nacoes_unidas():
    query = supabase.table('nacoes_unidas').select('*', count='exact')

    ano = request.args.get('ano', type=int)
    sexo = request.args.get('sexo', type=str)
    local = request.args.get('local', type=str)
    faixa_etaria = request.args.get('faixa_etaria', type=int)

    if ano: query = query.eq('ano', ano)
    if sexo: query = query.eq('sexo', sexo)
    if local: query = query.eq('local', local)
    if faixa_etaria: query = query.eq('faixa_etaria', faixa_etaria)
    
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 100, type=int)
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