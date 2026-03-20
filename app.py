from flask import Flask, jsonify, request, g
from flask_cors import CORS
from flasgger import Swagger
import os
from datetime import datetime
import secrets
from functools import wraps
from dotenv import load_dotenv
from supabase import create_client, Client
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from envio import enviar_email_boas_vindas, reenviar_email_token

# ============================================
# CONFIGURAÇÕES INICIAIS
# ============================================
load_dotenv()

app = Flask(__name__)

# Configuração de CORS
CORS(app, resources={
    r"/*": {
        "origins": ["http://localhost:3000", "http://localhost:5173", "http://localhost:8080", "http://127.0.0.1:8001", "https://oiatuarial.vercel.app"],
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# ============================================
# CONFIGURAÇÃO DO SWAGGER
# ============================================
swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": "apispec",
            "route": "/apispec.json",
            "rule_filter": lambda rule: True,
            "model_filter": lambda tag: True,
        }
    ],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/docs"
}

swagger_template = {
    "swagger": "2.0",
    "info": {
        "title": "OiAtuarial API",
        "description": (
            "API de dados atuariais do projeto **OiAtuarial**.\n\n"
            "Fornece acesso a tábuas de mortalidade originais, projeções de mortalidade, "
            "métricas de erro dos modelos preditivos e dados das Nações Unidas.\n\n"
            "### Autenticação\n"
            "Endpoints protegidos exigem uma **API Key** enviada via:\n"
            "- Header `Authorization: Bearer <sua_api_key>`\n"
            "- Query parameter `?api_key=<sua_api_key>`\n\n"
            "### Rate Limiting\n"
            "Limite de **20 requisições por minuto** por usuário."
        ),
        "version": "1.0.0",
        "contact": {
            "name": "Equipe OiAtuarial"
        }
    },
    "basePath": "/",
    "schemes": ["http", "https"],
    "securityDefinitions": {
        "Bearer": {
            "type": "apiKey",
            "name": "Authorization",
            "in": "header",
            "description": "Insira: **Bearer &lt;sua_api_key&gt;**"
        }
    },
    "tags": [
        {
            "name": "Autenticação",
            "description": "Cadastro de usuários e obtenção de API Key"
        },
        {
            "name": "Dados via Parquet",
            "description": "Download de dados completos em formato Parquet"
        },
        {
            "name": "Dimensões",
            "description": "Tabelas de dimensão para filtros (anos, locais, faixas, sexos, modelos)"
        },
        {
            "name": "Dados Principais",
            "description": "Consultas paginadas às tábuas de mortalidade e projeções"
        },
        {
            "name": "Utilidades",
            "description": "Endpoints auxiliares"
        }
    ]
}

swagger = Swagger(app, config=swagger_config, template=swagger_template)

# Inicialização do Supabase
supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)
url_tables = os.environ.get("TABLES")

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

# --- DOWNLOAD DE DADOS COMPLETOS (PARQUET) ---

@app.route('/oiatuarial_api/<tabela>')
@require_api_key
def obter_link_tabela(tabela):
    """
    Obter link de download de uma tabela completa em Parquet
    Retorna a URL direta para download do arquivo .parquet da tabela solicitada.
    Use o link retornado com `pandas.read_parquet()` para carregar os dados.
    ---
    tags:
      - Dados via Parquet
    security:
      - Bearer: []
    parameters:
      - name: tabela
        in: path
        type: string
        required: true
        description: Nome da tabela desejada
        enum:
          - dados_mortalidade1
          - projecoes
          - metricas_erro
          - nacoes_unidas
    responses:
      200:
        description: Link de download gerado com sucesso
        schema:
          type: object
          properties:
            status:
              type: string
              example: sucesso
            tabela:
              type: string
              example: dados_mortalidade1
            url_download:
              type: string
              example: https://exemplo.com/dados_mortalidade1.parquet
            mensagem:
              type: string
              example: "Use este link no pandas.read_parquet() para baixar os dados."
      400:
        description: Tabela inválida
        schema:
          type: object
          properties:
            erro:
              type: string
              example: Tabela inválida
      401:
        description: Chave de API ausente ou inválida
    """

    tabelas_permitidas = ['dados_mortalidade1', 'projecoes', 'metricas_erro', 'nacoes_unidas']
    
    if tabela not in tabelas_permitidas:
        return jsonify({"erro": "Tabela inválida"}), 400

    link_direto = f"{url_tables}{tabela}.parquet"
    print(link_direto)

    # Retorna o link em formato JSON
    return jsonify({
        "status": "sucesso",
        "tabela": tabela,
        "url_download": link_direto,
        "mensagem": "Use este link no pandas.read_parquet() para baixar os dados."
    }), 200

# --- CADASTRO DE USUÁRIO ---

@app.route('/cadastro', methods=['POST'])
def cadastro_usuario():
    """
    Cadastrar novo usuário e obter API Key
    Registra um novo usuário no sistema. Caso o e-mail já exista, reenvia o token existente.
    O token de acesso é enviado para o e-mail informado.
    ---
    tags:
      - Autenticação
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - nome
            - email
          properties:
            nome:
              type: string
              description: Nome completo do usuário
              example: João Silva
            email:
              type: string
              description: E-mail do usuário (será normalizado para minúsculo)
              example: joao@exemplo.com
            uso:
              type: string
              description: Finalidade de uso da API
              example: Pesquisa acadêmica
            descricao:
              type: string
              description: Descrição da instituição ou projeto
              example: Universidade Federal - Departamento de Atuária
    responses:
      201:
        description: Cadastro realizado com sucesso
        schema:
          type: object
          properties:
            mensagem:
              type: string
              example: "Cadastro realizado! Verifique seu e-mail para pegar o token."
      200:
        description: E-mail já cadastrado — token reenviado
        schema:
          type: object
          properties:
            mensagem:
              type: string
              example: "E-mail já cadastrado. Reenviamos o seu token para a caixa de entrada!"
      400:
        description: Dados obrigatórios ausentes
        schema:
          type: object
          properties:
            mensagem:
              type: string
              example: "Nome e email são obrigatórios!"
            status:
              type: string
              example: error
      500:
        description: Erro interno ao enviar e-mail
    """
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

        email_enviado = reenviar_email_token(email, token_antigo,nome.split()[0])

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
    email_enviado = enviar_email_boas_vindas(email, novo_usuario['api_key'], nome.split()[0])

    if email_enviado:
        return jsonify({"mensagem": "Cadastro realizado! Verifique seu e-mail para pegar o token."}), 201

    return jsonify({"erro": "Cadastro feito, mas houve um erro ao enviar o e-mail."}), 500

# --- UTILIDADES ---

@app.route('/')
def list_routes():
    """
    Listar todas as rotas disponíveis
    Retorna uma visão geral de todos os endpoints registrados na API.
    ---
    tags:
      - Utilidades
    responses:
      200:
        description: Lista de rotas em formato texto
        schema:
          type: string
          example: "Endpoint: list_routes | Métodos: [GET] | Caminho: /"
    """
    output = []
    for rule in app.url_map.iter_rules():
        if rule.endpoint != 'static':
            methods = ', '.join(rule.methods - {'OPTIONS', 'HEAD'})
            output.append(f"Endpoint: {rule.endpoint} | Métodos: [{methods}] | Caminho: {rule}")
    return "<br>".join(output)

# --- DIMENSÕES ---

@app.route('/dimensoes/anos_original')
def get_anos_original():
    """
    Listar anos disponíveis (dados originais)
    Retorna a lista distinta e ordenada de anos presentes na tabela de mortalidade original.
    ---
    tags:
      - Dimensões
    responses:
      200:
        description: Lista de anos
        schema:
          type: array
          items:
            type: integer
          example: [1980, 1991, 2000, 2010, 2022]
    """
    response = supabase.table('tabua_original').select('ano').execute()
    anos = sorted(list(set([item['ano'] for item in response.data if item['ano'] is not None])))
    return jsonify(anos)

@app.route('/dimensoes/anos_projecoes')
def get_anos_projecoes():
    """
    Listar anos disponíveis (projeções)
    Retorna a lista distinta e ordenada de anos presentes nas projeções de mortalidade.
    ---
    tags:
      - Dimensões
    responses:
      200:
        description: Lista de anos das projeções
        schema:
          type: array
          items:
            type: integer
          example: [2023, 2024, 2025, 2030, 2040]
    """
    response = supabase.table('tabuas_previsoes').select('ano').execute()
    anos = sorted(list(set([item['ano'] for item in response.data if item['ano'] is not None])))
    return jsonify(anos)

@app.route('/dimensoes/locais')
def get_locais():
    """
    Listar todos os locais (estados/regiões)
    Retorna a tabela completa de locais disponíveis no sistema com seus IDs.
    ---
    tags:
      - Dimensões
    responses:
      200:
        description: Lista de locais
        schema:
          type: array
          items:
            type: object
            properties:
              id:
                type: integer
                example: 1
              nome_local:
                type: string
                example: São Paulo
    """
    response = supabase.table('dim_locais').select('*').execute()
    return jsonify(response.data)

@app.route('/dimensoes/faixas')
def get_faixas():
    """
    Listar faixas etárias
    Retorna todas as faixas etárias cadastradas no sistema com seus IDs.
    ---
    tags:
      - Dimensões
    responses:
      200:
        description: Lista de faixas etárias
        schema:
          type: array
          items:
            type: object
            properties:
              id:
                type: integer
                example: 1
              descricao:
                type: string
                example: "0-4"
    """
    response = supabase.table('dim_faixas').select('*').execute()
    return jsonify(response.data)

@app.route('/dimensoes/sexos')
def get_sexos():
    """
    Listar categorias de sexo
    Retorna as categorias de sexo disponíveis no sistema.
    ---
    tags:
      - Dimensões
    responses:
      200:
        description: Lista de sexos
        schema:
          type: array
          items:
            type: object
            properties:
              id:
                type: integer
                example: 1
              descricao:
                type: string
                example: Masculino
    """
    response = supabase.table('dim_sexo').select('*').execute()
    return jsonify(response.data)

@app.route('/dimensoes/modelos')
def get_modelos():
    """
    Listar modelos de projeção
    Retorna os modelos estatísticos utilizados para as projeções de mortalidade.
    ---
    tags:
      - Dimensões
    responses:
      200:
        description: Lista de modelos
        schema:
          type: array
          items:
            type: object
            properties:
              id:
                type: integer
                example: 1
              descricao:
                type: string
                example: Lee-Carter
    """
    response = supabase.table('dim_modelo').select('*').execute()
    return jsonify(response.data)

# --- DADOS PRINCIPAIS ---

@app.route('/original')
@require_api_key
def get_original():
    """
    Consultar tábua de mortalidade original
    Retorna dados da tábua de mortalidade original com suporte a filtros e paginação.
    ---
    tags:
      - Dados Principais
    security:
      - Bearer: []
    parameters:
      - name: ano
        in: query
        type: integer
        required: false
        description: Filtrar por ano
      - name: sexo
        in: query
        type: integer
        required: false
        description: Filtrar por ID do sexo
      - name: local
        in: query
        type: integer
        required: false
        description: Filtrar por ID do local
      - name: faixa
        in: query
        type: integer
        required: false
        description: Filtrar por ID da faixa etária
      - name: page
        in: query
        type: integer
        required: false
        default: 1
        description: Número da página
      - name: per_page
        in: query
        type: integer
        required: false
        default: 1000
        description: Registros por página
    responses:
      200:
        description: Dados paginados da tábua original
        schema:
          type: object
          properties:
            data:
              type: array
              items:
                type: object
            total:
              type: integer
              description: Total de registros encontrados
            page:
              type: integer
            per_page:
              type: integer
            pages:
              type: integer
              description: Total de páginas
      401:
        description: Chave de API ausente ou inválida
    """
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
    per_page = request.args.get('per_page', 1000, type=int)
    start, end = get_pagination_params(page, per_page)

    response = query.range(start, end).execute()
    return jsonify(format_paginated_response(response, page, per_page))

@app.route('/previsoes')
@require_api_key
def get_tabua_projecoes():
    """
    Consultar projeções de mortalidade
    Retorna dados das projeções de mortalidade geradas pelos modelos estatísticos, com filtros e paginação.
    ---
    tags:
      - Dados Principais
    security:
      - Bearer: []
    parameters:
      - name: ano
        in: query
        type: integer
        required: false
        description: Filtrar por ano da projeção
      - name: sexo
        in: query
        type: integer
        required: false
        description: Filtrar por ID do sexo
      - name: local
        in: query
        type: integer
        required: false
        description: Filtrar por ID do local
      - name: faixa
        in: query
        type: integer
        required: false
        description: Filtrar por ID da faixa etária
      - name: modelo
        in: query
        type: integer
        required: false
        description: Filtrar por ID do modelo de projeção
      - name: page
        in: query
        type: integer
        required: false
        default: 1
        description: Número da página
      - name: per_page
        in: query
        type: integer
        required: false
        default: 1000
        description: Registros por página
    responses:
      200:
        description: Dados paginados das projeções
        schema:
          type: object
          properties:
            data:
              type: array
              items:
                type: object
            total:
              type: integer
            page:
              type: integer
            per_page:
              type: integer
            pages:
              type: integer
      401:
        description: Chave de API ausente ou inválida
    """
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
    per_page = request.args.get('per_page', 1000, type=int)
    start, end = get_pagination_params(page, per_page)

    response = query.range(start, end).execute()
    return jsonify(format_paginated_response(response, page, per_page))

@app.route('/metricas')
@require_api_key
def get_metricas_erro():
    """
    Consultar métricas de erro dos modelos
    Retorna as métricas de avaliação (erro) dos modelos de projeção utilizados, com paginação.
    ---
    tags:
      - Dados Principais
    security:
      - Bearer: []
    parameters:
      - name: page
        in: query
        type: integer
        required: false
        default: 1
        description: Número da página
      - name: per_page
        in: query
        type: integer
        required: false
        default: 1000
        description: Registros por página
    responses:
      200:
        description: Métricas de erro paginadas
        schema:
          type: object
          properties:
            data:
              type: array
              items:
                type: object
            total:
              type: integer
            page:
              type: integer
            per_page:
              type: integer
            pages:
              type: integer
      401:
        description: Chave de API ausente ou inválida
    """
    query = supabase.table('metricas_erro').select('*', count='exact')

    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 1000, type=int)
    start, end = get_pagination_params(page, per_page)

    response = query.range(start, end).execute()
    return jsonify(format_paginated_response(response, page, per_page))


@app.route('/nacoes_unidas')
@require_api_key
def get_nacoes_unidas():
    """
    Consultar dados das Nações Unidas
    Retorna dados de mortalidade provenientes das Nações Unidas, com filtros e paginação.
    ---
    tags:
      - Dados Principais
    security:
      - Bearer: []
    parameters:
      - name: ano
        in: query
        type: integer
        required: false
        description: Filtrar por ano
      - name: sexo
        in: query
        type: string
        required: false
        description: "Filtrar por sexo (ex: Male, Female)"
      - name: local
        in: query
        type: string
        required: false
        description: "Filtrar por local/país"
      - name: faixa_etaria
        in: query
        type: integer
        required: false
        description: Filtrar por faixa etária
      - name: page
        in: query
        type: integer
        required: false
        default: 1
        description: Número da página
      - name: per_page
        in: query
        type: integer
        required: false
        default: 1000
        description: Registros por página
    responses:
      200:
        description: Dados paginados das Nações Unidas
        schema:
          type: object
          properties:
            data:
              type: array
              items:
                type: object
            total:
              type: integer
            page:
              type: integer
            per_page:
              type: integer
            pages:
              type: integer
      401:
        description: Chave de API ausente ou inválida
    """
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
    per_page = request.args.get('per_page', 1000, type=int)
    start, end = get_pagination_params(page, per_page)

    response = query.range(start, end).execute()
    return jsonify(format_paginated_response(response, page, per_page))

# ============================================
# EXECUÇÃO
# ============================================

if __name__ == '__main__':
    print(f"API rodando em: http://0.0.0.0:8001")
    app.run(host='0.0.0.0', port=8001, debug=True)