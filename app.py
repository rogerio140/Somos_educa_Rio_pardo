# ====================
# Bibliotecas Flask
# ====================
from flask import Flask, render_template, request, session, redirect, url_for, flash, send_file, Response, make_response
from flask_session import Session  # pip install Flask-Session
from functools import wraps

# ====================
# Bibliotecas padrão
# ====================
from datetime import datetime, timezone
import os
import logging
import re
import csv
from io import StringIO, BytesIO

# ====================
# Bibliotecas de terceiros
# ====================
import pytz

# ====================
# Conexão com banco de dados
# ====================
import psycopg  # substitui psycopg2

# ====================
# Bibliotecas para relatórios e gráficos
# ====================
import matplotlib
matplotlib.use('Agg')  # Usar backend sem interface gráfica
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch

from reportlab.lib.pagesizes import letter
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors












app = Flask(__name__)
app.config['SESSION_TYPE'] = 'filesystem'  # Armazena sessões no sistema de arquivos
app.config['SESSION_FILE_DIR'] = './flask_session'  # Pasta para armazenar sessões
app.config['SESSION_FILE_THRESHOLD'] = 100  # Número máximo de sessões armazenadas
Session(app)

app.secret_key = os.environ.get('SECRET_KEY', 'dev-key-unsafe-in-production')
app.logger.setLevel(logging.DEBUG)



# Configurar o fuso horário padrão para o Brasil (São Paulo)
os.environ['TZ'] = 'America/Sao_Paulo'
app.config['TIMEZONE'] = 'America/Sao_Paulo'
# Configuração do Banco de Dados




DB_CONFIG = {
    "host": "dpg-d7jn378sfn5c738s2veg-a.virginia-postgres.render.com",
    "port": "5432",
    "dbname": "somos_educa_26_rp",
    "user": "somos_educa_26_rp_user",
    "password": "mPaPHRDIeuGiHxV3sNKnXH3N1BlmF4Ry",
    "sslmode": "require"
}

   

def get_db_connection():
    return psycopg.connect(**DB_CONFIG)

def validate_session(required_keys):
    return all(key in session for key in required_keys)

def handle_database_error(e):
    app.logger.error(f'Database error: {str(e)}')
    return render_template('error.html', error="Erro no banco de dados"), 500

def turma_foi_avaliada(turma_id):
    """Verifica se todos alunos da turma foram avaliados"""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(a.matricula) AS total,
                       COUNT(av.aluno_matricula) AS avaliados
                FROM alunos a
                LEFT JOIN (
                    SELECT aluno_matricula FROM avaliacoes_direitos
                    UNION
                    SELECT aluno_matricula FROM avaliacoes_habilidades
                ) av ON a.matricula = av.aluno_matricula
                WHERE a.turma_id = %s
            """, (turma_id,))
            
            result = cur.fetchone()
            return result and result[0] > 0 and result[0] == result[1]
            
    except Exception as e:
        app.logger.error(f"Erro ao verificar turma: {str(e)}")
        return False
    finally:
        if conn and not conn.closed:
            conn.close()

def init_db_tables():
    """Cria tabelas necessárias para o funcionamento do sistema"""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Tabela de avaliações de direitos (com timestamps de início e fim)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS avaliacoes_direitos (
                    aluno_matricula VARCHAR(20) REFERENCES alunos(matricula),
                    direito_id INTEGER REFERENCES direitos_aprendizagem(id),
                    estagio_numero INTEGER NOT NULL,
                    avaliador_id VARCHAR(20) REFERENCES professores(id_plurall),
                    inicio_avaliacao TIMESTAMP,
                    fim_avaliacao TIMESTAMP,
                    PRIMARY KEY (aluno_matricula, direito_id)
                );

            """)
            
            # Tabela de avaliações de habilidades (com timestamps de início e fim)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS avaliacoes_habilidades (
                    aluno_matricula VARCHAR(20) REFERENCES alunos(matricula),
                    habilidade_id INTEGER REFERENCES habilidades(id),
                    estagio_numero INTEGER NOT NULL CHECK (estagio_numero BETWEEN 1 AND 5),
                    avaliador_id VARCHAR(20) REFERENCES professores(id_plurall),
                    inicio_avaliacao TIMESTAMP,
                    fim_avaliacao TIMESTAMP,
                    PRIMARY KEY (aluno_matricula, habilidade_id)
                )
            """)
            
            conn.commit()
    except Exception as e:
        app.logger.error(f'Erro ao criar tabelas: {str(e)}')
        raise
    finally:
        if conn and not conn.closed:
            conn.close()

# Função para obter o horário atual no fuso do Brasil
def get_brazil_time():
    tz = pytz.timezone('America/Sao_Paulo')
    return datetime.now(timezone.utc).astimezone(tz)
# Garante a criação das tabelas ao iniciar o app
with app.app_context():
    init_db_tables()


# Adicione estas funções auxiliares no seu app.py

def get_escolas_com_progresso():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    e.id,
                    e.nome,
                    COUNT(DISTINCT t.id) as total_turmas,
                    COUNT(DISTINCT al.matricula) as total_alunos,
                    SUM(CASE WHEN EXISTS (
                        SELECT 1 FROM avaliacoes_direitos WHERE aluno_matricula = al.matricula
                        UNION ALL
                        SELECT 1 FROM avaliacoes_habilidades WHERE aluno_matricula = al.matricula
                    ) THEN 1 ELSE 0 END) as alunos_avaliados
                FROM escolas e
                LEFT JOIN turmas t ON e.id = t.escola_id
                LEFT JOIN alunos al ON t.id = al.turma_id
                GROUP BY e.id
                ORDER BY e.nome
            """)
            return cur.fetchall()
    finally:
        conn.close()

def get_turmas_com_progresso(escola_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    t.id,
                    t.nome,
                    a.nome as ano_serie,
                    COUNT(al.id) as total_alunos,
                    SUM(CASE WHEN EXISTS (
                        SELECT 1 FROM avaliacoes_direitos WHERE aluno_matricula = al.matricula
                        UNION ALL
                        SELECT 1 FROM avaliacoes_habilidades WHERE aluno_matricula = al.matricula
                    ) THEN 1 ELSE 0 END) as alunos_avaliados
                FROM turmas t
                JOIN ano_series a ON t.ano_serie_id = a.id
                LEFT JOIN alunos al ON t.id = al.turma_id
                WHERE t.escola_id = %s
                GROUP BY t.id, a.nome
                ORDER BY t.nome
            """, (escola_id,))
            return cur.fetchall()
    finally:
        conn.close()


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('admin'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function



@app.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        id_plurall = request.form.get('id_plurall', '').strip()
        app.logger.debug(f"ID Plurall recebido: {id_plurall}")

        if not id_plurall:
            return render_template('login.html', error="Informe o ID Plurall")

        # Acesso como administrador
        if id_plurall == "Admin123":
            session.clear()
            session['user_id'] = "admin"
            session['user_nome'] = "Administrador"
            session['admin'] = True
            app.logger.debug("Login como administrador bem-sucedido")
            return redirect(url_for('painel_admin'))

        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                # Buscar professor
                cur.execute("""
                    SELECT p.id_plurall, p.nome, p.segmento_id 
                    FROM professores p
                    WHERE p.id_plurall = %s
                """, (id_plurall,))
                professor = cur.fetchone()
                app.logger.debug(f"Resultado da consulta professor: {professor}")

                if not professor:
                    return render_template('login.html', error="ID Plurall não encontrado")

                # Buscar escolas associadas
                cur.execute("""
                    SELECT e.id, e.nome 
                    FROM escolas e
                    JOIN professor_escola pe ON e.id = pe.escola_id
                    WHERE pe.professor_id = %s
                """, (id_plurall,))
                escolas = [{'id': row[0], 'nome': row[1]} for row in cur.fetchall()]
                app.logger.debug(f"Escolas vinculadas: {escolas}")

                if not escolas:
                    return render_template('login.html', error="Professor não vinculado a nenhuma escola")

                # Salvar dados na sessão
                session.clear()
                session['professor_id'] = professor[0]
                session['professor_nome'] = professor[1]
                session['segmento_id'] = professor[2]
                session['escolas'] = escolas
                session['admin'] = False

                # Redirecionar sempre para seleção de escola, mesmo que só tenha uma
                return redirect(url_for('listar_escolas'))

        except Exception as e:
            app.logger.exception("Erro no processo de login:")
            return render_template('login.html', error="Erro durante o login. Verifique os logs.")
        finally:
            if conn:
                conn.close()

    return render_template('login.html')





@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/admin')
@admin_required
def painel_admin():
    segmento_id = request.args.get('segmento_id', type=int)  # Novo parâmetro para filtrar
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Consulta modificada para incluir filtro por segmento
            query = """
                SELECT 
                    e.id,
                    e.nome,
                    COUNT(DISTINCT t.id) as total_turmas,
                    COUNT(DISTINCT al.matricula) as total_alunos,
                    SUM(CASE WHEN EXISTS (
                        SELECT 1 FROM avaliacoes_direitos WHERE aluno_matricula = al.matricula
                        UNION ALL
                        SELECT 1 FROM avaliacoes_habilidades WHERE aluno_matricula = al.matricula
                    ) THEN 1 ELSE 0 END) as alunos_avaliados
                FROM escolas e
                LEFT JOIN turmas t ON e.id = t.escola_id
                LEFT JOIN alunos al ON t.id = al.turma_id
                LEFT JOIN ano_series ans ON t.ano_serie_id = ans.id
                WHERE 1=1
            """
            
            params = []
            
            if segmento_id:
                query += " AND ans.segmento_id = %s"
                params.append(segmento_id)
            
            query += " GROUP BY e.id ORDER BY e.nome"
            
            cur.execute(query, params)
            
            escolas = []
            total_alunos_geral = 0
            alunos_avaliados_geral = 0
            
            for row in cur.fetchall():
                escola = {
                    'id': row[0],
                    'nome': row[1],
                    'total_turmas': row[2] or 0,
                    'total_alunos': row[3] or 0,
                    'alunos_avaliados': row[4] or 0
                }
                escolas.append(escola)
                total_alunos_geral += escola['total_alunos']
                alunos_avaliados_geral += escola['alunos_avaliados']
            
            total_escolas = len(escolas)
            progresso_geral = (alunos_avaliados_geral / total_alunos_geral * 100) if total_alunos_geral > 0 else 0
            
            # Buscar segmentos para o dropdown
            cur.execute("SELECT id, nome FROM segmentos ORDER BY id")
            segmentos = [{'id': row[0], 'nome': row[1]} for row in cur.fetchall()]
            
        return render_template('painel_admin.html',
                            escolas=escolas,
                            total_escolas=total_escolas,
                            total_alunos=total_alunos_geral,
                            alunos_avaliados=alunos_avaliados_geral,
                            progresso_geral=round(progresso_geral, 1),
                            now=get_brazil_time(),
                            segmentos=segmentos,  # Novo parâmetro
                            segmento_selecionado=segmento_id)  # Novo parâmetro
    finally:
        conn.close()






@app.route('/admin/escola/<int:escola_id>')
@admin_required
def detalhes_escola(escola_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Obter nome da escola
            cur.execute("SELECT nome FROM escolas WHERE id = %s", (escola_id,))
            escola_row = cur.fetchone()
            if not escola_row:
                return render_template('error.html', error="Escola não encontrada"), 404
            escola_nome = escola_row[0]
            
            # Obter turmas com progresso (consulta simplificada)
            cur.execute("""
                SELECT 
                    t.id,
                    t.nome,
                    a.nome as ano_serie,
                    COUNT(al.matricula) as total_alunos,
                    SUM(CASE WHEN EXISTS (
                        SELECT 1 FROM avaliacoes_direitos WHERE aluno_matricula = al.matricula
                        UNION ALL
                        SELECT 1 FROM avaliacoes_habilidades WHERE aluno_matricula = al.matricula
                    ) THEN 1 ELSE 0 END) as alunos_avaliados
                FROM turmas t
                JOIN ano_series a ON t.ano_serie_id = a.id
                LEFT JOIN alunos al ON t.id = al.turma_id
                WHERE t.escola_id = %s
                GROUP BY t.id, a.nome, t.nome
                ORDER BY t.nome
            """, (escola_id,))
            
            turmas = []
            total_alunos = 0
            alunos_avaliados = 0
            
            for row in cur.fetchall():
                turma = {
                    'id': row[0],
                    'nome': row[1],
                    'ano_serie': row[2],
                    'total_alunos': row[3] if row[3] is not None else 0,
                    'alunos_avaliados': row[4] if row[4] is not None else 0,
                    'tempo_medio': None  # Inicializa como None
                }
                turmas.append(turma)
                total_alunos += turma['total_alunos']
                alunos_avaliados += turma['alunos_avaliados']
            
            # Calcular tempo médio para cada turma (consulta separada mais simples)
            for turma in turmas:
                try:
                    # DEBUG: Antes da consulta
                    app.logger.debug(f"Calculando tempo para turma ID: {turma['id']}")
                    cur.execute("""
                        SELECT AVG(EXTRACT(EPOCH FROM (fim_avaliacao - inicio_avaliacao)))/60 as tempo_medio
                        FROM (
                            SELECT aluno_matricula, MAX(fim_avaliacao) as fim_avaliacao, MIN(inicio_avaliacao) as inicio_avaliacao
                            FROM avaliacoes_direitos
                            WHERE aluno_matricula IN (SELECT matricula FROM alunos WHERE turma_id = %s)
                            GROUP BY aluno_matricula
                            
                            UNION ALL
                            
                            SELECT aluno_matricula, MAX(fim_avaliacao) as fim_avaliacao, MIN(inicio_avaliacao) as inicio_avaliacao
                            FROM avaliacoes_habilidades
                            WHERE aluno_matricula IN (SELECT matricula FROM alunos WHERE turma_id = %s)
                            GROUP BY aluno_matricula
                        ) as tempos_avaliacao
                    """, (turma['id'], turma['id']))
                    
                    resultado = cur.fetchone()
                    if resultado and resultado[0] is not None:
                        turma['tempo_medio'] = round(float(resultado[0]), 1)
                    # DEBUG: Após a consulta
                    app.logger.debug(f"Resultado: {resultado}")
                except Exception as e:
                    app.logger.error(f"Erro ao calcular tempo médio para turma {turma['id']}: {str(e)}")
                    turma['tempo_medio'] = None
            
            # Calcular média geral do tempo de avaliação
            tempos_validos = [t['tempo_medio'] for t in turmas if t['tempo_medio'] is not None]
            media_geral_tempo = round(sum(tempos_validos) / len(tempos_validos), 1) if tempos_validos else 0.0
            
        return render_template('detalhes_escola.html',
                            escola_id=escola_id,
                            escola_nome=escola_nome,
                            turmas=turmas,
                            total_alunos=total_alunos,
                            alunos_avaliados=alunos_avaliados,
                            media_geral_tempo=media_geral_tempo)
    
    except Exception as e:
        app.logger.error(f"Erro ao carregar detalhes da escola: {str(e)}", exc_info=True)
        return render_template('error.html', error="Erro ao carregar dados da escola"), 500
    finally:
        conn.close()






@app.route('/escolas', methods=['GET', 'POST'])
def listar_escolas():
    if not session.get('professor_id'):
        return redirect(url_for('login'))

    if request.method == 'POST':
        escola_id = request.form.get('escola_id')
        if not escola_id:
            flash('Escola não selecionada', 'error')
            return redirect(url_for('listar_escolas'))
        
        session['escola_id'] = escola_id
        return redirect(url_for('listar_turmas'))

    # Se for admin, mostra todas as escolas
    if session.get('admin'):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id, nome FROM escolas")
                escolas = [{'id': row[0], 'nome': row[1]} for row in cur.fetchall()]
            return render_template('escolas.html', escolas=escolas)
        finally:
            conn.close()

    # Se for professor, mostra apenas suas escolas (armazenadas na sessão)
    if 'escolas' in session:
        return render_template('escolas.html', escolas=session['escolas'])

    return render_template('error.html', error="Acesso não autorizado"), 403




@app.route('/turmas', methods=['GET', 'POST'])
def listar_turmas():
    professor_id = session.get('professor_id') or session.get('user_id')
    if not professor_id:
        return redirect(url_for('login'))

    conn = None
    try:
        conn = get_db_connection()
        
        if request.method == 'POST':
            if not all(key in request.form for key in ['turma_id', 'ano_serie_id']):
                flash('Selecione uma turma válida', 'error')
                return redirect(url_for('listar_turmas'))
            
            session['turma_id'] = request.form['turma_id']
            session['ano_serie_id'] = request.form['ano_serie_id']
            return redirect(url_for('listar_alunos'))

        with conn.cursor() as cur:
            # BUSCAR TODAS AS TURMAS QUE O PROFESSOR TEM ACESSO NA ESCOLA
            # (considerando todos os segmentos que ele tem permissão)
            cur.execute("""
                SELECT 
                    t.id, 
                    t.nome, 
                    a.nome as ano_serie,
                    a.id as ano_serie_id,
                    t.segmento_id,
                    s.nome as segmento_nome,
                    NOT EXISTS (
                        SELECT 1 FROM alunos al
                        WHERE al.turma_id = t.id AND NOT EXISTS (
                            SELECT 1 FROM avaliacoes_direitos 
                            WHERE aluno_matricula = al.matricula
                            UNION ALL
                            SELECT 1 FROM avaliacoes_habilidades 
                            WHERE aluno_matricula = al.matricula
                        )
                    ) as completa
                FROM turmas t
                JOIN ano_series a ON t.ano_serie_id = a.id
                JOIN segmentos s ON t.segmento_id = s.id
                JOIN professores_escolas_segmentos pes ON t.escola_id = pes.escola_id AND t.segmento_id = pes.segmento_id
                WHERE t.escola_id = %s
                AND pes.professor_id = %s
                ORDER BY t.segmento_id, completa, t.nome
            """, (session.get('escola_id'), session.get('professor_id')))
            
            turmas_resultado = cur.fetchall()
            
            if not turmas_resultado:
                flash('Nenhuma turma disponível para você nesta escola', 'error')
                return redirect(url_for('listar_escolas'))
            
            turmas = []
            turmas_completas = 0
            segmentos_presentes = set()
            
            for row in turmas_resultado:
                turma = {
                    'id': row[0],
                    'nome': row[1],
                    'ano_serie': row[2],
                    'ano_serie_id': row[3],
                    'segmento_id': row[4],
                    'segmento_nome': row[5],
                    'completa': row[6]
                }
                
                if turma['completa']:
                    turmas_completas += 1
                
                segmentos_presentes.add(turma['segmento_nome'])
                turmas.append(turma)

            # Determinar o título baseado nos segmentos presentes
            if len(segmentos_presentes) == 1:
                titulo_segmento = f"Segmento: {list(segmentos_presentes)[0]}"
            else:
                titulo_segmento = f"Segmentos: {', '.join(sorted(segmentos_presentes))}"

        return render_template('turmas.html',
                               turmas=turmas,
                               turmas_completas=turmas_completas,
                               titulo_segmento=titulo_segmento,
                               segmentos_presentes=sorted(segmentos_presentes))

    except Exception as e:
        app.logger.error(f"Erro ao listar turmas: {str(e)}", exc_info=True)
        flash('Erro ao carregar as turmas', 'error')
        return render_template('error.html'), 500
        
    finally:
        if conn and not conn.closed:
            conn.close()




@app.route('/alunos', methods=['GET', 'POST'])
def listar_alunos():
    if not validate_session(['professor_id', 'escola_id']) or \
       (request.method == 'POST' and not request.form.get('turma_id')):
        return redirect(url_for('listar_turmas'))

    try:
        if request.method == 'POST':
            session['turma_id'] = request.form['turma_id']
            session['ano_serie_id'] = request.form['ano_serie_id']
            session.modified = True

        conn = get_db_connection()
        with conn.cursor() as cur:
            # Modifique a query para incluir informação de avaliação
            cur.execute("""
                SELECT 
                    a.matricula, 
                    a.nome,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM avaliacoes_direitos 
                        WHERE aluno_matricula = a.matricula
                        UNION ALL
                        SELECT 1 FROM avaliacoes_habilidades 
                        WHERE aluno_matricula = a.matricula
                    ) THEN TRUE ELSE FALSE END AS avaliado
                FROM alunos a
                WHERE a.turma_id = %s
                ORDER BY a.nome
            """, (session['turma_id'],))
            
            alunos = cur.fetchall()
            return render_template('alunos.html', 
                                 alunos=alunos,
                                 total_alunos=len(alunos))

    except Exception as e:
        return handle_database_error(e)
    finally:
        if conn and not conn.closed:
            conn.close()




@app.route('/avaliar/<string:aluno_matricula>', methods=['GET', 'POST'])
def avaliar_aluno(aluno_matricula):
    app.logger.debug(f"\n\n=== INICIANDO AVALIAÇÃO PARA ALUNO: {aluno_matricula} ===")
    
    # 1. Verificação inicial da sessão
    if not validate_session(['professor_id', 'turma_id', 'ano_serie_id']):
        app.logger.warning("Sessão inválida - redirecionando para login")
        return redirect(url_for('login'))

    conn = None
    try:
        # 2. Conexão com o banco de dados
        conn = get_db_connection()
        app.logger.debug("Conexão com banco de dados estabelecida com sucesso")
        
        # 3. Obter informações do aluno e tipo de questionário
        with conn.cursor() as cur:
            app.logger.debug(f"Buscando informações do aluno {aluno_matricula} na turma {session.get('turma_id')}")
            cur.execute("""
                SELECT a.nome, t.segmento_id, t.ano_serie_id 
                FROM alunos a JOIN turmas t ON a.turma_id = t.id
                WHERE a.matricula = %s AND t.id = %s
            """, (aluno_matricula, session['turma_id']))
            aluno_info = cur.fetchone()
            
            if not aluno_info:
                app.logger.error(f"Aluno {aluno_matricula} não encontrado na turma {session['turma_id']}")
                return render_template('error.html', error="Aluno não encontrado"), 404

            aluno_nome, segmento_id, ano_serie_id = aluno_info
            
            # CORREÇÃO: Lógica simplificada para determinar o tipo de questionário
            is_fundamental = (segmento_id == 2)
            
            app.logger.debug(f"Aluno: {aluno_nome} | Segmento: {segmento_id} | Ano/Série: {ano_serie_id}")
            app.logger.debug(f"Tipo de questionário: {'FUNDAMENTAL' if is_fundamental else 'INFANTIL'}")

        # 4. Processamento para método GET
        if request.method == 'GET':
            app.logger.debug("\n--- PROCESSANDO MÉTODO GET ---")
            
            # Verificar se precisa iniciar nova avaliação na sessão
            if 'avaliacao' not in session or session['avaliacao'].get('aluno') != aluno_matricula:
                app.logger.debug("Iniciando nova avaliação na sessão")
                session['avaliacao'] = {
                    'aluno': aluno_matricula,
                    'inicio': datetime.now().isoformat(),
                    'tipo': 'fundamental' if is_fundamental else 'infantil',
                    'grupos': [],
                    'respostas': {},
                    'grupo_atual': 0
                }
                
                with conn.cursor() as cur:
                    if is_fundamental:
                        app.logger.debug("Carregando dados para questionário FUNDAMENTAL")
                        
                        # DEBUG: Verificar se existem disciplinas para este ano/série
                        cur.execute("""
                            SELECT COUNT(*), ano_serie_id 
                            FROM disciplinas 
                            WHERE segmento_id = 2 
                            GROUP BY ano_serie_id
                        """)
                        disciplinas_por_ano = cur.fetchall()
                        app.logger.debug(f"Disciplinas disponíveis por ano: {disciplinas_por_ano}")
                        
                        # Tentar primeiro com o ano/série específico
                        cur.execute("""
                            SELECT d.id, d.nome, h.id as habilidade_id, h.descricao as habilidade_desc
                            FROM disciplinas d
                            JOIN unidades_tematicas ut ON d.id = ut.disciplina_id
                            JOIN habilidades h ON ut.id = h.unidade_tematica_id
                            WHERE d.segmento_id = 2 AND d.ano_serie_id = %s
                            ORDER BY d.nome, h.id
                        """, (ano_serie_id,))
                        
                        disciplinas_data = cur.fetchall()
                        app.logger.debug(f"Dados encontrados para ano_serie_id {ano_serie_id}: {len(disciplinas_data)} registros")
                        
                        # Se não encontrar dados para este ano específico, buscar de qualquer ano do fundamental
                        if not disciplinas_data:
                            app.logger.debug(f"Nenhum dado encontrado para ano_serie_id {ano_serie_id}, buscando qualquer ano do fundamental")
                            cur.execute("""
                                SELECT d.id, d.nome, h.id as habilidade_id, h.descricao as habilidade_desc
                                FROM disciplinas d
                                JOIN unidades_tematicas ut ON d.id = ut.disciplina_id
                                JOIN habilidades h ON ut.id = h.unidade_tematica_id
                                WHERE d.segmento_id = 2
                                ORDER BY d.ano_serie_id, d.nome, h.id
                            """)
                            disciplinas_data = cur.fetchall()
                            app.logger.debug(f"Dados encontrados (qualquer ano): {len(disciplinas_data)} registros")
                        
                        if not disciplinas_data:
                            app.logger.error("Nenhuma disciplina/habilidade encontrada para o Ensino Fundamental")
                            return render_template('error.html', error="Dados de avaliação não configurados para o Ensino Fundamental"), 500
                        
                        disciplinas = {}
                        for disciplina_id, disciplina_nome, habilidade_id, habilidade_desc in disciplinas_data:
                            if disciplina_id not in disciplinas:
                                disciplinas[disciplina_id] = {
                                    'id': disciplina_id,
                                    'nome': disciplina_nome,
                                    'questoes': []
                                }
                            
                            # Buscar estágios para cada habilidade
                            cur.execute("""
                                SELECT numero_estagio, descricao 
                                FROM estagios_habilidades
                                WHERE habilidade_id = %s
                                ORDER BY numero_estagio
                            """, (habilidade_id,))
                            
                            estagios_data = cur.fetchall()
                            estagios = [{'numero': e[0], 'descricao': e[1]} for e in estagios_data]
                            
                            disciplinas[disciplina_id]['questoes'].append({
                                'id': habilidade_id,
                                'id_str': str(habilidade_id),
                                'descricao': habilidade_desc,
                                'estagios': estagios
                            })
                        
                        session['avaliacao']['grupos'] = list(disciplinas.values())
                        app.logger.debug(f"Carregadas {len(disciplinas)} disciplinas com {sum(len(d['questoes']) for d in disciplinas.values())} habilidades")
                    
                    else:
                        app.logger.debug("Carregando dados para questionário INFANTIL")
                        try:
                            # Buscar faixa etária da turma
                            cur.execute("""
                                SELECT sfe.faixa_etaria_id 
                                FROM serie_faixa_etaria sfe
                                JOIN turmas t ON sfe.serie_id = t.ano_serie_id
                                WHERE t.id = %s
                            """, (session['turma_id'],))
                            faixa_result = cur.fetchone()
                            
                            if not faixa_result:
                                app.logger.error("Faixa etária não encontrada para a turma")
                                return render_template('error.html', error="Configuração de faixa etária não encontrada"), 500
                                
                            faixa_etaria = faixa_result[0]
                            app.logger.debug(f"Faixa etária encontrada: {faixa_etaria}")
                            
                            # Buscar campos de experiência e direitos
                            cur.execute("""
                                SELECT c.id, c.nome, d.id as direito_id, d.descricao as direito_desc
                                FROM campos_experiencia c
                                JOIN direitos_aprendizagem d ON c.id = d.campo_experiencia_id
                                WHERE d.faixa_etaria_id = %s
                                ORDER BY c.nome, d.id
                            """, (faixa_etaria,))
                            campos_data = cur.fetchall()
                            
                            if not campos_data:
                                app.logger.error("Nenhum campo de experiência encontrado para a faixa etária")
                                return render_template('error.html', error="Dados de avaliação não configurados"), 500
                            
                            app.logger.debug(f"Encontrados {len(campos_data)} direitos de aprendizagem")
                            
                            campos = {}
                            for campo_id, campo_nome, direito_id, direito_desc in campos_data:
                                if campo_id not in campos:
                                    campos[campo_id] = {
                                        'id': campo_id,
                                        'nome': campo_nome,
                                        'questoes': []
                                    }
                                
                                # Buscar estágios para cada direito
                                cur.execute("""
                                    SELECT numero_estagio, descricao 
                                    FROM estagios
                                    WHERE direito_id = %s
                                    ORDER BY numero_estagio
                                """, (direito_id,))
                                
                                estagios_data = cur.fetchall()
                                if not estagios_data:
                                    app.logger.warning(f"Nenhum estágio encontrado para direito {direito_id}")
                                    continue
                                
                                estagios = [{'numero': e[0], 'descricao': e[1]} for e in estagios_data]
                                
                                campos[campo_id]['questoes'].append({
                                    'id': direito_id,
                                    'id_str': str(direito_id),
                                    'descricao': direito_desc,
                                    'estagios': estagios
                                })
                            
                            session['avaliacao']['grupos'] = list(campos.values())
                            app.logger.debug(f"Carregados {len(campos)} campos de experiência com {sum(len(c['questoes']) for c in campos.values())} direitos")
                            
                        except Exception as e:
                            app.logger.error(f"Erro ao buscar dados infantis: {str(e)}", exc_info=True)
                            return render_template('error.html', error="Erro ao carregar formulário"), 500
                    
                    session.modified = True
                    app.logger.debug("Sessão atualizada com dados da avaliação")

            # Preparar grupo atual para exibição
            grupo_atual = session['avaliacao']['grupo_atual']
            total_grupos = len(session['avaliacao']['grupos'])
            
            if total_grupos == 0:
                app.logger.error("Nenhum grupo de questões carregado")
                return render_template('error.html', error="Nenhuma questão disponível para avaliação"), 400
                
            if grupo_atual >= total_grupos:
                app.logger.error(f"Índice de grupo inválido: {grupo_atual} (total: {total_grupos})")
                return render_template('error.html', error="Erro na navegação do questionário"), 400
                
            grupo = session['avaliacao']['grupos'][grupo_atual]
            app.logger.debug(f"\nPreparando grupo {grupo_atual + 1}/{total_grupos}: {grupo.get('nome', 'Sem nome')}")
            app.logger.debug(f"Questões no grupo: {len(grupo['questoes'])}")
            
            # Verificar respostas já existentes para pré-seleção
            for questao in grupo['questoes']:
                questao['resposta'] = session['avaliacao']['respostas'].get(questao['id_str'])
                app.logger.debug(f"  Questão {questao['id']} - Resposta atual: {questao.get('resposta')}")
            
            app.logger.debug("Renderizando template questionario_grupo_completo.html")
            return render_template('questionario_grupo_completo.html',
                                aluno_nome=aluno_nome,
                                aluno_matricula=aluno_matricula,
                                grupo=grupo,
                                grupo_index=grupo_atual,
                                total_grupos=total_grupos)

        # 5. Processamento para método POST
        if request.method == 'POST':
            app.logger.debug("\n--- PROCESSANDO MÉTODO POST ---")
            
            if 'avaliacao' not in session or session['avaliacao']['aluno'] != aluno_matricula:
                app.logger.error("Sessão de avaliação inválida ou não encontrada")
                return render_template('error.html', error="Sessão inválida"), 400

            action = request.form.get('action')
            grupo_atual = session['avaliacao']['grupo_atual']
            app.logger.debug(f"Ação recebida: {action} | Grupo atual: {grupo_atual}")
            
            # Processar respostas do grupo atual
            respostas_processadas = 0
            for key, value in request.form.items():
                if key.startswith('questao_'):
                    questao_id = key.split('_')[1]
                    try:
                        session['avaliacao']['respostas'][questao_id] = int(value)
                        respostas_processadas += 1
                        app.logger.debug(f"Resposta registrada para questão {questao_id}: {value}")
                    except (ValueError, TypeError) as e:
                        app.logger.error(f"Valor inválido para questão {questao_id}: {value} - {str(e)}")
                        continue
            
            app.logger.debug(f"Total de respostas processadas neste grupo: {respostas_processadas}")
            session.modified = True

            # Navegação entre grupos
            if action == 'anterior' and grupo_atual > 0:
                app.logger.debug("Navegação: Voltando para grupo anterior")
                session['avaliacao']['grupo_atual'] -= 1
                return redirect(url_for('avaliar_aluno', aluno_matricula=aluno_matricula))
                
            elif action == 'proximo' and grupo_atual < len(session['avaliacao']['grupos']) - 1:
                app.logger.debug("Navegação: Avançando para próximo grupo")
                session['avaliacao']['grupo_atual'] += 1
                return redirect(url_for('avaliar_aluno', aluno_matricula=aluno_matricula))
                
            elif action == 'finalizar':
                app.logger.debug("\n--- FINALIZANDO AVALIAÇÃO ---")
                # Salvar todas as respostas no banco
                inicio = datetime.fromisoformat(session['avaliacao']['inicio'])
                fim = datetime.now()
                avaliador_id = session['professor_id']
                tipo_avaliacao = session['avaliacao']['tipo']
                
                app.logger.debug(f"Tipo: {tipo_avaliacao} | Início: {inicio} | Fim: {fim}")
                app.logger.debug(f"Total de respostas a salvar: {len(session['avaliacao']['respostas'])}")
                
                try:
                    with conn.cursor() as cur:
                        total_gravadas = 0
                        for q_id, estagio in session['avaliacao']['respostas'].items():
                            try:
                                if tipo_avaliacao == 'fundamental':
                                    cur.execute("""
                                        INSERT INTO avaliacoes_habilidades
                                        (aluno_matricula, habilidade_id, estagio_numero, avaliador_id, inicio_avaliacao, fim_avaliacao)
                                        VALUES (%s, %s, %s, %s, %s, %s)
                                        ON CONFLICT (aluno_matricula, habilidade_id) 
                                        DO UPDATE SET
                                            estagio_numero = EXCLUDED.estagio_numero,
                                            avaliador_id = EXCLUDED.avaliador_id,
                                            inicio_avaliacao = EXCLUDED.inicio_avaliacao,
                                            fim_avaliacao = EXCLUDED.fim_avaliacao
                                    """, (aluno_matricula, q_id, estagio, avaliador_id, inicio, fim))
                                else:
                                    cur.execute("""
                                        INSERT INTO avaliacoes_direitos
                                        (aluno_matricula, direito_id, estagio_numero, avaliador_id, inicio_avaliacao, fim_avaliacao)
                                        VALUES (%s, %s, %s, %s, %s, %s)
                                        ON CONFLICT (aluno_matricula, direito_id) 
                                        DO UPDATE SET
                                            estagio_numero = EXCLUDED.estagio_numero,
                                            avaliador_id = EXCLUDED.avaliador_id,
                                            inicio_avaliacao = EXCLUDED.inicio_avaliacao,
                                            fim_avaliacao = EXCLUDED.fim_avaliacao
                                    """, (aluno_matricula, q_id, estagio, avaliador_id, inicio, fim))
                                
                                total_gravadas += 1
                            except Exception as e:
                                app.logger.error(f"Erro ao salvar resposta {q_id}: {str(e)}")
                                continue
                        
                        conn.commit()
                        app.logger.debug(f"Avaliação finalizada com sucesso - {total_gravadas} respostas salvas")
                        session.pop('avaliacao', None)
                        return redirect(url_for('confirmacao'))
                    
                except Exception as e:
                    conn.rollback()
                    app.logger.error(f"Erro ao salvar avaliação: {str(e)}", exc_info=True)
                    return render_template('error.html', error="Erro ao finalizar avaliação"), 500

            app.logger.debug("Redirecionando para mesma página (ação não reconhecida ou navegação concluída)")
            return redirect(url_for('avaliar_aluno', aluno_matricula=aluno_matricula))

    except Exception as e:
        app.logger.error(f"\n!!! ERRO NA AVALIAÇÃO: {str(e)}", exc_info=True)
        return render_template('error.html', error="Erro no servidor"), 500
    finally:
        if conn:
            conn.close()
            app.logger.debug("Conexão com banco de dados fechada")
        app.logger.debug("=== FIM DO PROCESSAMENTO ===")


@app.route('/confirmacao')
def confirmacao():
    return render_template('confirmacao.html')





@app.route('/admin/infantil')
@admin_required
def painel_infantil():
    try:
        escola_id = request.args.get('escola_id', type=int)
        turma_id = request.args.get('turma_id', type=int)
        aluno_matricula = request.args.get('aluno_matricula', type=str)

        conn = get_db_connection()
        cur = conn.cursor()

        # Filtro para turmas do infantil
        filtro_infantil = """
            t.segmento_id = 1 OR
            EXISTS (
                SELECT 1 FROM serie_faixa_etaria sfe
                WHERE sfe.serie_id = t.ano_serie_id AND sfe.faixa_etaria_id = 1
            )
        """

        # 1. Buscar todas as escolas com turmas infantis
        cur.execute(f"""
            SELECT DISTINCT e.id, e.nome
            FROM escolas e
            JOIN turmas t ON e.id = t.escola_id
            WHERE {filtro_infantil}
            ORDER BY e.nome
        """)
        escolas = [{'id': row[0], 'nome': row[1]} for row in cur.fetchall()]

        # 2. Buscar turmas da escola selecionada
        turmas = []
        if escola_id:
            cur.execute(f"""
                SELECT t.id, t.nome, ans.nome
                FROM turmas t
                JOIN ano_series ans ON t.ano_serie_id = ans.id
                WHERE t.escola_id = %s AND ({filtro_infantil})
                ORDER BY t.nome
            """, (escola_id,))
            turmas = [{'id': row[0], 'nome': row[1], 'ano_serie': row[2]} for row in cur.fetchall()]

        # 3. Buscar anos/séries disponíveis para o infantil
        cur.execute(f"""
            SELECT DISTINCT ans.id, ans.nome
            FROM ano_series ans
            JOIN turmas t ON ans.id = t.ano_serie_id
            WHERE {filtro_infantil}
            ORDER BY ans.nome
        """)
        anos_series = cur.fetchall()

        # 4. Dados de Resumo (Dinâmicos por seleção)
        resumo = {
            'tipo': 'Geral',
            'nome': 'Rede de Ensino Infantil',
            'total_escolas': len(escolas),
            'total_turmas': 0,
            'total_alunos': 0,
            'alunos_avaliados': 0,
            'progresso': 0,
            'media_geral': 0
        }

        dados_grafico = []
        max_escala = 5  # Escala padrão
        
        if aluno_matricula:
            # RESUMO DO ALUNO
            cur.execute("""
                SELECT a.nome, t.nome, e.nome, ans.nome
                FROM alunos a
                JOIN turmas t ON a.turma_id = t.id
                JOIN escolas e ON t.escola_id = e.id
                JOIN ano_series ans ON t.ano_serie_id = ans.id
                WHERE a.matricula = %s
            """, (aluno_matricula,))
            aluno_info = cur.fetchone()
            
            cur.execute("SELECT COUNT(*) FROM avaliacoes_direitos WHERE aluno_matricula = %s", (aluno_matricula,))
            total_avaliacoes = cur.fetchone()[0]
            
            cur.execute("SELECT AVG(estagio_numero) FROM avaliacoes_direitos WHERE aluno_matricula = %s", (aluno_matricula,))
            media_aluno = cur.fetchone()[0] or 0
            
            resumo.update({
                'tipo': 'Aluno',
                'nome': aluno_info[0],
                'matricula': aluno_matricula,
                'turma': aluno_info[1],
                'escola': aluno_info[2],
                'serie': aluno_info[3],
                'total_avaliacoes': total_avaliacoes,
                'media_geral': float(media_aluno)
            })
            
        elif turma_id:
            # RESUMO DA TURMA
            cur.execute("""
                SELECT t.nome, e.nome, ans.nome
                FROM turmas t
                JOIN escolas e ON t.escola_id = e.id
                JOIN ano_series ans ON t.ano_serie_id = ans.id
                WHERE t.id = %s
            """, (turma_id,))
            turma_info = cur.fetchone()
            
            cur.execute("SELECT COUNT(*) FROM alunos WHERE turma_id = %s", (turma_id,))
            total_alunos = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(DISTINCT aluno_matricula) 
                FROM avaliacoes_direitos ad
                JOIN alunos a ON ad.aluno_matricula = a.matricula
                WHERE a.turma_id = %s
            """, (turma_id,))
            alunos_avaliados = cur.fetchone()[0]
            
            cur.execute("""
                SELECT AVG(estagio_numero) 
                FROM avaliacoes_direitos ad
                JOIN alunos a ON ad.aluno_matricula = a.matricula
                WHERE a.turma_id = %s
            """, (turma_id,))
            media_turma = cur.fetchone()[0] or 0
            
            resumo.update({
                'tipo': 'Turma',
                'nome': turma_info[0],
                'escola': turma_info[1],
                'serie': turma_info[2],
                'total_alunos': total_alunos,
                'alunos_avaliados': alunos_avaliados,
                'progresso': round((alunos_avaliados / total_alunos * 100) if total_alunos > 0 else 0),
                'media_geral': float(media_turma)
            })
            
        elif escola_id:
            # RESUMO DA ESCOLA
            cur.execute("SELECT nome FROM escolas WHERE id = %s", (escola_id,))
            escola_nome = cur.fetchone()[0]
            
            cur.execute(f"SELECT COUNT(*) FROM turmas t WHERE t.escola_id = %s AND ({filtro_infantil})", (escola_id,))
            total_turmas = cur.fetchone()[0]

            cur.execute(f"""
                SELECT COUNT(DISTINCT a.matricula)
                FROM alunos a
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.escola_id = %s AND ({filtro_infantil})
            """, (escola_id,))
            total_alunos = cur.fetchone()[0]

            cur.execute(f"""
                SELECT COUNT(DISTINCT a.matricula)
                FROM alunos a
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.escola_id = %s AND ({filtro_infantil})
                AND EXISTS (SELECT 1 FROM avaliacoes_direitos WHERE aluno_matricula = a.matricula)
            """, (escola_id,))
            alunos_avaliados = cur.fetchone()[0]
            
            cur.execute(f"""
                SELECT AVG(ad.estagio_numero)
                FROM avaliacoes_direitos ad
                JOIN alunos a ON ad.aluno_matricula = a.matricula
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.escola_id = %s
            """, (escola_id,))
            media_escola = cur.fetchone()[0] or 0

            resumo.update({
                'tipo': 'Escola',
                'nome': escola_nome,
                'total_turmas': total_turmas,
                'total_alunos': total_alunos,
                'alunos_avaliados': alunos_avaliados,
                'progresso': round((alunos_avaliados / total_alunos * 100) if total_alunos > 0 else 0),
                'media_geral': float(media_escola)
            })

        # 5. Dados para o Gráfico (Escola, Turma ou Aluno)
        if aluno_matricula:
            cur.execute("""
                SELECT ce.nome, AVG(ad.estagio_numero), COUNT(*)
                FROM avaliacoes_direitos ad
                JOIN direitos_aprendizagem d ON ad.direito_id = d.id
                JOIN campos_experiencia ce ON d.campo_experiencia_id = ce.id
                WHERE ad.aluno_matricula = %s
                GROUP BY ce.nome ORDER BY ce.nome
            """, (aluno_matricula,))
        elif turma_id:
            cur.execute("""
                SELECT ce.nome, AVG(ad.estagio_numero), COUNT(*)
                FROM avaliacoes_direitos ad
                JOIN direitos_aprendizagem d ON ad.direito_id = d.id
                JOIN campos_experiencia ce ON d.campo_experiencia_id = ce.id
                JOIN alunos a ON ad.aluno_matricula = a.matricula
                WHERE a.turma_id = %s
                GROUP BY ce.nome ORDER BY ce.nome
            """, (turma_id,))
        elif escola_id:
            cur.execute("""
                SELECT ce.nome, AVG(ad.estagio_numero), COUNT(*)
                FROM avaliacoes_direitos ad
                JOIN direitos_aprendizagem d ON ad.direito_id = d.id
                JOIN campos_experiencia ce ON d.campo_experiencia_id = ce.id
                JOIN alunos a ON ad.aluno_matricula = a.matricula
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.escola_id = %s
                GROUP BY ce.nome ORDER BY ce.nome
            """, (escola_id,))
        
        if escola_id or turma_id or aluno_matricula:
            for row in cur.fetchall():
                campo_nome = row[0]
                media_valor = float(row[1])
                dados_grafico.append({'campo': campo_nome, 'media': media_valor, 'total': row[2]})
                # Se encontrar "Primeira Escrita", a escala sobe para 8
                if "Primeira Escrita" in campo_nome:
                    max_escala = 8

        # 6. Alunos da turma selecionada
        alunos = []
        if turma_id:
            cur.execute("SELECT matricula, nome FROM alunos WHERE turma_id = %s ORDER BY nome", (turma_id,))
            alunos = [{'matricula': row[0], 'nome': row[1]} for row in cur.fetchall()]

        # 7. Avaliações do aluno selecionado
        avaliacoes = []
        if aluno_matricula:
            cur.execute("""
                SELECT 
                    ce.nome, d.descricao, ad.estagio_numero, 
                    p.nome, ad.inicio_avaliacao, ad.fim_avaliacao
                FROM avaliacoes_direitos ad
                JOIN direitos_aprendizagem d ON ad.direito_id = d.id
                JOIN campos_experiencia ce ON d.campo_experiencia_id = ce.id
                LEFT JOIN professores p ON ad.avaliador_id = p.id_plurall
                WHERE ad.aluno_matricula = %s
                ORDER BY ce.nome, d.descricao
            """, (aluno_matricula,))
            
            for row in cur.fetchall():
                duracao = (row[5] - row[4]).total_seconds() / 60 if row[4] and row[5] else 0
                avaliacoes.append({
                    'campo_experiencia': row[0],
                    'direito': row[1],
                    'estagio': row[2],
                    'avaliador': row[3] or 'Não informado',
                    'data_avaliacao': row[5],
                    'duracao': round(duracao, 1)
                })

        return render_template('painel_infantil.html',
                           escolas=escolas,
                           turmas=turmas,
                           alunos=alunos,
                           avaliacoes=avaliacoes,
                           dados_grafico=dados_grafico,
                           resumo=resumo,
                           max_escala=max_escala,
                           anos_series=anos_series,
                           escola_selecionada=escola_id,
                           turma_selecionada=turma_id,
                           aluno_selecionado=aluno_matricula,
                           now=get_brazil_time())

    except Exception as e:
        app.logger.error(f"Erro no painel infantil: {str(e)}", exc_info=True)
        return render_template('error.html', error="Erro ao carregar dados do ensino infantil"), 500
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()




@app.route('/gerar-pdf-infantil/<string:aluno_matricula>')
@admin_required
def gerar_pdf_infantil(aluno_matricula):
    try:
        # Configurar Matplotlib
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import numpy as np
        from matplotlib.patches import Patch
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib import colors
        from io import BytesIO
        from datetime import datetime
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Buscar dados básicos do aluno
        cur.execute("""
            SELECT a.nome, a.matricula, t.nome as turma, e.nome as escola
            FROM alunos a
            JOIN turmas t ON a.turma_id = t.id
            JOIN escolas e ON t.escola_id = e.id
            WHERE a.matricula = %s
        """, (aluno_matricula,))
        aluno_info = cur.fetchone()
        
        if not aluno_info:
            return render_template('error.html', error="Aluno não encontrado"), 404
        
        # Buscar dados para o gráfico radar
        cur.execute("""
            SELECT 
                ce.nome as campo_experiencia,
                AVG(ad.estagio_numero) as media_estagio,
                COUNT(ad.estagio_numero) as total_avaliacoes
            FROM avaliacoes_direitos ad
            JOIN direitos_aprendizagem d ON ad.direito_id = d.id
            JOIN campos_experiencia ce ON d.campo_experiencia_id = ce.id
            WHERE ad.aluno_matricula = %s
            GROUP BY ce.nome
            ORDER BY ce.nome
        """, (aluno_matricula,))
        
        dados_grafico = []
        campos = []
        medias = []
        primeira_escrita = False
        for row in cur.fetchall():
            is_primeira_escrita = row[0] == 'Primeira Escrita'
            if is_primeira_escrita:
                primeira_escrita = True
            dados_grafico.append({
                'campo': row[0],
                'media': float(row[1]),
                'total': row[2],
                'is_primeira_escrita': is_primeira_escrita
            })
            campos.append(row[0])
            medias.append(float(row[1]))

        # Buscar dados detalhados com verbos
        cur.execute("""
            SELECT 
                ce.nome as campo_experiencia,
                v.nome as verbo,
                d.descricao as direito_descricao,
                ad.estagio_numero,
                ad.inicio_avaliacao,
                ad.avaliador_id
            FROM avaliacoes_direitos ad
            JOIN direitos_aprendizagem d ON ad.direito_id = d.id
            JOIN campos_experiencia ce ON d.campo_experiencia_id = ce.id
            JOIN verbos v ON d.verbo_id = v.id
            WHERE ad.aluno_matricula = %s
            ORDER BY ce.nome, ad.estagio_numero
        """, (aluno_matricula,))
        
        dados_detalhados = cur.fetchall()
        
        # Organizar dados por campo de experiência
        dados_por_campo = {}
        for row in dados_detalhados:
            campo = row[0]
            is_primeira_escrita = campo == 'Primeira Escrita'
            
            if campo not in dados_por_campo:
                dados_por_campo[campo] = {
                    'dados': [],
                    'is_primeira_escrita': is_primeira_escrita
                }
            
            # Determinar descrição do estágio
            if is_primeira_escrita:
                descricao_estagio = {
                    1: 'Garatuja',
                    2: 'Pré-silábico I',
                    3: 'Pré-silábico II',
                    4: 'Silábico',
                    5: 'Silábico-alfabético',
                    6: 'Alfabético',
                    7: 'Alfabético consolidado',
                    8: 'Ortográfico'
                }.get(row[3], 'Não avaliado')
            else:
                descricao_estagio = {
                    1: 'Inicial',
                    2: 'Intermediário',
                    3: 'Avançado',
                    4: 'Consolidado',
                    5: 'Excelente'
                }.get(row[3], 'Não avaliado')
            
            dados_por_campo[campo]['dados'].append({
                'verbo': row[1],
                'direito_descricao': row[2],
                'estagio_numero': row[3],
                'descricao_estagio': descricao_estagio,
                'inicio': row[4].strftime('%d/%m/%Y') if row[4] else '-',
                'avaliador': f"Avaliador ID: {row[5]}" if row[5] else 'Não informado'
            })

        # Criar PDF
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter,
                              rightMargin=40, leftMargin=40,
                              topMargin=40, bottomMargin=40)
        styles = getSampleStyleSheet()
        story = []
        
        # Configurar fontes Unicode
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        try:
            pdfmetrics.registerFont(TTFont('DejaVuSans', 'DejaVuSans.ttf'))
            pdfmetrics.registerFont(TTFont('DejaVuSans-Bold', 'DejaVuSans-Bold.ttf'))
            font_name = 'DejaVuSans'
        except:
            font_name = 'Helvetica'
        
        # Estilos
        estilo_titulo = styles['Title']
        estilo_titulo.alignment = 1
        estilo_titulo.fontName = font_name
        estilo_normal = styles['Normal']
        estilo_normal.fontName = font_name
        estilo_normal.fontSize = 10
        estilo_destaque = styles['Normal']
        estilo_destaque.fontName = font_name + '-Bold' if font_name != 'Helvetica' else 'Helvetica-Bold'
        estilo_destaque.textColor = colors.HexColor("#000000")
        
        # CABEÇALHO COM LOGOS
        logo_somos = 'static/logo_somos.png'  # Ajuste o caminho conforme necessário
        logo_pref = 'static/prefeitura_sj.png'  # Ajuste o caminho conforme necessário
        
        # Tabela com 3 colunas para o cabeçalho (logo esquerda, título, logo direita)
        cabecalho_tabela = Table([
            [Image(logo_somos, width=60, height=30), 
             Paragraph("<b>Acompanhamento do Desenvolvimento da Aprendizagem - ENSINO INFANTIL</b>", estilo_titulo), 
             Image(logo_pref, width=60, height=30)]
        ], colWidths=[80, 360, 80])
        
        # Estilo da tabela do cabeçalho
        cabecalho_tabela.setStyle(TableStyle([
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('ALIGN', (1, 0), (1, 0), 'CENTER'),
            ('ALIGN', (2, 0), (2, 0), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ]))
        
        story.append(cabecalho_tabela)
        story.append(Spacer(1, 15))

        # Função para garantir encoding correto
        def safe_paragraph(text, style):
            if isinstance(text, str):
                try:
                    text = text.decode('utf-8')
                except (UnicodeDecodeError, AttributeError):
                    pass
            return Paragraph(text, style)
        
        #story.append(safe_paragraph("<b>Acompanhamento do Desenvolvimento da Aprendizagem - ENSINO INFANTIL</b>", estilo_titulo))
        story.append(Spacer(1, 15))
        
        # Informações do aluno
        info_aluno = [
            f"<b>Escola:</b> {aluno_info[3]}",
            f"<b>Turma:</b> {aluno_info[2]}",
            f"<b>Aluno:</b> {aluno_info[0]}",
            f"<b>Matrícula:</b> {aluno_info[1]}"
        ]
        for info in info_aluno:
            story.append(safe_paragraph(info, estilo_normal))
            story.append(Spacer(1, 5))
        story.append(Spacer(1, 20))

        # Gráfico Radar
        if dados_grafico:
            fig = plt.figure(figsize=(8, 8))
            ax = fig.add_subplot(111, polar=True)
            cor_principal = '#36a2eb'
            cor_fundo = 'white'
            cor_grid = (0, 0, 0, 0.1)
            N = len(campos)
            angles = [n / float(N) * 2 * np.pi for n in range(N)]
            angles += angles[:1]
            valores = medias + [medias[0]]
            ax.plot(angles, valores, linewidth=2, color=cor_principal)
            ax.fill(angles, valores, alpha=0.25, color=cor_principal)
            ax.set_theta_offset(np.pi / 2)
            ax.set_theta_direction(-1)
            ax.set_thetagrids(np.degrees(angles[:-1]), labels=campos)
            for label, angle in zip(ax.get_xticklabels(), angles[:-1]):
                label.set_horizontalalignment('center')
                angle_deg = angle * 180/np.pi
                if angle_deg > 90:
                    angle_deg -= 180
                label.set_rotation(angle_deg)
                label.set_rotation_mode('anchor')
            
            # Ajustar escala para Primeira Escrita
            if primeira_escrita:
                ax.set_ylim(0, 8)
                ax.set_yticks(range(0, 9))
                ax.set_yticklabels([str(i) for i in range(0, 9)])
            else:
                ax.set_ylim(0, 5)
                ax.set_yticks(range(0, 6))
                ax.set_yticklabels([str(i) for i in range(0, 6)])
            
            ax.grid(color=cor_grid, linestyle='-', linewidth=0.5)
            ax.set_facecolor(cor_fundo)
            ax.spines['polar'].set_visible(False)
            img_buffer = BytesIO()
            plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', facecolor=cor_fundo)
            plt.close(fig)
            
            story.append(safe_paragraph("<b>Evolução da Aprendizagem por Campo de Experiência</b>", estilo_titulo))
            story.append(Spacer(1, 10))
            img_buffer.seek(0)
            img = Image(img_buffer)
            img.drawHeight = 300
            img.drawWidth = img.drawHeight * (1.0 * img.imageWidth / img.imageHeight)
            story.append(img)
            story.append(Spacer(1, 50))
            
        # Gráficos de barras e Tabelas detalhadas
        if dados_por_campo:
            story.append(PageBreak())
            story.append(safe_paragraph("<b>Desenvolvimento da Aprendizagem dos Campos de Experiência</b>", estilo_titulo))
            story.append(Spacer(1, 10))
            
            for campo, dados_campo in dados_por_campo.items():
                direitos = dados_campo['dados']
                is_primeira_escrita = dados_campo['is_primeira_escrita']
                
                if not direitos:
                    continue
                
                # Identificar menor estágio para destaque
                todos_estagios = [d['estagio_numero'] for d in direitos]
                destacar_menor = len(set(todos_estagios)) > 1
                menor_estagio = min(todos_estagios) if destacar_menor else None

                # Identificar TODOS os direitos com menor estágio
                direitos_destaque = []
                if destacar_menor:
                    direitos_destaque = [d for d in direitos if d['estagio_numero'] == menor_estagio]

                # Preparar dados para gráfico de barras
                verbos = [d['verbo'] for d in direitos]
                estagios = [d['estagio_numero'] for d in direitos]
                cores = ['#ff6b6b' if d in direitos_destaque else '#36a2eb' for d in direitos]

                # Criar gráfico de barras
                fig, ax = plt.subplots(figsize=(10, 6))
                bars = ax.bar(verbos, estagios, color=cores, width=0.6)
                
                # Ajustar escala para Primeira Escrita
                if is_primeira_escrita:
                    ax.set_ylim(0, 8)
                    ax.set_yticks(range(0, 9))
                else:
                    ax.set_ylim(0, 5)
                    ax.set_yticks(range(0, 6))
                
                ax.set_ylabel('Estágio de Desenvolvimento')
                ax.set_title(f'Campo: {campo}')
                plt.xticks(rotation=45, ha='right')
                
                # Adicionar valores nas barras
                for bar in bars:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                            f'{height:.1f}', ha='center', va='bottom')
                
                # Legenda
                legend_elements = [
                    Patch(facecolor='#36a2eb', label='Direitos'),
                    Patch(facecolor='#ff6b6b', label='Maior atenção necessária')
                ]
                ax.legend(handles=legend_elements, loc='upper right')
                plt.tight_layout()
                
                # Tabela Detalhada
                story.append(safe_paragraph(f"<b>Campo: {campo}</b>", estilo_destaque))
                story.append(Spacer(1, 5))

                # Salvar gráfico
                img_buffer = BytesIO()
                plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', facecolor='white')
                plt.close(fig)
                img_buffer.seek(0)
                img = Image(img_buffer)
                img.drawHeight = 300
                img.drawWidth = min(500, img.drawHeight * (1.0 * img.imageWidth / img.imageHeight))
                story.append(img)
                story.append(Spacer(1, 15))

                
                
                # Cabeçalhos da tabela
                tabela_dados = [["Verbo", "Estágio"]]
                
                # Adicionar dados à tabela
                for direito in sorted(direitos, key=lambda x: x['estagio_numero']):
                    tabela_dados.append([
                        direito['verbo'],
                        str(direito['estagio_numero'])
                    ])
                
                # Ajustar larguras das colunas
                col_widths = [80, 50, 120, 70, 100]
                if is_primeira_escrita:
                    col_widths[1] = 60  # Mais espaço para o estágio
                    col_widths[2] = 60  # Mais espaço para descrição mais longa
                
                # Criar tabela
                tabela = Table(tabela_dados, colWidths=col_widths)
                
                # Estilo base da tabela
                estilo_base = [
                    ('BACKGROUND', (0, 0), (-1, 0), '#4472C4'),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('ALIGN', (1, 0), (1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 9),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
                    ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6')),
                    ('FONTSIZE', (0, 1), (-1, -1), 8),
                ]
                
                # Aplicar destaque para todos os direitos com menor estágio
                for i, d in enumerate(sorted(direitos, key=lambda x: x['estagio_numero']), start=1):
                    if d['estagio_numero'] == menor_estagio:
                        estilo_base += [
                            ('BACKGROUND', (0, i), (-1, i), '#fff3bf'),
                            ('TEXTCOLOR', (0, i), (-1, i), '#d63900'),
                            ('FONTNAME', (0, i), (-1, i), 'Helvetica-Bold')
                        ]
                
                tabela.setStyle(TableStyle(estilo_base))
                story.append(tabela)
                
                # Adicionar observação sobre TODOS os direitos destacados
                if direitos_destaque:
                    story.append(Spacer(1, 10))
                    obs_titulo = safe_paragraph("<font color='#d63900'><b>Direitos que requerem atenção:</b></font>", estilo_normal)
                    story.append(obs_titulo)
                    
                    for direito in direitos_destaque:
                        obs_texto = (f"<font color='#d63900'>• <b>{direito['verbo']}</b> - {direito['direito_descricao']} "
                                   f"(Estágio {direito['estagio_numero']} - {direito['descricao_estagio']})</font>")
                        story.append(safe_paragraph(obs_texto, estilo_normal))
                        story.append(Spacer(1, 5))
                    
                    story.append(Spacer(1, 10))
                
                story.append(Spacer(1, 20))
                
                

                # Quebra de página ANTES do próximo campo (exceto no último)
                if campo != campos[-1]:
                    story.append(PageBreak())  # <-- Nova página para o próximo campo


        # Rodapé
        #data_emissao = datetime.now().strftime("%d/%m/%Y ")
        #rodape = safe_paragraph(f"<i>Relatório gerado em {data_emissao} - Sistema de Avaliação</i>", styles['Italic'])
        #story.append(rodape)

        # Gerar PDF
        doc.build(story)
        buffer.seek(0)

        return send_file(
            buffer,
            as_attachment=True,
            download_name=f"relatorio_{aluno_info[0]}.pdf".replace(" ", "_"),
            mimetype='application/pdf'
        )
        
    except Exception as e:
        app.logger.error(f"Erro ao gerar PDF: {str(e)}", exc_info=True)
        return render_template('error.html', error="Erro ao gerar relatório"), 500
    finally:
        if 'conn' in locals():
            conn.close()



@app.route('/gerar-pdf-escola-turma')
@admin_required
def gerar_pdf_escola_turma():
    try:
        escola_id = request.args.get('escola_id', type=int)
        turma_id = request.args.get('turma_id', type=int)
        segmento_id = request.args.get('segmento_id', type=int, default=1)  # 1=Infantil, 2=Fundamental
        
        if not escola_id and not turma_id:
            return render_template('error.html', error="Nenhuma escola ou turma selecionada"), 400

        # Configurações do PDF
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import numpy as np
        from matplotlib.patches import Patch
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib import colors
        from io import BytesIO
        from datetime import datetime
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Buscar informações básicas
        if turma_id:
            cur.execute("""
                SELECT t.nome, e.nome, ans.nome 
                FROM turmas t
                JOIN escolas e ON t.escola_id = e.id
                JOIN ano_series ans ON t.ano_serie_id = ans.id
                WHERE t.id = %s
            """, (turma_id,))
            info = cur.fetchone()
            titulo = f"Turma: {info[0]} - {info[1]}"
            subtitulo = f"Série: {info[2]}"
        else:
            cur.execute("SELECT nome FROM escolas WHERE id = %s", (escola_id,))
            info = cur.fetchone()
            titulo = f"Escola: {info[0]}"
            subtitulo = ""

        # Criar PDF
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter,
                              rightMargin=40, leftMargin=40,
                              topMargin=40, bottomMargin=40)
        styles = getSampleStyleSheet()
        story = []
        
        # Estilos
        estilo_titulo = styles['Title']
        estilo_titulo.alignment = 1
        estilo_normal = styles['Normal']
        estilo_normal.fontName = 'Helvetica'
        estilo_normal.fontSize = 10
        estilo_destaque = styles['Normal']
        estilo_destaque.fontName = 'Helvetica-Bold'
        estilo_destaque.textColor = colors.HexColor("#000000")
        # CABEÇALHO COM LOGOS
        logo_somos = 'static/logo_somos.png'  # Ajuste o caminho conforme necessário
        logo_pref = 'static/prefeitura_sj.png'  # Ajuste o caminho conforme necessário
        
        # Tabela com 3 colunas para o cabeçalho (logo esquerda, título, logo direita)
        cabecalho_tabela = Table([
            [Image(logo_somos, width=60, height=30), 
             Paragraph("<b>Acompanhamento do Desenvolvimento da Aprendizagem</b>", estilo_titulo), 
             Image(logo_pref, width=60, height=30)]
        ], colWidths=[80, 360, 80])
        
        # Estilo da tabela do cabeçalho
        cabecalho_tabela.setStyle(TableStyle([
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('ALIGN', (1, 0), (1, 0), 'CENTER'),
            ('ALIGN', (2, 0), (2, 0), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ]))
        
        story.append(cabecalho_tabela)
        story.append(Spacer(1, 10))
        
        # Título e subtítulo da escola/turma
        story.append(Paragraph(titulo, estilo_titulo))
        if subtitulo:
            story.append(Paragraph(subtitulo, estilo_normal))
        story.append(Spacer(1, 20))

        

        # Dados estatísticos gerais
        if turma_id:
            cur.execute("""
                SELECT COUNT(*) FROM alunos WHERE turma_id = %s
            """, (turma_id,))
            total_alunos = cur.fetchone()[0]
            
            if segmento_id == 1:
                cur.execute("""
                    SELECT COUNT(DISTINCT a.matricula)
                    FROM alunos a
                    WHERE a.turma_id = %s
                    AND EXISTS (
                        SELECT 1 FROM avaliacoes_direitos WHERE aluno_matricula = a.matricula
                    )
                """, (turma_id,))
            else:
                cur.execute("""
                    SELECT COUNT(DISTINCT a.matricula)
                    FROM alunos a
                    WHERE a.turma_id = %s
                    AND EXISTS (
                        SELECT 1 FROM avaliacoes_habilidades WHERE aluno_matricula = a.matricula
                    )
                """, (turma_id,))
            alunos_avaliados = cur.fetchone()[0]
        else:
            if segmento_id == 1:
                cur.execute("""
                    SELECT COUNT(DISTINCT a.matricula)
                    FROM alunos a
                    JOIN turmas t ON a.turma_id = t.id
                    WHERE t.escola_id = %s AND (
                        t.segmento_id = 1 OR EXISTS (
                            SELECT 1 FROM serie_faixa_etaria sfe
                            JOIN ano_series ans ON sfe.serie_id = ans.id
                            WHERE ans.id = t.ano_serie_id AND sfe.faixa_etaria_id = 1
                        )
                    )
                """, (escola_id,))
                total_alunos = cur.fetchone()[0]
                
                cur.execute("""
                    SELECT COUNT(DISTINCT a.matricula)
                    FROM alunos a
                    JOIN turmas t ON a.turma_id = t.id
                    WHERE t.escola_id = %s AND (
                        t.segmento_id = 1 OR EXISTS (
                            SELECT 1 FROM serie_faixa_etaria sfe
                            JOIN ano_series ans ON sfe.serie_id = ans.id
                            WHERE ans.id = t.ano_serie_id AND sfe.faixa_etaria_id = 1
                        )
                    )
                    AND EXISTS (
                        SELECT 1 FROM avaliacoes_direitos WHERE aluno_matricula = a.matricula
                    )
                """, (escola_id,))
            else:
                cur.execute("""
                    SELECT COUNT(DISTINCT a.matricula)
                    FROM alunos a
                    JOIN turmas t ON a.turma_id = t.id
                    WHERE t.escola_id = %s AND t.segmento_id = 2
                """, (escola_id,))
                total_alunos = cur.fetchone()[0]
                
                cur.execute("""
                    SELECT COUNT(DISTINCT a.matricula)
                    FROM alunos a
                    JOIN turmas t ON a.turma_id = t.id
                    WHERE t.escola_id = %s AND t.segmento_id = 2
                    AND EXISTS (
                        SELECT 1 FROM avaliacoes_habilidades WHERE aluno_matricula = a.matricula
                    )
                """, (escola_id,))
            alunos_avaliados = cur.fetchone()[0]
        
        progresso = (alunos_avaliados / total_alunos * 100) if total_alunos > 0 else 0
        
        # Adicionar estatísticas ao PDF
        story.append(Paragraph("<b>DADOS GERAIS</b>", estilo_destaque))
        story.append(Spacer(1, 10))
        
        dados_gerais = [
            ["Total de Alunos", total_alunos],
            ["Alunos Avaliados", alunos_avaliados],
            ["Progresso", f"{progresso:.1f}%"]
        ]
        
        tabela_geral = Table(dados_gerais, colWidths=[150, 100])
        tabela_geral.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), '#4472C4'),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6'))
        ]))
        story.append(tabela_geral)
        story.append(Spacer(1, 20))

        # Gráfico Radar (média por campo/disciplina)
        if segmento_id == 1:
            # Infantil - Campos de Experiência
            if turma_id:
                cur.execute("""
                    SELECT 
                        ce.nome,
                        AVG(ad.estagio_numero),
                        COUNT(ad.estagio_numero)
                    FROM avaliacoes_direitos ad
                    JOIN direitos_aprendizagem d ON ad.direito_id = d.id
                    JOIN campos_experiencia ce ON d.campo_experiencia_id = ce.id
                    JOIN alunos a ON ad.aluno_matricula = a.matricula
                    WHERE a.turma_id = %s
                    GROUP BY ce.nome
                    ORDER BY ce.nome
                """, (turma_id,))
            else:
                cur.execute("""
                    SELECT 
                        ce.nome,
                        AVG(ad.estagio_numero),
                        COUNT(ad.estagio_numero)
                    FROM avaliacoes_direitos ad
                    JOIN direitos_aprendizagem d ON ad.direito_id = d.id
                    JOIN campos_experiencia ce ON d.campo_experiencia_id = ce.id
                    JOIN alunos a ON ad.aluno_matricula = a.matricula
                    JOIN turmas t ON a.turma_id = t.id
                    WHERE t.escola_id = %s AND (
                        t.segmento_id = 1 OR EXISTS (
                            SELECT 1 FROM serie_faixa_etaria sfe
                            JOIN ano_series ans ON sfe.serie_id = ans.id
                            WHERE ans.id = t.ano_serie_id AND sfe.faixa_etaria_id = 1
                        )
                    )
                    GROUP BY ce.nome
                    ORDER BY ce.nome
                """, (escola_id,))
            
            dados_radar = cur.fetchall()
            campos = [row[0] for row in dados_radar]
            medias = [float(row[1]) for row in dados_radar]
            
            if campos:
                is_primeira_escrita = "Primeira Escrita" in campos
                
                fig = plt.figure(figsize=(8, 8))
                ax = fig.add_subplot(111, polar=True)
                
                N = len(campos)
                angles = [n / float(N) * 2 * np.pi for n in range(N)]
                angles += angles[:1]
                valores = medias + [medias[0]]
                
                ax.plot(angles, valores, linewidth=2, color='#36a2eb')
                ax.fill(angles, valores, alpha=0.25, color='#36a2eb')
                ax.set_theta_offset(np.pi / 2)
                ax.set_theta_direction(-1)
                ax.set_thetagrids(np.degrees(angles[:-1]), labels=campos)
                
                for label, angle in zip(ax.get_xticklabels(), angles[:-1]):
                    label.set_horizontalalignment('center')
                    angle_deg = angle * 180/np.pi
                    if angle_deg > 90:
                        angle_deg -= 180
                    label.set_rotation(angle_deg)
                    label.set_rotation_mode('anchor')
                
                if is_primeira_escrita:
                    ax.set_ylim(0, 8)
                    ax.set_yticks(range(0, 9))
                    ax.set_yticklabels([str(i) for i in range(0, 9)])
                else:
                    ax.set_ylim(0, 5)
                    ax.set_yticks(range(0, 6))
                    ax.set_yticklabels([str(i) for i in range(0, 6)])
                
                ax.grid(color=(0, 0, 0, 0.1), linestyle='-', linewidth=0.5)
                ax.set_facecolor('white')
                ax.spines['polar'].set_visible(False)
                ax.set_title('Média por Campo de Experiência', pad=20)
                
                img_buffer = BytesIO()
                plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', facecolor='white')
                plt.close(fig)
                
                story.append(Paragraph("<b>Desenvolvimento da Aprendizagem dos Campos de Experiência</b>", estilo_destaque))
                story.append(Spacer(1, 10))
                img_buffer.seek(0)
                img = Image(img_buffer)
                img.drawHeight = 300
                img.drawWidth = img.drawHeight * (1.0 * img.imageWidth / img.imageHeight)
                story.append(img)
                story.append(Spacer(1, 20))
                
                # Adicionar legenda sobre o destaque
                story.append(Paragraph(
                    "<i>Nota: Será destacado em amarelo o direito em desenvovimento (maior concentração nos estágios iniciais)</i>", 
                    styles['Italic']
                ))
                story.append(Spacer(1, 15))
                story.append(PageBreak())
            for i, campo in enumerate(campos):
                is_primeira_escrita = campo == "Primeira Escrita"
                
                
                # Título do Campo
                story.append(Paragraph(f"<b>CAMPO DE EXPERIÊNCIA: {campo}</b>", estilo_titulo))
                
                # Consulta SQL (mantida igual)
                if turma_id:
                    cur.execute("""
                        SELECT 
                            v.nome as verbo,
                            d.descricao as direito,
                            ad.estagio_numero,
                            COUNT(*) as total
                        FROM avaliacoes_direitos ad
                        JOIN direitos_aprendizagem d ON ad.direito_id = d.id
                        JOIN verbos v ON d.verbo_id = v.id
                        JOIN campos_experiencia ce ON d.campo_experiencia_id = ce.id
                        JOIN alunos a ON ad.aluno_matricula = a.matricula
                        WHERE a.turma_id = %s AND ce.nome = %s
                        GROUP BY v.nome, d.descricao, ad.estagio_numero
                        ORDER BY v.nome, d.descricao, ad.estagio_numero
                    """, (turma_id, campo))
                else:
                    cur.execute("""
                        SELECT 
                            v.nome as verbo,
                            d.descricao as direito,
                            ad.estagio_numero,
                            COUNT(*) as total
                        FROM avaliacoes_direitos ad
                        JOIN direitos_aprendizagem d ON ad.direito_id = d.id
                        JOIN verbos v ON d.verbo_id = v.id
                        JOIN campos_experiencia ce ON d.campo_experiencia_id = ce.id
                        JOIN alunos a ON ad.aluno_matricula = a.matricula
                        JOIN turmas t ON a.turma_id = t.id
                        WHERE t.escola_id = %s AND ce.nome = %s AND (
                            t.segmento_id = 1 OR EXISTS (
                                SELECT 1 FROM serie_faixa_etaria sfe
                                JOIN ano_series ans ON sfe.serie_id = ans.id
                                WHERE ans.id = t.ano_serie_id AND sfe.faixa_etaria_id = 1
                            )
                        )
                        GROUP BY v.nome, d.descricao, ad.estagio_numero
                        ORDER BY v.nome, d.descricao, ad.estagio_numero
                    """, (escola_id, campo))
                
                dados_campo = cur.fetchall()
                
                verbos_data = {}
                direitos_por_verbo = {}
                for verbo, direito, estagio, total in dados_campo:
                    if verbo not in verbos_data:
                        if is_primeira_escrita:
                            verbos_data[verbo] = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0}
                        else:
                            verbos_data[verbo] = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
                        direitos_por_verbo[verbo] = direito
                    verbos_data[verbo][estagio] += total
                
                verbos = sorted(verbos_data.keys())
                
                # Identificar o verbo com maior dificuldade (APENAS para campos que não são Primeira Escrita)
                verbo_destaque = None
                if not is_primeira_escrita:
                    max_concentracao = 0
                    menor_estagio = float('inf')
                    
                    for verbo in verbos:
                        total_verbo = sum(verbos_data[verbo].values())
                        if total_verbo == 0:
                            continue
                        
                        concentracao = (verbos_data[verbo][1] + verbos_data[verbo][2]) / total_verbo
                        estagios_com_alunos = [e for e in verbos_data[verbo] if verbos_data[verbo][e] > 0]
                        min_estagio = min(estagios_com_alunos) if estagios_com_alunos else float('inf')
                        
                        if (concentracao > max_concentracao) or \
                        (concentracao == max_concentracao and min_estagio < menor_estagio):
                            max_concentracao = concentracao
                            verbo_destaque = verbo
                            menor_estagio = min_estagio
                
                # Preparar dados para gráfico
                dados_grafico = []
                for verbo in verbos:
                    total_verbo = sum(verbos_data[verbo].values())
                    if total_verbo == 0:
                        continue
                    
                    if is_primeira_escrita:
                        porcentagens = [
                            (verbos_data[verbo][1] / total_verbo * 100),
                            (verbos_data[verbo][2] / total_verbo * 100),
                            (verbos_data[verbo][3] / total_verbo * 100),
                            (verbos_data[verbo][4] / total_verbo * 100),
                            (verbos_data[verbo][5] / total_verbo * 100),
                            (verbos_data[verbo][6] / total_verbo * 100),
                            (verbos_data[verbo][7] / total_verbo * 100),
                            (verbos_data[verbo][8] / total_verbo * 100)
                        ]
                    else:
                        porcentagens = [
                            (verbos_data[verbo][1] / total_verbo * 100),
                            (verbos_data[verbo][2] / total_verbo * 100),
                            (verbos_data[verbo][3] / total_verbo * 100),
                            (verbos_data[verbo][4] / total_verbo * 100),
                            (verbos_data[verbo][5] / total_verbo * 100)
                        ]
                    dados_grafico.append({
                        'verbo': verbo,
                        'porcentagens': porcentagens,
                        'total': total_verbo,
                        'destaque': verbo == verbo_destaque,
                        'direito': direitos_por_verbo.get(verbo, '')
                    })
                
                if not dados_grafico:
                    continue
                
                # Criar gráfico de barras
                fig, ax = plt.subplots(figsize=(10, 6))
                
                if is_primeira_escrita:
                    cores = ['#ff6b6b', '#ff8f8f', '#ffb3b3', '#ffd7d7', '#d7e3ff', '#a3c8ff', '#6ea6ff', '#36a2eb']
                    labels_estagios = [f'Estágio {i}' for i in range(1, 9)]
                else:
                    cores = ['#ff6b6b', '#ffa3a3', '#a3d8ff', '#4da6ff', '#36a2eb']
                    labels_estagios = [f'Estágio {i}' for i in range(1, 6)]
                
                bottom = np.zeros(len(dados_grafico))
                for i in range(len(cores)):
                    valores = [d['porcentagens'][i] for d in dados_grafico]
                    barras = ax.bar(
                        [d['verbo'] for d in dados_grafico],
                        valores,
                        bottom=bottom,
                        color=cores[i],
                        label=labels_estagios[i],
                        edgecolor='white'
                    )
                    
                    # Aplicar destaque no gráfico (apenas para campos que não são Primeira Escrita)
                    if verbo_destaque and not is_primeira_escrita:
                        for j, bar in enumerate(barras):
                            if dados_grafico[j]['destaque']:
                                bar.set_edgecolor('#d63900')
                                bar.set_linewidth(2)
                    
                    for bar in barras:
                        height = bar.get_height()
                        if height > 10:
                            ax.text(
                                bar.get_x() + bar.get_width() / 2,
                                bar.get_y() + height / 2,
                                f'{height:.0f}%',
                                ha='center',
                                va='center',
                                color='white' if i < 2 else 'black',
                                fontsize=8
                            )
                    
                    bottom += valores
                
                ax.set_ylabel('Porcentagem de Alunos')
                ax.set_title(f'Distribuição por Estágio - {campo}')
                ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1))
                plt.xticks(rotation=45, ha='right')
                plt.tight_layout()
                
                img_buffer = BytesIO()
                plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight')
                plt.close(fig)
                
                # Adicionar mensagem de dificuldade (apenas para campos que não são Primeira Escrita)
                if verbo_destaque and not is_primeira_escrita:
                    direito_destaque = direitos_por_verbo.get(verbo_destaque, '')
                    story.append(Paragraph(
                        f"<font color='#003366'><b>Em desenvovimento: {verbo_destaque} ({direito_destaque}) - " +
                        f"{max_concentracao:.1%} </b></font>", 
                        estilo_normal
                    ))
                
                story.append(Spacer(1, 10))
                img_buffer.seek(0)
                img = Image(img_buffer)
                img.drawHeight = 300
                img.drawWidth = min(500, img.drawHeight * (1.0 * img.imageWidth / img.imageHeight))
                story.append(img)
                story.append(Spacer(1, 15))
                
                # Tabela com os dados
                if is_primeira_escrita:
                    headers = ["Verbo"] + [f'Estágio {i}' for i in range(1, 9)] 
                    col_widths = [160] + [50]*8  # Larguras ajustadas
                else:
                    headers = ["Verbo"] + [f'Estágio {i}' for i in range(1, 6)] 
                    col_widths = [160] + [50]*5  # Larguras ajustadas
                
                tabela_dados = [headers]
                for dado in dados_grafico:
                    linha = [dado['verbo']]
                    linha.extend([f"{p:.1f}%" for p in dado['porcentagens']])
                    tabela_dados.append(linha)
                
                tabela = Table(tabela_dados, colWidths=col_widths)
                estilo = [
                    ('BACKGROUND', (0, 0), (-1, 0), '#4472C4'),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 8),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
                    ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6')),
                ]
                
                # Aplicar destaque na tabela (apenas para campos que não são Primeira Escrita)
                if verbo_destaque and not is_primeira_escrita:
                    for idx, linha in enumerate(tabela_dados[1:], start=1):
                        if linha[0] == verbo_destaque:
                            estilo.extend([
                                ('BACKGROUND', (0, idx), (-1, idx), '#fff3bf'),
                                ('TEXTCOLOR', (0, idx), (-1, idx), '#d63900'),
                                ('FONTNAME', (0, idx), (-1, idx), 'Helvetica-Bold')
                            ])
                            break
                
                tabela.setStyle(TableStyle(estilo))
                story.append(tabela)
                story.append(PageBreak())
                
            # Quebra de página ANTES do próximo campo (exceto no último)
            #if i < len(campos) - 1:
             #       story.append(PageBreak())
                    
        
        # Rodapé
        data_emissao = datetime.now().strftime("%d/%m/%Y")
        rodape = Paragraph(f"<i>Relatório gerado em {data_emissao} - Sistema de Avaliação</i>", styles['Italic'])
        #story.append(rodape)

        doc.build(story)
        buffer.seek(0)

        nome_arquivo = f"relatorio_{'turma' if turma_id else 'escola'}_{info[0].replace(' ', '_')}.pdf"
        return send_file(
            buffer,
            as_attachment=True,
            download_name=nome_arquivo,
            mimetype='application/pdf'
        )
        
    except Exception as e:
        app.logger.error(f"Erro ao gerar PDF escola/turma: {str(e)}", exc_info=True)
        return render_template('error.html', error="Erro ao gerar relatório"), 500
    finally:
        if 'conn' in locals():
            conn.close()




@app.route('/admin/fundamental')
@admin_required
def painel_fundamental():
    try:
        escola_id = request.args.get('escola_id', type=int)
        turma_id = request.args.get('turma_id', type=int)
        aluno_matricula = request.args.get('aluno_matricula', type=str)

        conn = get_db_connection()
        cur = conn.cursor()

        # Filtro para ensino fundamental (segmento_id = 2)
        filtro_fundamental = "t.segmento_id = 2"

        # 1. Escolas com turmas de ensino fundamental
        cur.execute(f"""
            SELECT DISTINCT e.id, e.nome
            FROM escolas e
            WHERE EXISTS (
                SELECT 1 FROM turmas t
                WHERE t.escola_id = e.id AND ({filtro_fundamental})
            )
            ORDER BY e.nome
        """)
        escolas = [{'id': row[0], 'nome': row[1]} for row in cur.fetchall()]

        # 2. Turmas da escola
        turmas = []
        if escola_id:
            cur.execute(f"""
                SELECT t.id, t.nome, ans.nome
                FROM turmas t
                JOIN ano_series ans ON t.ano_serie_id = ans.id
                WHERE t.escola_id = %s AND ({filtro_fundamental})
                ORDER BY t.nome
            """, (escola_id,))
            turmas = [{'id': row[0], 'nome': row[1], 'ano_serie': row[2]} for row in cur.fetchall()]

        # 3. Dados de Resumo (Dinâmicos por seleção)
        resumo = {
            'tipo': 'Geral',
            'nome': 'Rede de Ensino Fundamental',
            'total_escolas': len(escolas),
            'total_turmas': 0,
            'total_alunos': 0,
            'alunos_avaliados': 0,
            'progresso': 0,
            'media_geral': 0
        }

        dados_grafico = []
        
        if aluno_matricula:
            # RESUMO DO ALUNO
            cur.execute("""
                SELECT a.nome, t.nome, e.nome, ans.nome
                FROM alunos a
                JOIN turmas t ON a.turma_id = t.id
                JOIN escolas e ON t.escola_id = e.id
                JOIN ano_series ans ON t.ano_serie_id = ans.id
                WHERE a.matricula = %s
            """, (aluno_matricula,))
            aluno_info = cur.fetchone()
            
            cur.execute("SELECT COUNT(*) FROM avaliacoes_habilidades WHERE aluno_matricula = %s", (aluno_matricula,))
            total_avaliacoes = cur.fetchone()[0]
            
            cur.execute("SELECT AVG(estagio_numero) FROM avaliacoes_habilidades WHERE aluno_matricula = %s", (aluno_matricula,))
            media_aluno = cur.fetchone()[0] or 0
            
            resumo.update({
                'tipo': 'Aluno',
                'nome': aluno_info[0],
                'matricula': aluno_matricula,
                'turma': aluno_info[1],
                'escola': aluno_info[2],
                'serie': aluno_info[3],
                'total_avaliacoes': total_avaliacoes,
                'media_geral': float(media_aluno)
            })
            
        elif turma_id:
            # RESUMO DA TURMA
            cur.execute("""
                SELECT t.nome, e.nome, ans.nome
                FROM turmas t
                JOIN escolas e ON t.escola_id = e.id
                JOIN ano_series ans ON t.ano_serie_id = ans.id
                WHERE t.id = %s
            """, (turma_id,))
            turma_info = cur.fetchone()
            
            cur.execute("SELECT COUNT(*) FROM alunos WHERE turma_id = %s", (turma_id,))
            total_alunos = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(DISTINCT aluno_matricula) 
                FROM avaliacoes_habilidades ah
                JOIN alunos a ON ah.aluno_matricula = a.matricula
                WHERE a.turma_id = %s
            """, (turma_id,))
            alunos_avaliados = cur.fetchone()[0]
            
            cur.execute("""
                SELECT AVG(estagio_numero) 
                FROM avaliacoes_habilidades ah
                JOIN alunos a ON ah.aluno_matricula = a.matricula
                WHERE a.turma_id = %s
            """, (turma_id,))
            media_turma = cur.fetchone()[0] or 0
            
            resumo.update({
                'tipo': 'Turma',
                'nome': turma_info[0],
                'escola': turma_info[1],
                'serie': turma_info[2],
                'total_alunos': total_alunos,
                'alunos_avaliados': alunos_avaliados,
                'progresso': round((alunos_avaliados / total_alunos * 100) if total_alunos > 0 else 0),
                'media_geral': float(media_turma)
            })
            
        elif escola_id:
            # RESUMO DA ESCOLA
            cur.execute("SELECT nome FROM escolas WHERE id = %s", (escola_id,))
            escola_nome = cur.fetchone()[0]
            
            cur.execute(f"SELECT COUNT(*) FROM turmas t WHERE t.escola_id = %s AND ({filtro_fundamental})", (escola_id,))
            total_turmas = cur.fetchone()[0]

            cur.execute(f"""
                SELECT COUNT(DISTINCT a.matricula)
                FROM alunos a
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.escola_id = %s AND ({filtro_fundamental})
            """, (escola_id,))
            total_alunos = cur.fetchone()[0]

            cur.execute(f"""
                SELECT COUNT(DISTINCT a.matricula)
                FROM alunos a
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.escola_id = %s AND ({filtro_fundamental})
                AND EXISTS (SELECT 1 FROM avaliacoes_habilidades WHERE aluno_matricula = a.matricula)
            """, (escola_id,))
            alunos_avaliados = cur.fetchone()[0]
            
            cur.execute(f"""
                SELECT AVG(ah.estagio_numero)
                FROM avaliacoes_habilidades ah
                JOIN alunos a ON ah.aluno_matricula = a.matricula
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.escola_id = %s
            """, (escola_id,))
            media_escola = cur.fetchone()[0] or 0

            resumo.update({
                'tipo': 'Escola',
                'nome': escola_nome,
                'total_turmas': total_turmas,
                'total_alunos': total_alunos,
                'alunos_avaliados': alunos_avaliados,
                'progresso': round((alunos_avaliados / total_alunos * 100) if total_alunos > 0 else 0),
                'media_geral': float(media_escola)
            })

        # 4. Dados para o Gráfico (Escola, Turma ou Aluno)
        if aluno_matricula:
            cur.execute("""
                SELECT d.nome, AVG(ah.estagio_numero), COUNT(*)
                FROM avaliacoes_habilidades ah
                JOIN habilidades h ON ah.habilidade_id = h.id
                JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                JOIN disciplinas d ON ut.disciplina_id = d.id
                WHERE ah.aluno_matricula = %s
                GROUP BY d.nome ORDER BY d.nome
            """, (aluno_matricula,))
        elif turma_id:
            cur.execute("""
                SELECT d.nome, AVG(ah.estagio_numero), COUNT(*)
                FROM avaliacoes_habilidades ah
                JOIN habilidades h ON ah.habilidade_id = h.id
                JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                JOIN disciplinas d ON ut.disciplina_id = d.id
                JOIN alunos a ON ah.aluno_matricula = a.matricula
                WHERE a.turma_id = %s
                GROUP BY d.nome ORDER BY d.nome
            """, (turma_id,))
        elif escola_id:
            cur.execute("""
                SELECT d.nome, AVG(ah.estagio_numero), COUNT(*)
                FROM avaliacoes_habilidades ah
                JOIN habilidades h ON ah.habilidade_id = h.id
                JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                JOIN disciplinas d ON ut.disciplina_id = d.id
                JOIN alunos a ON ah.aluno_matricula = a.matricula
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.escola_id = %s
                GROUP BY d.nome ORDER BY d.nome
            """, (escola_id,))
        
        if escola_id or turma_id or aluno_matricula:
            dados_grafico = [{'disciplina': row[0], 'media': float(row[1]), 'total': row[2]} for row in cur.fetchall()]

        # 5. Alunos da turma selecionada
        alunos = []
        if turma_id:
            cur.execute("SELECT matricula, nome FROM alunos WHERE turma_id = %s ORDER BY nome", (turma_id,))
            alunos = [{'matricula': row[0], 'nome': row[1]} for row in cur.fetchall()]

        # 6. Avaliações do aluno selecionado
        avaliacoes = []
        if aluno_matricula:
            cur.execute("""
                SELECT 
                    d.nome, ut.nome, h.descricao, ah.estagio_numero, 
                    p.nome, ah.fim_avaliacao, ah.inicio_avaliacao
                FROM avaliacoes_habilidades ah
                JOIN habilidades h ON ah.habilidade_id = h.id
                JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                JOIN disciplinas d ON ut.disciplina_id = d.id
                LEFT JOIN professores p ON ah.avaliador_id = p.id_plurall
                WHERE ah.aluno_matricula = %s
                ORDER BY d.nome, ut.nome, h.descricao
            """, (aluno_matricula,))
            
            for row in cur.fetchall():
                duracao = (row[5] - row[6]).total_seconds() / 60 if row[5] and row[6] else 0
                avaliacoes.append({
                    'disciplina': row[0],
                    'unidade_tematica': row[1],
                    'habilidade': row[2],
                    'estagio': row[3],
                    'avaliador': row[4] or 'Não informado',
                    'data_avaliacao': row[5],
                    'duracao': round(duracao, 1)
                })

        return render_template('painel_fundamental.html',
                           escolas=escolas,
                           turmas=turmas,
                           alunos=alunos,
                           avaliacoes=avaliacoes,
                           dados_grafico=dados_grafico,
                           resumo=resumo,
                           escola_selecionada=escola_id,
                           turma_selecionada=turma_id,
                           aluno_selecionado=aluno_matricula,
                           now=get_brazil_time())

    except Exception as e:
        app.logger.error(f"Erro no painel fundamental: {str(e)}", exc_info=True)
        return render_template('error.html', error="Erro ao carregar dados do ensino fundamental"), 500
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()



@app.route('/relatorio/fundamental/<tipo>/<id>')
@admin_required
def gerar_relatorio_fundamental(tipo, id):
    try:
        
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Função para extrair código da habilidade (conteúdo entre parênteses)
        def extrair_codigo_habilidade(descricao):
            match = re.search(r'\((.*?)\)', descricao)
            return match.group(1) if match else descricao
        
        # Buffer para o PDF
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter,
                              rightMargin=40, leftMargin=40,
                              topMargin=40, bottomMargin=40)
        story = []
        
        # Configurar fontes Unicode
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        try:
            pdfmetrics.registerFont(TTFont('DejaVuSans', 'DejaVuSans.ttf'))
            pdfmetrics.registerFont(TTFont('DejaVuSans-Bold', 'DejaVuSans-Bold.ttf'))
            font_name = 'DejaVuSans'
        except:
            font_name = 'Helvetica'
        
        # Estilos
        styles = getSampleStyleSheet()
        estilo_titulo = styles['Title']
        estilo_titulo.alignment = 1
        estilo_titulo.fontName = font_name
        estilo_normal = styles['Normal']
        estilo_normal.fontName = font_name
        estilo_normal.fontSize = 10
        estilo_destaque = styles['Normal']
        estilo_destaque.fontName = font_name + '-Bold' if font_name != 'Helvetica' else 'Helvetica-Bold'
        estilo_destaque.textColor = colors.HexColor("#000000")
        
        # CABEÇALHO COM LOGOS
        logo_somos = 'static/logo_somos.png'
        logo_pref = 'static/prefeitura_sj.png'
        
        cabecalho_tabela = Table([
            [Image(logo_somos, width=60, height=30), 
             Paragraph("<b>Acompanhamento do Desenvolvimento da Aprendizagem - ENSINO FUNDAMENTAL</b>", estilo_titulo), 
             Image(logo_pref, width=60, height=30)]
        ], colWidths=[80, 360, 80])
        
        cabecalho_tabela.setStyle(TableStyle([
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('ALIGN', (1, 0), (1, 0), 'CENTER'),
            ('ALIGN', (2, 0), (2, 0), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ]))
        
        story.append(cabecalho_tabela)
        story.append(Spacer(1, 15))

        def safe_paragraph(text, style):
            if isinstance(text, str):
                try:
                    text = text.decode('utf-8')
                except (UnicodeDecodeError, AttributeError):
                    pass
            return Paragraph(text, style)
        
        # Dados comuns
        escola_nome = ""
        turma_nome = ""
        aluno_nome = ""
        aluno_matricula = ""
        
        # Consulta dados básicos conforme o tipo
        if tipo.startswith('escola'):
            cur.execute("SELECT nome FROM escolas WHERE id = %s", (id,))
            escola_nome = cur.fetchone()[0]
            titulo = f"Relatório da Escola - {escola_nome}"
            
        elif tipo.startswith('turma'):
            cur.execute("""
                SELECT t.nome, e.nome 
                FROM turmas t
                JOIN escolas e ON t.escola_id = e.id
                WHERE t.id = %s
            """, (id,))
            turma_nome, escola_nome = cur.fetchone()
            titulo = f"Relatório da Turma - {turma_nome} ({escola_nome})"
            
        elif tipo.startswith('aluno'):
            cur.execute("""
                SELECT a.nome, a.matricula, t.nome, e.nome 
                FROM alunos a
                JOIN turmas t ON a.turma_id = t.id
                JOIN escolas e ON t.escola_id = e.id
                WHERE a.matricula = %s
            """, (id,))
            aluno_nome, aluno_matricula, turma_nome, escola_nome = cur.fetchone()
            titulo = f"Relatório do Aluno - {aluno_nome} ({turma_nome})"
        
        # Informações do aluno/turma/escola
        if tipo.startswith('aluno'):
            info_aluno = [
                f"<b>Escola:</b> {escola_nome}",
                f"<b>Turma:</b> {turma_nome}",
                f"<b>Aluno:</b> {aluno_nome}",
                f"<b>Matrícula:</b> {aluno_matricula}"
            ]
        elif tipo.startswith('turma'):
            info_aluno = [
                f"<b>Escola:</b> {escola_nome}",
                f"<b>Turma:</b> {turma_nome}"
            ]
        else:
            info_aluno = [
                f"<b>Escola:</b> {escola_nome}"
            ]
            
        for info in info_aluno:
            story.append(safe_paragraph(info, estilo_normal))
            story.append(Spacer(1, 5))
        story.append(Spacer(1, 20))
        
        # Gráfico Radar com médias por disciplina (para aluno ou turma)
        if tipo.startswith('aluno') or tipo.startswith('turma'):
            if tipo.startswith('aluno'):
                query = """
                    SELECT d.nome, AVG(ah.estagio_numero)
                    FROM avaliacoes_habilidades ah
                    JOIN habilidades h ON ah.habilidade_id = h.id
                    JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                    JOIN disciplinas d ON ut.disciplina_id = d.id
                    WHERE ah.aluno_matricula = %s
                    GROUP BY d.nome
                    ORDER BY d.nome
                """
                params = (id,)
            else:
                query = """
                    SELECT d.nome, AVG(ah.estagio_numero)
                    FROM avaliacoes_habilidades ah
                    JOIN habilidades h ON ah.habilidade_id = h.id
                    JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                    JOIN disciplinas d ON ut.disciplina_id = d.id
                    JOIN alunos a ON ah.aluno_matricula = a.matricula
                    WHERE a.turma_id = %s
                    GROUP BY d.nome
                    ORDER BY d.nome
                """
                params = (id,)
            
            cur.execute(query, params)
            dados = cur.fetchall()
            disciplinas = [row[0] for row in dados]
            medias = [float(row[1]) for row in dados]
            
            # Cria gráfico radar
            fig = plt.figure(figsize=(8, 8))
            ax = fig.add_subplot(111, polar=True)
            cor_principal = '#36a2eb'
            cor_fundo = 'white'
            cor_grid = (0, 0, 0, 0.1)
            
            N = len(disciplinas)
            angles = [n / float(N) * 2 * np.pi for n in range(N)]
            angles += angles[:1]
            valores = medias + [medias[0]]
            
            ax.plot(angles, valores, linewidth=2, color=cor_principal)
            ax.fill(angles, valores, alpha=0.25, color=cor_principal)
            
            ax.set_theta_offset(np.pi / 2)
            ax.set_theta_direction(-1)
            ax.set_thetagrids(np.degrees(angles[:-1]), labels=disciplinas)
            
            for label, angle in zip(ax.get_xticklabels(), angles[:-1]):
                label.set_horizontalalignment('center')
                angle_deg = angle * 180/np.pi
                if angle_deg > 90:
                    angle_deg -= 180
                label.set_rotation(angle_deg)
                label.set_rotation_mode('anchor')
            
            ax.set_ylim(0, 5)
            ax.set_yticks(range(0, 6))
            ax.set_yticklabels([str(i) for i in range(0, 6)])
            
            ax.grid(color=cor_grid, linestyle='-', linewidth=0.5)
            ax.set_facecolor(cor_fundo)
            ax.spines['polar'].set_visible(False)
            
            story.append(safe_paragraph("<b>DESEMPENHO POR DISCIPLINA</b>", estilo_titulo))
            story.append(Spacer(1, 10))
            
            img_buffer = BytesIO()
            plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', facecolor=cor_fundo)
            plt.close(fig)
            
            img_buffer.seek(0)
            img = Image(img_buffer)
            img.drawHeight = 300
            img.drawWidth = img.drawHeight * (1.0 * img.imageWidth / img.imageHeight)
            story.append(img)
            story.append(Spacer(1, 50))
        
        # Gráficos por disciplina (para aluno)
        if tipo.startswith('aluno'):
            # Pega todas as disciplinas do aluno
            cur.execute("""
                SELECT DISTINCT d.id, d.nome
                FROM avaliacoes_habilidades ah
                JOIN habilidades h ON ah.habilidade_id = h.id
                JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                JOIN disciplinas d ON ut.disciplina_id = d.id
                WHERE ah.aluno_matricula = %s
                ORDER BY d.nome
            """, (id,))
            disciplinas = cur.fetchall()
            
            for disciplina_id, disciplina_nome in disciplinas:
                # Gráfico radar por disciplina (unidades temáticas)
                cur.execute("""
                    SELECT ut.nome, AVG(ah.estagio_numero)
                    FROM avaliacoes_habilidades ah
                    JOIN habilidades h ON ah.habilidade_id = h.id
                    JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                    WHERE ah.aluno_matricula = %s AND ut.disciplina_id = %s
                    GROUP BY ut.nome
                    ORDER BY ut.nome
                """, (id, disciplina_id))
                dados_ut = cur.fetchall()
                
                if len(dados_ut) > 2:  # Radar precisa de pelo menos 3 pontos
                    unidades = [row[0] for row in dados_ut]
                    medias_ut = [float(row[1]) for row in dados_ut]
                    
                    fig = plt.figure(figsize=(8, 8))
                    ax = fig.add_subplot(111, polar=True)
                    
                    angles = np.linspace(0, 2*np.pi, len(unidades), endpoint=False).tolist()
                    medias_ut += medias_ut[:1]
                    angles += angles[:1]
                    
                    ax.plot(angles, medias_ut, 'o-', linewidth=2, color=cor_principal)
                    ax.fill(angles, medias_ut, alpha=0.25, color=cor_principal)
                    
                    ax.set_theta_offset(np.pi / 2)
                    ax.set_theta_direction(-1)
                    ax.set_thetagrids(np.degrees(angles[:-1]), labels=unidades)
                    
                    for label, angle in zip(ax.get_xticklabels(), angles[:-1]):
                        label.set_horizontalalignment('center')
                        angle_deg = angle * 180/np.pi
                        if angle_deg > 90:
                            angle_deg -= 180
                        label.set_rotation(angle_deg)
                        label.set_rotation_mode('anchor')
                    
                    ax.set_ylim(0, 5)
                    ax.set_yticks(range(0, 6))
                    ax.set_yticklabels([str(i) for i in range(0, 6)])
                    
                    ax.grid(color=cor_grid, linestyle='-', linewidth=0.5)
                    ax.set_facecolor(cor_fundo)
                    ax.spines['polar'].set_visible(False)
                    ax.set_title(f'Média por Unidade Temática - {disciplina_nome}', y=1.1)
                    
                    story.append(PageBreak())
                    story.append(safe_paragraph(f"<b>Desempenho em {disciplina_nome}</b>", estilo_titulo))
                    story.append(Spacer(1, 10))
                    
                    img_buffer = BytesIO()
                    plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', facecolor=cor_fundo)
                    plt.close(fig)
                    
                    img_buffer.seek(0)
                    img = Image(img_buffer)
                    img.drawHeight = 300
                    img.drawWidth = img.drawHeight * (1.0 * img.imageWidth / img.imageHeight)
                    story.append(img)
                    story.append(Spacer(1, 20))
                
                # Gráfico de barras por unidade temática (habilidades) - VERTICAL
                cur.execute("""
                    SELECT ut.nome, h.descricao, ah.estagio_numero
                    FROM avaliacoes_habilidades ah
                    JOIN habilidades h ON ah.habilidade_id = h.id
                    JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                    WHERE ah.aluno_matricula = %s AND ut.disciplina_id = %s
                    ORDER BY ut.nome, h.descricao
                """, (id, disciplina_id))
                habilidades = cur.fetchall()
                
                # Organiza por unidade temática
                unidades_habilidades = {}
                for ut_nome, hab_desc, estagio in habilidades:
                    if ut_nome not in unidades_habilidades:
                        unidades_habilidades[ut_nome] = []
                    unidades_habilidades[ut_nome].append((hab_desc, estagio))
                
                # Cria gráfico para cada unidade temática
                for ut_nome, habs in unidades_habilidades.items():
                    # Extrai códigos e mantém descrições completas
                    habilidades_codigos = [extrair_codigo_habilidade(h[0]) for h in habs]
                    habilidades_completas = [h[0] for h in habs]
                    estagios = [h[1] for h in habs]
                    
                    # Verifica se há diferença entre os estágios para destacar
                    tem_diferenca = len(set(estagios)) > 1
                    min_estagio = min(estagios) if tem_diferenca else None
                    
                    # Cores - destaque apenas se houver diferença
                    cores = ['#1f77b4'] * len(estagios)
                    if tem_diferenca:
                        min_estagio_idx = estagios.index(min_estagio)
                        cores[min_estagio_idx] = '#ff7f0e'
                    
                    # Cria gráfico de barras VERTICAL
                    fig, ax = plt.subplots(figsize=(10, 6))
                    bars = ax.bar(habilidades_codigos, estagios, color=cores, width=0.6)
                    ax.set_ylim(0, 5)
                    ax.set_yticks(range(0, 6))
                    ax.set_title(f'Habilidades - {ut_nome}')
                    ax.set_ylabel('Estágio')
                    
                    # Rotaciona os rótulos do eixo X para melhor visualização
                    plt.xticks(rotation=45, ha='right')
                    
                    # Adiciona valores no topo das barras
                    for bar in bars:
                        height = bar.get_height()
                        ax.text(bar.get_x() + bar.get_width()/2., height,
                                f'{height:.1f}', ha='center', va='bottom')
                    
                    # Legenda (apenas se houver diferença)
                    if tem_diferenca:
                        legend_elements = [
                            Patch(facecolor='#1f77b4', label='Habilidades'),
                            Patch(facecolor='#ff7f0e', label='Maior atenção necessária')
                        ]
                        ax.legend(handles=legend_elements, loc='upper right')
                    plt.tight_layout()
                    
                    story.append(safe_paragraph(f"<b>Unidade Temática: {ut_nome}</b>", estilo_destaque))
                    story.append(Spacer(1, 10))
                    
                    img_buffer = BytesIO()
                    plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', facecolor='white')
                    plt.close(fig)
                    
                    img_buffer.seek(0)
                    img = Image(img_buffer)
                    img.drawHeight = 300
                    img.drawWidth = min(500, img.drawHeight * (1.0 * img.imageWidth / img.imageHeight))
                    story.append(img)
                    story.append(Spacer(1, 15))
                    
                    # Tabela com detalhes das habilidades (mostrando apenas códigos)
                    tabela_dados = [["Código", "Estágio"]]
                    
                    for hab_cod, estagio in zip(habilidades_codigos, estagios):
                        tabela_dados.append([hab_cod, str(estagio)])
                    
                    # Criar tabela
                    tabela = Table(tabela_dados, colWidths=[100, 50])
                    
                    # Estilo da tabela (destaque apenas se houver diferença)
                    estilo_tabela = [
                        ('BACKGROUND', (0, 0), (-1, 0), '#4472C4'),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                        ('ALIGN', (1, 0), (1, -1), 'CENTER'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, 0), 9),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
                        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6')),
                        ('FONTSIZE', (0, 1), (-1, -1), 8),
                    ]
                    
                    # Aplica destaque apenas se houver diferença
                    if tem_diferenca:
                        estilo_tabela.extend([
                            ('BACKGROUND', (0, min_estagio_idx+1), (-1, min_estagio_idx+1), '#fff3bf'),
                            ('TEXTCOLOR', (0, min_estagio_idx+1), (-1, min_estagio_idx+1), '#d63900'),
                            ('FONTNAME', (0, min_estagio_idx+1), (-1, min_estagio_idx+1), 'Helvetica-Bold')
                        ])
                    
                    tabela.setStyle(TableStyle(estilo_tabela))
                    story.append(tabela)
                    
                    # Adicionar observação sobre a habilidade destacada (apenas se houver diferença)
                    if tem_diferenca and min_estagio < 3:  # Se o estágio for baixo e houver diferença
                        story.append(Spacer(1, 10))
                        obs_texto = (f"<font color='#d63900'><b>Atenção:</b> A habilidade <b>'{habilidades_completas[min_estagio_idx]}'</b> "
                                   f"apresenta o menor estágio ({min_estagio}) e pode requerer intervenção pedagógica.</font>")
                        story.append(safe_paragraph(obs_texto, estilo_normal))
                        story.append(Spacer(1, 10))
                    
                    story.append(Spacer(1, 20))
                    
                    # Quebra de página se não for a última unidade
                    if ut_nome != list(unidades_habilidades.keys())[-1]:
                        story.append(PageBreak())
                
                # Quebra de página se não for a última disciplina
                if disciplina_nome != disciplinas[-1][1]:
                    story.append(PageBreak())
        
        # Rodapé
        #data_emissao = datetime.now().strftime("%d/%m/%Y ")
        #rodape = safe_paragraph(f"<i>Relatório gerado em {data_emissao} - Sistema de Avaliação</i>", styles['Italic'])
        #story.append(rodape)

        # Gerar PDF
        doc.build(story)
        buffer.seek(0)

        # Nome do arquivo baseado no tipo de relatório
        if tipo.startswith('aluno'):
            filename = f"relatorio_{aluno_nome}.pdf".replace(" ", "_")
        elif tipo.startswith('turma'):
            filename = f"relatorio_{turma_nome}.pdf".replace(" ", "_")
        else:
            filename = f"relatorio_{escola_nome}.pdf".replace(" ", "_")

        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype='application/pdf'
        )

    except Exception as e:
        app.logger.error(f"Erro ao gerar relatório: {str(e)}", exc_info=True)
        return render_template('error.html', error="Erro ao gerar relatório"), 500
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()







@app.route('/exportar/fundamental')
@admin_required
def exportar_dados_fundamental():
    try:
        escola_id = request.args.get('escola_id', type=int)
        turma_id = request.args.get('turma_id', type=int)
        aluno_matricula = request.args.get('aluno_matricula', type=str)

        conn = get_db_connection()
        cur = conn.cursor()

        # Configura a query base conforme os parâmetros
        if aluno_matricula:
            # Exportação para um aluno específico
            query = """
                SELECT 
                    e.nome AS escola,
                    t.nome AS turma,
                    a.nome AS aluno,
                    d.nome AS disciplina,
                    ut.nome AS unidade_tematica,
                    h.descricao AS habilidade,
                    ah.estagio_numero AS estagio,
                    ah.inicio_avaliacao AS inicio,
                    ah.fim_avaliacao AS fim,
                    p.nome AS avaliador
                FROM avaliacoes_habilidades ah
                JOIN alunos a ON ah.aluno_matricula = a.matricula
                JOIN turmas t ON a.turma_id = t.id
                JOIN escolas e ON t.escola_id = e.id
                JOIN habilidades h ON ah.habilidade_id = h.id
                JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                JOIN disciplinas d ON ut.disciplina_id = d.id
                LEFT JOIN professores p ON ah.avaliador_id = p.id_plurall
                WHERE ah.aluno_matricula = %s
                ORDER BY d.nome, ut.nome, h.descricao
            """
            params = (aluno_matricula,)
            filename = f"aluno_{aluno_matricula}.csv"
            
        elif turma_id:
            # Exportação para uma turma específica
            query = """
                SELECT 
                    e.nome AS escola,
                    t.nome AS turma,
                    a.nome AS aluno,
                    a.matricula,
                    d.nome AS disciplina,
                    ut.nome AS unidade_tematica,
                    h.descricao AS habilidade,
                    ah.estagio_numero AS estagio,
                    ah.inicio_avaliacao AS inicio,
                    ah.fim_avaliacao AS fim,
                    p.nome AS avaliador
                FROM avaliacoes_habilidades ah
                JOIN alunos a ON ah.aluno_matricula = a.matricula
                JOIN turmas t ON a.turma_id = t.id
                JOIN escolas e ON t.escola_id = e.id
                JOIN habilidades h ON ah.habilidade_id = h.id
                JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                JOIN disciplinas d ON ut.disciplina_id = d.id
                LEFT JOIN professores p ON ah.avaliador_id = p.id_plurall
                WHERE t.id = %s
                ORDER BY a.nome, d.nome, ut.nome, h.descricao
            """
            params = (turma_id,)
            filename = f"turma_{turma_id}.csv"
            
        elif escola_id:
            # Exportação para uma escola específica
            query = """
                SELECT 
                    e.nome AS escola,
                    t.nome AS turma,
                    a.nome AS aluno,
                    a.matricula,
                    d.nome AS disciplina,
                    ut.nome AS unidade_tematica,
                    h.descricao AS habilidade,
                    ah.estagio_numero AS estagio,
                    ah.inicio_avaliacao AS inicio,
                    ah.fim_avaliacao AS fim,
                    p.nome AS avaliador
                FROM avaliacoes_habilidades ah
                JOIN alunos a ON ah.aluno_matricula = a.matricula
                JOIN turmas t ON a.turma_id = t.id
                JOIN escolas e ON t.escola_id = e.id
                JOIN habilidades h ON ah.habilidade_id = h.id
                JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                JOIN disciplinas d ON ut.disciplina_id = d.id
                LEFT JOIN professores p ON ah.avaliador_id = p.id_plurall
                WHERE e.id = %s AND t.segmento_id = 2
                ORDER BY t.nome, a.nome, d.nome, ut.nome, h.descricao
            """
            params = (escola_id,)
            filename = f"escola_{escola_id}_fundamental.csv"
            
        else:
            return render_template('error.html', error="Nenhum filtro especificado para exportação"), 400

        cur.execute(query, params)
        dados = cur.fetchall()

        # Gera o CSV em memória
        output = StringIO()
        writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        
        # Escreve cabeçalho
        writer.writerow([
            'Escola', 'Turma', 'Aluno', 'Matrícula', 'Disciplina',
            'Unidade Temática', 'Habilidade', 'Estágio',
            'Início Avaliação', 'Fim Avaliação', 'Avaliador'
        ])
        
        # Escreve os dados
        for row in dados:
            writer.writerow([
                row[0], row[1], row[2], row[3] if len(row) > 3 else '',
                row[4] if len(row) > 4 else '', row[5] if len(row) > 5 else '',
                row[6] if len(row) > 6 else '', row[7] if len(row) > 7 else '',
                row[8].strftime('%d/%m/%Y %H:%M') if row[8] else '',
                row[9].strftime('%d/%m/%Y %H:%M') if row[9] else '',
                row[10] if len(row) > 10 else ''
            ])

        # Retorna o arquivo CSV para download
        output.seek(0)
        return Response(
            output,
            mimetype="text/csv",
            headers={"Content-disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        app.logger.error(f"Erro ao exportar dados: {str(e)}", exc_info=True)
        return render_template('error.html', error="Erro ao exportar dados"), 500
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()







@app.route('/gerar-pdf-escola-turma-fundamental')
@admin_required
def gerar_pdf_escola_turma_fundamental():
    try:
        escola_id = request.args.get('escola_id', type=int)
        turma_id = request.args.get('turma_id', type=int)
        
        if not escola_id and not turma_id:
            return render_template('error.html', error="Nenhuma escola ou turma selecionada"), 400

        # Configurações do PDF
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import numpy as np
        from matplotlib.patches import Patch
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib import colors
        from io import BytesIO
        from datetime import datetime
        import re
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Função para extrair código da habilidade (conteúdo entre parênteses)
        def extrair_codigo_habilidade(descricao):
            match = re.search(r'\((.*?)\)', descricao)
            return match.group(1) if match else descricao
        
        # Buscar informações básicas
        if turma_id:
            cur.execute("""
                SELECT t.nome, e.nome, ans.nome 
                FROM turmas t
                JOIN escolas e ON t.escola_id = e.id
                JOIN ano_series ans ON t.ano_serie_id = ans.id
                WHERE t.id = %s
            """, (turma_id,))
            info = cur.fetchone()
            titulo = f"Turma: {info[0]} - {info[1]}"
            subtitulo = f"Série: {info[2]}"
        else:
            cur.execute("SELECT nome FROM escolas WHERE id = %s", (escola_id,))
            info = cur.fetchone()
            titulo = f"Escola: {info[0]}"
            subtitulo = ""

        # Criar PDF
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter,
                              rightMargin=40, leftMargin=40,
                              topMargin=40, bottomMargin=40)
        styles = getSampleStyleSheet()
        story = []
        
        # Estilos
        estilo_titulo = styles['Title']
        estilo_titulo.alignment = 1
        estilo_normal = styles['Normal']
        estilo_normal.fontName = 'Helvetica'
        estilo_normal.fontSize = 10
        estilo_destaque = styles['Normal']
        estilo_destaque.fontName = 'Helvetica-Bold'
        estilo_destaque.textColor = colors.HexColor("#000000")
        
        # CABEÇALHO COM LOGOS
        logo_somos = 'static/logo_somos.png'
        logo_pref = 'static/prefeitura_sj.png'
        
        cabecalho_tabela = Table([
            [Image(logo_somos, width=60, height=30), 
             Paragraph("<b>Acompanhamento do Desenvolvimento da Aprendizagem - ENSINO FUNDAMENTAL</b>", estilo_titulo), 
             Image(logo_pref, width=60, height=30)]
        ], colWidths=[80, 360, 80])
        
        cabecalho_tabela.setStyle(TableStyle([
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('ALIGN', (1, 0), (1, 0), 'CENTER'),
            ('ALIGN', (2, 0), (2, 0), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ]))
        
        story.append(cabecalho_tabela)
        story.append(Spacer(1, 10))
        
        # Título e subtítulo da escola/turma
        story.append(Paragraph(titulo, estilo_titulo))
        if subtitulo:
            story.append(Paragraph(subtitulo, estilo_normal))
        story.append(Spacer(1, 20))

        # Dados estatísticos gerais
        if turma_id:
            cur.execute("""
                SELECT COUNT(*) FROM alunos WHERE turma_id = %s
            """, (turma_id,))
            total_alunos = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(DISTINCT a.matricula)
                FROM alunos a
                WHERE a.turma_id = %s
                AND EXISTS (
                    SELECT 1 FROM avaliacoes_habilidades WHERE aluno_matricula = a.matricula
                )
            """, (turma_id,))
            alunos_avaliados = cur.fetchone()[0]
        else:
            cur.execute("""
                SELECT COUNT(DISTINCT a.matricula)
                FROM alunos a
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.escola_id = %s AND t.segmento_id = 2
            """, (escola_id,))
            total_alunos = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(DISTINCT a.matricula)
                FROM alunos a
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.escola_id = %s AND t.segmento_id = 2
                AND EXISTS (
                    SELECT 1 FROM avaliacoes_habilidades WHERE aluno_matricula = a.matricula
                )
            """, (escola_id,))
            alunos_avaliados = cur.fetchone()[0]
        
        progresso = (alunos_avaliados / total_alunos * 100) if total_alunos > 0 else 0
        
        # Adicionar estatísticas ao PDF
        story.append(Paragraph("<b>DADOS GERAIS</b>", estilo_destaque))
        story.append(Spacer(1, 10))
        
        dados_gerais = [
            ["Total de Alunos", total_alunos],
            ["Alunos Avaliados", alunos_avaliados],
            ["Progresso", f"{progresso:.1f}%"]
        ]
        
        tabela_geral = Table(dados_gerais, colWidths=[150, 100])
        tabela_geral.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), '#4472C4'),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6'))
        ]))
        story.append(tabela_geral)
        story.append(Spacer(1, 20))

        # Buscar todas as disciplinas avaliadas
        if turma_id:
            cur.execute("""
                SELECT DISTINCT d.nome
                FROM avaliacoes_habilidades ah
                JOIN habilidades h ON ah.habilidade_id = h.id
                JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                JOIN disciplinas d ON ut.disciplina_id = d.id
                JOIN alunos a ON ah.aluno_matricula = a.matricula
                WHERE a.turma_id = %s
                ORDER BY d.nome
            """, (turma_id,))
        else:
            cur.execute("""
                SELECT DISTINCT d.nome
                FROM avaliacoes_habilidades ah
                JOIN habilidades h ON ah.habilidade_id = h.id
                JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                JOIN disciplinas d ON ut.disciplina_id = d.id
                JOIN alunos a ON ah.aluno_matricula = a.matricula
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.escola_id = %s AND t.segmento_id = 2
                ORDER BY d.nome
            """, (escola_id,))
        
        disciplinas = [row[0] for row in cur.fetchall()]
        
        if not disciplinas:
            story.append(Paragraph("Nenhuma avaliação encontrada para esta turma/escola.", estilo_normal))
            story.append(Spacer(1, 20))
        else:
            # Gráfico Radar (média por disciplina)
            if turma_id:
                cur.execute("""
                    SELECT 
                        d.nome,
                        AVG(ah.estagio_numero),
                        COUNT(ah.estagio_numero)
                    FROM avaliacoes_habilidades ah
                    JOIN habilidades h ON ah.habilidade_id = h.id
                    JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                    JOIN disciplinas d ON ut.disciplina_id = d.id
                    JOIN alunos a ON ah.aluno_matricula = a.matricula
                    WHERE a.turma_id = %s
                    GROUP BY d.nome
                    ORDER BY d.nome
                """, (turma_id,))
            else:
                cur.execute("""
                    SELECT 
                        d.nome,
                        AVG(ah.estagio_numero),
                        COUNT(ah.estagio_numero)
                    FROM avaliacoes_habilidades ah
                    JOIN habilidades h ON ah.habilidade_id = h.id
                    JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                    JOIN disciplinas d ON ut.disciplina_id = d.id
                    JOIN alunos a ON ah.aluno_matricula = a.matricula
                    JOIN turmas t ON a.turma_id = t.id
                    WHERE t.escola_id = %s AND t.segmento_id = 2
                    GROUP BY d.nome
                    ORDER BY d.nome
                """, (escola_id,))
            
            dados_radar = cur.fetchall()
            disciplinas_radar = [row[0] for row in dados_radar]
            medias = [float(row[1]) for row in dados_radar]
            
            if disciplinas_radar:
                fig = plt.figure(figsize=(8, 8))
                ax = fig.add_subplot(111, polar=True)
                
                N = len(disciplinas_radar)
                angles = [n / float(N) * 2 * np.pi for n in range(N)]
                angles += angles[:1]
                valores = medias + [medias[0]]
                
                ax.plot(angles, valores, linewidth=2, color='#36a2eb')
                ax.fill(angles, valores, alpha=0.25, color='#36a2eb')
                ax.set_theta_offset(np.pi / 2)
                ax.set_theta_direction(-1)
                ax.set_thetagrids(np.degrees(angles[:-1]), labels=disciplinas_radar)
                
                for label, angle in zip(ax.get_xticklabels(), angles[:-1]):
                    label.set_horizontalalignment('center')
                    angle_deg = angle * 180/np.pi
                    if angle_deg > 90:
                        angle_deg -= 180
                    label.set_rotation(angle_deg)
                    label.set_rotation_mode('anchor')
                
                ax.set_ylim(0, 5)
                ax.set_yticks(range(0, 6))
                ax.set_yticklabels([str(i) for i in range(0, 6)])
                
                ax.grid(color=(0, 0, 0, 0.1), linestyle='-', linewidth=0.5)
                ax.set_facecolor('white')
                ax.spines['polar'].set_visible(False)
                ax.set_title('Média por Disciplina', pad=20)
                
                img_buffer = BytesIO()
                plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', facecolor='white')
                plt.close(fig)
                
                story.append(Paragraph("<b>Acompanhamento por componente curricular</b>", estilo_destaque))
                story.append(Spacer(1, 10))
                img_buffer.seek(0)
                img = Image(img_buffer)
                img.drawHeight = 300
                img.drawWidth = img.drawHeight * (1.0 * img.imageWidth / img.imageHeight)
                story.append(img)
                story.append(Spacer(1, 20))
                story.append(PageBreak())

            # Processar cada disciplina separadamente
            for disciplina in disciplinas:
                story.append(Paragraph(f"<b>Componente curricular: {disciplina}</b>", estilo_titulo))
                story.append(Spacer(1, 10))
                
                # Buscar unidades temáticas desta disciplina
                if turma_id:
                    cur.execute("""
                        SELECT 
                            ut.nome,
                            AVG(ah.estagio_numero),
                            COUNT(ah.estagio_numero)
                        FROM avaliacoes_habilidades ah
                        JOIN habilidades h ON ah.habilidade_id = h.id
                        JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                        JOIN disciplinas d ON ut.disciplina_id = d.id
                        JOIN alunos a ON ah.aluno_matricula = a.matricula
                        WHERE a.turma_id = %s AND d.nome = %s
                        GROUP BY ut.nome
                        ORDER BY ut.nome
                    """, (turma_id, disciplina))
                else:
                    cur.execute("""
                        SELECT 
                            ut.nome,
                            AVG(ah.estagio_numero),
                            COUNT(ah.estagio_numero)
                        FROM avaliacoes_habilidades ah
                        JOIN habilidades h ON ah.habilidade_id = h.id
                        JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                        JOIN disciplinas d ON ut.disciplina_id = d.id
                        JOIN alunos a ON ah.aluno_matricula = a.matricula
                        JOIN turmas t ON a.turma_id = t.id
                        WHERE t.escola_id = %s AND t.segmento_id = 2 AND d.nome = %s
                        GROUP BY ut.nome
                        ORDER BY ut.nome
                    """, (escola_id, disciplina))
                
                unidades_tematicas = cur.fetchall()
                
                # Preparar dados para o gráfico de barras (média por unidade temática)
                unidades_nomes = [ut[0] for ut in unidades_tematicas]
                unidades_medias = [float(ut[1]) for ut in unidades_tematicas]
                unidades_contagens = [ut[2] for ut in unidades_tematicas]
                
                # Criar gráfico de barras para a disciplina
                fig, ax = plt.subplots(figsize=(10, 6))
                
                # Definir cores baseadas na média (mesma paleta do infantil)
                cores = []
                for media in unidades_medias:
                    if media < 1.5:
                        cores.append((1.0, 0.5, 0.5))  # Vermelho claro
                    elif media < 2.5:
                        cores.append((1.0, 0.8, 0.6))  # Laranja claro
                    elif media < 3.5:
                        cores.append((0.9, 0.9, 0.5))  # Amarelo
                    elif media < 4.5:
                        cores.append((0.6, 0.9, 0.6))  # Verde claro
                    else:
                        cores.append((0.5, 0.8, 0.5))  # Verde
                
                barras = ax.bar(unidades_nomes, unidades_medias, color=cores)
                
                # Adicionar valores nas barras
                for bar, media in zip(barras, unidades_medias):
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                            f'{media:.1f}',
                            ha='center', va='bottom',
                            fontsize=10)
                
                # Configurar eixos e título
                ax.set_ylabel('Média dos Estágios')
                ax.set_title(f'Média por Unidade Temática - {disciplina}')
                plt.xticks(rotation=45, ha='right')
                plt.ylim(0, 5)
                plt.tight_layout()
                
                # Salvar gráfico
                img_buffer = BytesIO()
                plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight')
                plt.close(fig)
                
                # Adicionar gráfico ao PDF
                img_buffer.seek(0)
                img = Image(img_buffer)
                img.drawHeight = 250
                img.drawWidth = min(500, img.drawHeight * (1.0 * img.imageWidth / img.imageHeight))
                story.append(img)
                story.append(Spacer(1, 15))
                
                # Tabela com os dados
                tabela_dados = [["Unidade Temática", "Média"]]
                for nome, media in zip(unidades_nomes, unidades_medias):
                    tabela_dados.append([nome, f"{media:.2f}"])
                
                tabela = Table(tabela_dados, colWidths=[300, 80])
                tabela.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), '#4472C4'),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
                    ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6')),
                ]))
                story.append(tabela)
                story.append(Spacer(1, 20))
                story.append(PageBreak())
                # Detalhamento por unidade temática
                for i, unidade in enumerate(unidades_nomes):
                    
                    story.append(Paragraph(f"<b>Unidade Temática: {unidade}</b>", estilo_destaque))
                    story.append(Spacer(1, 10))
                    
                    # Consulta para habilidades e distribuição por estágio
                    if turma_id:
                        cur.execute("""
                            SELECT 
                                h.descricao,
                                ah.estagio_numero,
                                COUNT(*) as total
                            FROM avaliacoes_habilidades ah
                            JOIN habilidades h ON ah.habilidade_id = h.id
                            JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                            JOIN disciplinas d ON ut.disciplina_id = d.id
                            JOIN alunos a ON ah.aluno_matricula = a.matricula
                            WHERE a.turma_id = %s AND d.nome = %s AND ut.nome = %s
                            GROUP BY h.descricao, ah.estagio_numero
                            ORDER BY h.descricao, ah.estagio_numero
                        """, (turma_id, disciplina, unidade))
                    else:
                        cur.execute("""
                            SELECT 
                                h.descricao,
                                ah.estagio_numero,
                                COUNT(*) as total
                            FROM avaliacoes_habilidades ah
                            JOIN habilidades h ON ah.habilidade_id = h.id
                            JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                            JOIN disciplinas d ON ut.disciplina_id = d.id
                            JOIN alunos a ON ah.aluno_matricula = a.matricula
                            JOIN turmas t ON a.turma_id = t.id
                            WHERE t.escola_id = %s AND t.segmento_id = 2 
                            AND d.nome = %s AND ut.nome = %s
                            GROUP BY h.descricao, ah.estagio_numero
                            ORDER BY h.descricao, ah.estagio_numero
                        """, (escola_id, disciplina, unidade))
                    
                    dados_habilidades = cur.fetchall()
                    
                    # Organizar dados por habilidade
                    habilidades_data = {}
                    for descricao, estagio, total in dados_habilidades:
                        if descricao not in habilidades_data:
                            habilidades_data[descricao] = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
                        habilidades_data[descricao][estagio] = total
                    
                    if not habilidades_data:
                        story.append(Paragraph("Nenhuma avaliação encontrada para esta unidade temática.", estilo_normal))
                        story.append(Spacer(1, 20))
                        continue
                    
                    # Identificar habilidade com maior dificuldade (mesmo critério do infantil)
                    habilidade_destaque = None
                    max_concentracao = 0
                    menor_estagio = float('inf')
                    
                    # Só vamos destacar se houver mais de uma habilidade
                    if len(habilidades_data) > 1:
                        for habilidade, dados in habilidades_data.items():
                            total_habilidade = sum(dados.values())
                            if total_habilidade == 0:
                                continue
                            
                            # Calcular concentração nos estágios mais baixos (1 e 2)
                            concentracao = (dados[1] + dados[2]) / total_habilidade
                            
                            # Encontrar o estágio mais baixo com alunos
                            estagios_com_alunos = [e for e in dados if dados[e] > 0]
                            min_estagio = min(estagios_com_alunos) if estagios_com_alunos else float('inf')
                            
                            # Critério: maior concentração nos estágios baixos
                            # Em caso de empate, menor estágio com alunos
                            if (concentracao > max_concentracao) or \
                               (concentracao == max_concentracao and min_estagio < menor_estagio):
                                max_concentracao = concentracao
                                habilidade_destaque = habilidade
                                menor_estagio = min_estagio
                    
                    # Preparar dados para gráfico
                    habilidades_codigos = [extrair_codigo_habilidade(h) for h in habilidades_data.keys()]
                    habilidades_completas = list(habilidades_data.keys())
                    
                    # Cores para os estágios (mesma paleta do infantil)
                    cores_estagios = ['#ff6b6b', '#ffa3a3', '#a3d8ff', '#4da6ff', '#36a2eb']
                    
                    # Criar gráfico de barras verticais
                    fig, ax = plt.subplots(figsize=(10, 6))
                    
                    # Preparar dados para gráfico de barras empilhadas
                    bottom = np.zeros(len(habilidades_codigos))
                    for estagio in range(1, 6):
                        valores = [habilidades_data[hab].get(estagio, 0) / sum(habilidades_data[hab].values()) * 100 
                                  if sum(habilidades_data[hab].values()) > 0 else 0 
                                  for hab in habilidades_completas]
                        
                        barras = ax.bar(habilidades_codigos, valores, bottom=bottom, 
                                       color=cores_estagios[estagio-1], 
                                       label=f'Estágio {estagio}',
                                       width=0.6)
                        
                        # Aplicar destaque no gráfico (apenas se houver mais de uma habilidade)
                        if habilidade_destaque and len(habilidades_data) > 1:
                            for j, bar in enumerate(barras):
                                if habilidades_completas[j] == habilidade_destaque:
                                    bar.set_edgecolor('#d63900')
                                    bar.set_linewidth(2)
                        
                        # Adicionar valores nas barras
                        for bar in barras:
                            height = bar.get_height()
                            if height > 5:  # Só mostra porcentagem se for maior que 5%
                                ax.text(bar.get_x() + bar.get_width()/2., 
                                        bar.get_y() + height/2,
                                        f'{height:.0f}%', 
                                        ha='center', va='center',
                                        color='white' if estagio <= 2 else 'black',
                                        fontsize=8)
                        
                        bottom += valores
                    
                    # Configurações do gráfico
                    ax.set_ylabel('Porcentagem de Alunos')
                    ax.set_title(f'Distribuição por Estágio - {unidade}')
                    ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1))
                    plt.xticks(rotation=45, ha='right')
                    plt.ylim(0, 100)
                    plt.tight_layout()
                    
                    # Salvar gráfico
                    img_buffer = BytesIO()
                    plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight')
                    plt.close(fig)
                    
                    # Adicionar mensagem sobre a habilidade destacada
                    if habilidade_destaque and len(habilidades_data) > 1:
                        story.append(Paragraph(
                            f"<font color='#d63900'><b>Habilidade a desenvolver:</b> {habilidade_destaque}</font>", 
                            estilo_normal
                        ))
                        story.append(Spacer(1, 10))
                    
                    # Adicionar gráfico ao PDF
                    img_buffer.seek(0)
                    img = Image(img_buffer)
                    img.drawHeight = 300
                    img.drawWidth = min(500, img.drawHeight * (1.0 * img.imageWidth / img.imageHeight))
                    story.append(img)
                    story.append(Spacer(1, 15))
                    
                    # Tabela com os dados
                    tabela_dados = [["Código", "Estágio 1", "Estágio 2", "Estágio 3", "Estágio 4", "Estágio 5"]]

                    for hab_cod, hab_comp in zip(habilidades_codigos, habilidades_completas):
                        dados = habilidades_data[hab_comp]
                        total = sum(dados.values())
                        linha = [hab_cod]
                        for estagio in range(1, 6):
                            porcentagem = (dados.get(estagio, 0) / total * 100) if total > 0 else 0
                            linha.append(f"{porcentagem:.1f}%")
                        tabela_dados.append(linha)

                    # Criar tabela
                    tabela = Table(tabela_dados, colWidths=[80] + [60]*5)

                    # Estilo da tabela
                    estilo = [
                        ('BACKGROUND', (0, 0), (-1, 0), '#4472C4'),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, -1), 8),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
                        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6')),
                    ]

                    
                    # Aplicar destaque na tabela (apenas se houver mais de uma habilidade)
                    if habilidade_destaque and len(habilidades_data) > 1:
                        idx_destaque = habilidades_completas.index(habilidade_destaque)
                        estilo.extend([
                            ('BACKGROUND', (0, idx_destaque+1), (-1, idx_destaque+1), '#fff3bf'),
                            ('TEXTCOLOR', (0, idx_destaque+1), (-1, idx_destaque+1), '#d63900'),
                            ('FONTNAME', (0, idx_destaque+1), (-1, idx_destaque+1), 'Helvetica-Bold')
                        ])
                    
                    tabela.setStyle(TableStyle(estilo))
                    story.append(tabela)
                    story.append(Spacer(1, 20))
                    # Inserir quebra de página antes de cada unidade, exceto a primeira
                    story.append(PageBreak())
                        
        
        # Rodapé
        #data_emissao = datetime.now().strftime("%d/%m/%Y")
        #rodape = Paragraph(f"<i>Relatório gerado em {data_emissao} - Sistema de Avaliação</i>", styles['Italic'])
        #story.append(rodape)

        doc.build(story)
        buffer.seek(0)

        nome_arquivo = f"relatorio_{'turma' if turma_id else 'escola'}_{info[0].replace(' ', '_')}.pdf"
        return send_file(
            buffer,
            as_attachment=True,
            download_name=nome_arquivo,
            mimetype='application/pdf'
        )
        
    except Exception as e:
        app.logger.error(f"Erro ao gerar PDF escola/turma (Fundamental): {str(e)}", exc_info=True)
        return render_template('error.html', error="Erro ao gerar relatório"), 500
    finally:
        if 'conn' in locals():
            conn.close()




@app.route('/gerar-pdf-por-ano-serie')
@admin_required
def gerar_pdf_por_ano_serie():
    try:
        ano_serie_id = request.args.get('ano_serie_id', type=int)
        
        if not ano_serie_id:
            return render_template('error.html', error="Nenhum ano/série selecionado"), 400

        # Configurações do PDF
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import numpy as np
        from matplotlib.patches import Patch
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib import colors
        from io import BytesIO
        from datetime import datetime
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Buscar informações básicas do ano/série
        cur.execute("""
            SELECT ans.nome 
            FROM ano_series ans
            WHERE ans.id = %s
        """, (ano_serie_id,))
        ano_serie_nome = cur.fetchone()[0]
        titulo = f"Relatório Consolidado - {ano_serie_nome}"
        subtitulo = "Ensino Infantil"  # Definido como Infantil conforme sua informação

        # Buscar turmas deste ano/série
        cur.execute("""
            SELECT t.id, t.nome, e.nome
            FROM turmas t
            JOIN escolas e ON t.escola_id = e.id
            WHERE t.ano_serie_id = %s
            ORDER BY e.nome, t.nome
        """, (ano_serie_id,))
        turmas = cur.fetchall()

        # Criar PDF
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter,
                              rightMargin=40, leftMargin=40,
                              topMargin=40, bottomMargin=40)
        styles = getSampleStyleSheet()
        story = []
        
        # Estilos
        estilo_titulo = styles['Title']
        estilo_titulo.alignment = 1
        estilo_normal = styles['Normal']
        estilo_normal.fontName = 'Helvetica'
        estilo_normal.fontSize = 10
        estilo_destaque = styles['Normal']
        estilo_destaque.fontName = 'Helvetica-Bold'
        estilo_destaque.textColor = colors.HexColor("#000000")
        
        # CABEÇALHO COM LOGOS
        logo_somos = 'static/logo_somos.png'
        logo_pref = 'static/prefeitura_sj.png'
        
        cabecalho_tabela = Table([
            [Image(logo_somos, width=60, height=30), 
             Paragraph("<b>Acompanhamento do Desenvolvimento da Aprendizagem</b>", estilo_titulo), 
             Image(logo_pref, width=60, height=30)]
        ], colWidths=[80, 360, 80])
        
        cabecalho_tabela.setStyle(TableStyle([
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('ALIGN', (1, 0), (1, 0), 'CENTER'),
            ('ALIGN', (2, 0), (2, 0), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ]))
        
        story.append(cabecalho_tabela)
        story.append(Spacer(1, 10))
        
        # Título e subtítulo
        story.append(Paragraph(titulo, estilo_titulo))
        story.append(Paragraph(subtitulo, estilo_normal))
        story.append(Spacer(1, 20))

        # Dados estatísticos gerais
        cur.execute("""
            SELECT COUNT(DISTINCT a.matricula)
            FROM alunos a
            JOIN turmas t ON a.turma_id = t.id
            WHERE t.ano_serie_id = %s
        """, (ano_serie_id,))
        total_alunos = cur.fetchone()[0]
        
        # Como é infantil, usamos sempre avaliacoes_direitos
        cur.execute("""
            SELECT COUNT(DISTINCT a.matricula)
            FROM alunos a
            JOIN turmas t ON a.turma_id = t.id
            WHERE t.ano_serie_id = %s
            AND EXISTS (
                SELECT 1 FROM avaliacoes_direitos WHERE aluno_matricula = a.matricula
            )
        """, (ano_serie_id,))
        alunos_avaliados = cur.fetchone()[0]
        
        progresso = (alunos_avaliados / total_alunos * 100) if total_alunos > 0 else 0
        
        # Adicionar estatísticas ao PDF
        story.append(Paragraph("<b>DADOS GERAIS</b>", estilo_destaque))
        story.append(Spacer(1, 10))
        
        dados_gerais = [
            ["Total de Alunos", total_alunos],
            ["Alunos Avaliados", alunos_avaliados],
            ["Progresso", f"{progresso:.1f}%"],
            ["Total de Turmas", len(turmas)]
        ]
        
        tabela_geral = Table(dados_gerais, colWidths=[150, 100])
        tabela_geral.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), '#4472C4'),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6'))
        ]))
        story.append(tabela_geral)
        story.append(Spacer(1, 20))

        # Lista de turmas
        story.append(Paragraph("<b>TURMAS INCLUÍDAS</b>", estilo_destaque))
        story.append(Spacer(1, 10))
        
        dados_turmas = [["Turma", "Escola"]]
        for turma in turmas:
            dados_turmas.append([turma[1], turma[2]])
        
        tabela_turmas = Table(dados_turmas, colWidths=[200, 300])
        tabela_turmas.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), '#4472C4'),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6'))
        ]))
        story.append(tabela_turmas)
        story.append(Spacer(1, 20))
        story.append(PageBreak())

        # Gráfico Radar (média por campo de experiência)
        cur.execute("""
            SELECT 
                ce.nome,
                AVG(ad.estagio_numero),
                COUNT(ad.estagio_numero)
            FROM avaliacoes_direitos ad
            JOIN direitos_aprendizagem d ON ad.direito_id = d.id
            JOIN campos_experiencia ce ON d.campo_experiencia_id = ce.id
            JOIN alunos a ON ad.aluno_matricula = a.matricula
            JOIN turmas t ON a.turma_id = t.id
            WHERE t.ano_serie_id = %s
            GROUP BY ce.nome
            ORDER BY ce.nome
        """, (ano_serie_id,))
        
        dados_radar = cur.fetchall()
        campos = [row[0] for row in dados_radar]
        medias = [float(row[1]) for row in dados_radar]
        
        if campos:
            is_primeira_escrita = "Primeira Escrita" in campos
            
            fig = plt.figure(figsize=(8, 8))
            ax = fig.add_subplot(111, polar=True)
            
            N = len(campos)
            angles = [n / float(N) * 2 * np.pi for n in range(N)]
            angles += angles[:1]
            valores = medias + [medias[0]]
            
            ax.plot(angles, valores, linewidth=2, color='#36a2eb')
            ax.fill(angles, valores, alpha=0.25, color='#36a2eb')
            ax.set_theta_offset(np.pi / 2)
            ax.set_theta_direction(-1)
            ax.set_thetagrids(np.degrees(angles[:-1]), labels=campos)
            
            for label, angle in zip(ax.get_xticklabels(), angles[:-1]):
                label.set_horizontalalignment('center')
                angle_deg = angle * 180/np.pi
                if angle_deg > 90:
                    angle_deg -= 180
                label.set_rotation(angle_deg)
                label.set_rotation_mode('anchor')
            
            if is_primeira_escrita:
                ax.set_ylim(0, 8)
                ax.set_yticks(range(0, 9))
                ax.set_yticklabels([str(i) for i in range(0, 9)])
            else:
                ax.set_ylim(0, 5)
                ax.set_yticks(range(0, 6))
                ax.set_yticklabels([str(i) for i in range(0, 6)])
            
            ax.grid(color=(0, 0, 0, 0.1), linestyle='-', linewidth=0.5)
            ax.set_facecolor('white')
            ax.spines['polar'].set_visible(False)
            ax.set_title('Média por Campo de Experiência', pad=20)
            
            img_buffer = BytesIO()
            plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', facecolor='white')
            plt.close(fig)
            
            story.append(Paragraph("<b>Desenvolvimento da Aprendizagem dos Campos de Experiência</b>", estilo_destaque))
            story.append(Spacer(1, 10))
            img_buffer.seek(0)
            img = Image(img_buffer)
            img.drawHeight = 300
            img.drawWidth = img.drawHeight * (1.0 * img.imageWidth / img.imageHeight)
            story.append(img)
            story.append(Spacer(1, 20))
            
            # Adicionar legenda sobre o destaque
            story.append(Paragraph(
                "<i>Nota: Será destacado em amarelo o direito com maior em desenvovimento (maior concentração nos estágios iniciais)</i>", 
                styles['Italic']
            ))
            story.append(Spacer(1, 15))
            story.append(PageBreak())
        
        # Detalhamento por campo de experiência
        for i, campo in enumerate(campos):
            is_primeira_escrita = campo == "Primeira Escrita"
            
            # Título do Campo
            story.append(Paragraph(f"<b>CAMPO DE EXPERIÊNCIA: {campo}</b>", estilo_titulo))
            
            # Consulta SQL para obter os dados do campo
            cur.execute("""
                SELECT 
                    v.nome as verbo,
                    d.descricao as direito,
                    ad.estagio_numero,
                    COUNT(*) as total
                FROM avaliacoes_direitos ad
                JOIN direitos_aprendizagem d ON ad.direito_id = d.id
                JOIN verbos v ON d.verbo_id = v.id
                JOIN campos_experiencia ce ON d.campo_experiencia_id = ce.id
                JOIN alunos a ON ad.aluno_matricula = a.matricula
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.ano_serie_id = %s AND ce.nome = %s
                GROUP BY v.nome, d.descricao, ad.estagio_numero
                ORDER BY v.nome, d.descricao, ad.estagio_numero
            """, (ano_serie_id, campo))
            
            dados_campo = cur.fetchall()
            
            verbos_data = {}
            direitos_por_verbo = {}
            for verbo, direito, estagio, total in dados_campo:
                if verbo not in verbos_data:
                    if is_primeira_escrita:
                        verbos_data[verbo] = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0}
                    else:
                        verbos_data[verbo] = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
                    direitos_por_verbo[verbo] = direito
                verbos_data[verbo][estagio] += total
            
            verbos = sorted(verbos_data.keys())
            
            # Identificar o verbo com maior dificuldade (APENAS para campos que não são Primeira Escrita)
            verbo_destaque = None
            if not is_primeira_escrita:
                max_concentracao = 0
                menor_estagio = float('inf')
                
                for verbo in verbos:
                    total_verbo = sum(verbos_data[verbo].values())
                    if total_verbo == 0:
                        continue
                    
                    concentracao = (verbos_data[verbo][1] + verbos_data[verbo][2]) / total_verbo
                    estagios_com_alunos = [e for e in verbos_data[verbo] if verbos_data[verbo][e] > 0]
                    min_estagio = min(estagios_com_alunos) if estagios_com_alunos else float('inf')
                    
                    if (concentracao > max_concentracao) or \
                    (concentracao == max_concentracao and min_estagio < menor_estagio):
                        max_concentracao = concentracao
                        verbo_destaque = verbo
                        menor_estagio = min_estagio
            
            # Preparar dados para gráfico
            dados_grafico = []
            for verbo in verbos:
                total_verbo = sum(verbos_data[verbo].values())
                if total_verbo == 0:
                    continue
                
                if is_primeira_escrita:
                    porcentagens = [
                        (verbos_data[verbo][1] / total_verbo * 100),
                        (verbos_data[verbo][2] / total_verbo * 100),
                        (verbos_data[verbo][3] / total_verbo * 100),
                        (verbos_data[verbo][4] / total_verbo * 100),
                        (verbos_data[verbo][5] / total_verbo * 100),
                        (verbos_data[verbo][6] / total_verbo * 100),
                        (verbos_data[verbo][7] / total_verbo * 100),
                        (verbos_data[verbo][8] / total_verbo * 100)
                    ]
                else:
                    porcentagens = [
                        (verbos_data[verbo][1] / total_verbo * 100),
                        (verbos_data[verbo][2] / total_verbo * 100),
                        (verbos_data[verbo][3] / total_verbo * 100),
                        (verbos_data[verbo][4] / total_verbo * 100),
                        (verbos_data[verbo][5] / total_verbo * 100)
                    ]
                dados_grafico.append({
                    'verbo': verbo,
                    'porcentagens': porcentagens,
                    'total': total_verbo,
                    'destaque': verbo == verbo_destaque,
                    'direito': direitos_por_verbo.get(verbo, '')
                })
            
            if not dados_grafico:
                continue
            
            # Criar gráfico de barras
            fig, ax = plt.subplots(figsize=(10, 6))
            
            if is_primeira_escrita:
                cores = ['#ff6b6b', '#ff8f8f', '#ffb3b3', '#ffd7d7', '#d7e3ff', '#a3c8ff', '#6ea6ff', '#36a2eb']
                labels_estagios = [f'Estágio {i}' for i in range(1, 9)]
            else:
                cores = ['#ff6b6b', '#ffa3a3', '#a3d8ff', '#4da6ff', '#36a2eb']
                labels_estagios = [f'Estágio {i}' for i in range(1, 6)]
            
            bottom = np.zeros(len(dados_grafico))
            for i in range(len(cores)):
                valores = [d['porcentagens'][i] for d in dados_grafico]
                barras = ax.bar(
                    [d['verbo'] for d in dados_grafico],
                    valores,
                    bottom=bottom,
                    color=cores[i],
                    label=labels_estagios[i],
                    edgecolor='white'
                )
                
                # Aplicar destaque no gráfico (apenas para campos que não são Primeira Escrita)
                if verbo_destaque and not is_primeira_escrita:
                    for j, bar in enumerate(barras):
                        if dados_grafico[j]['destaque']:
                            bar.set_edgecolor('#d63900')
                            bar.set_linewidth(2)
                
                for bar in barras:
                    height = bar.get_height()
                    if height > 10:
                        ax.text(
                            bar.get_x() + bar.get_width() / 2,
                            bar.get_y() + height / 2,
                            f'{height:.0f}%',
                            ha='center',
                            va='center',
                            color='white' if i < 2 else 'black',
                            fontsize=8
                        )
                
                bottom += valores
            
            ax.set_ylabel('Porcentagem de Alunos')
            ax.set_title(f'Distribuição por Estágio - {campo}')
            ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1))
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            
            img_buffer = BytesIO()
            plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            
            # Adicionar mensagem de dificuldade (apenas para campos que não são Primeira Escrita)
            if verbo_destaque and not is_primeira_escrita:
                direito_destaque = direitos_por_verbo.get(verbo_destaque, '')
                story.append(Paragraph(
                    f"<font color='#003366'><b>Em desenvovimento: {verbo_destaque} ({direito_destaque}) - " +
                    f"{max_concentracao:.1%} dos alunos nos estágios iniciais</b></font>", 
                    estilo_normal
                ))
            
            story.append(Spacer(1, 10))
            img_buffer.seek(0)
            img = Image(img_buffer)
            img.drawHeight = 300
            img.drawWidth = min(500, img.drawHeight * (1.0 * img.imageWidth / img.imageHeight))
            story.append(img)
            story.append(Spacer(1, 15))
            
            # Tabela com os dados
            if is_primeira_escrita:
                headers = ["Verbo"] + [f'Estágio {i}' for i in range(1, 9)] 
                col_widths = [160] + [50]*8  # Larguras ajustadas
            else:
                headers = ["Verbo"] + [f'Estágio {i}' for i in range(1, 6)] 
                col_widths = [160] + [50]*5  # Larguras ajustadas
            
            tabela_dados = [headers]
            for dado in dados_grafico:
                linha = [dado['verbo']]
                linha.extend([f"{p:.1f}%" for p in dado['porcentagens']])
                tabela_dados.append(linha)
            
            tabela = Table(tabela_dados, colWidths=col_widths)
            estilo = [
                ('BACKGROUND', (0, 0), (-1, 0), '#4472C4'),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
                ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6')),
            ]
            
            # Aplicar destaque na tabela (apenas para campos que não são Primeira Escrita)
            if verbo_destaque and not is_primeira_escrita:
                for idx, linha in enumerate(tabela_dados[1:], start=1):
                    if linha[0] == verbo_destaque:
                        estilo.extend([
                            ('BACKGROUND', (0, idx), (-1, idx), '#fff3bf'),
                            ('TEXTCOLOR', (0, idx), (-1, idx), '#d63900'),
                            ('FONTNAME', (0, idx), (-1, idx), 'Helvetica-Bold')
                        ])
                        break
            
            tabela.setStyle(TableStyle(estilo))
            story.append(tabela)
            story.append(PageBreak())
        
        # Rodapé
        data_emissao = datetime.now().strftime("%d/%m/%Y")
        rodape = Paragraph(f"<i>Relatório gerado em {data_emissao} - Sistema de Avaliação</i>", styles['Italic'])
        story.append(rodape)

        doc.build(story)
        buffer.seek(0)

        nome_arquivo = f"relatorio_consolidado_{ano_serie_nome.lower().replace(' ', '_')}.pdf"
        return send_file(
            buffer,
            as_attachment=True,
            download_name=nome_arquivo,
            mimetype='application/pdf'
        )
        
    except Exception as e:
        app.logger.error(f"Erro ao gerar PDF por ano/série: {str(e)}", exc_info=True)
        return render_template('error.html', error="Erro ao gerar relatório"), 500
    finally:
        if 'conn' in locals():
            conn.close()




@app.route('/gerar-pdf-todas-escolas-infantil')
@admin_required
def gerar_pdf_todas_escolas_infantil():
    try:
        # Configurações do PDF
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import numpy as np
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib import colors
        from io import BytesIO
        from datetime import datetime
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Buscar todas as escolas que têm turmas do infantil
        cur.execute("""
            SELECT DISTINCT e.id, e.nome 
            FROM escolas e
            JOIN turmas t ON e.id = t.escola_id
            WHERE t.segmento_id = 1 OR EXISTS (
                SELECT 1 FROM serie_faixa_etaria sfe
                JOIN ano_series ans ON sfe.serie_id = ans.id
                WHERE ans.id = t.ano_serie_id AND sfe.faixa_etaria_id = 1
            )
            ORDER BY e.nome
        """)
        escolas = cur.fetchall()
        
        if not escolas:
            return render_template('error.html', error="Nenhuma escola com turmas do infantil encontrada"), 400

        # Criar PDF
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter,
                              rightMargin=40, leftMargin=40,
                              topMargin=40, bottomMargin=40)
        styles = getSampleStyleSheet()
        story = []
        
        # Estilos
        estilo_titulo = styles['Title']
        estilo_titulo.alignment = 1
        estilo_normal = styles['Normal']
        estilo_normal.fontName = 'Helvetica'
        estilo_normal.fontSize = 10
        estilo_destaque = styles['Normal']
        estilo_destaque.fontName = 'Helvetica-Bold'
        estilo_destaque.textColor = colors.HexColor("#000000")
        
        # CABEÇALHO COM LOGOS
        logo_somos = 'static/logo_somos.png'
        logo_pref = 'static/prefeitura_sj.png'
        
        cabecalho_tabela = Table([
            [Image(logo_somos, width=60, height=30), 
             Paragraph("<b>Acompanhamento do Desenvolvimento da Aprendizagem</b>", estilo_titulo), 
             Image(logo_pref, width=60, height=30)]
        ], colWidths=[80, 360, 80])
        
        cabecalho_tabela.setStyle(TableStyle([
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('ALIGN', (1, 0), (1, 0), 'CENTER'),
            ('ALIGN', (2, 0), (2, 0), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ]))
        
        story.append(cabecalho_tabela)
        story.append(Spacer(1, 10))
        
        # Título e data
        #story.append(Paragraph("<b>RELATÓRIO REDE - EDUCAÇÃO INFANTIL</b>", estilo_titulo))
        #data_emissao = datetime.now().strftime("%d/%m/%Y")
        #story.append(Paragraph(f"<i>Data de emissão: {data_emissao}</i>", styles['Italic']))
        #story.append(Spacer(1, 20))

        # Resumo geral
        story.append(Paragraph("<b>DADOS GERAIS</b>", estilo_destaque))
        story.append(Spacer(1, 10))
        
        # Contar total de escolas, turmas e alunos
        cur.execute("""
            SELECT COUNT(DISTINCT e.id)
            FROM escolas e
            JOIN turmas t ON e.id = t.escola_id
            WHERE t.segmento_id = 1 OR EXISTS (
                SELECT 1 FROM serie_faixa_etaria sfe
                JOIN ano_series ans ON sfe.serie_id = ans.id
                WHERE ans.id = t.ano_serie_id AND sfe.faixa_etaria_id = 1
            )
        """)
        total_escolas = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(DISTINCT t.id)
            FROM turmas t
            WHERE t.segmento_id = 1 OR EXISTS (
                SELECT 1 FROM serie_faixa_etaria sfe
                JOIN ano_series ans ON sfe.serie_id = ans.id
                WHERE ans.id = t.ano_serie_id AND sfe.faixa_etaria_id = 1
            )
        """)
        total_turmas = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(DISTINCT a.matricula)
            FROM alunos a
            JOIN turmas t ON a.turma_id = t.id
            WHERE t.segmento_id = 1 OR EXISTS (
                SELECT 1 FROM serie_faixa_etaria sfe
                JOIN ano_series ans ON sfe.serie_id = ans.id
                WHERE ans.id = t.ano_serie_id AND sfe.faixa_etaria_id = 1
            )
        """)
        total_alunos = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(DISTINCT a.matricula)
            FROM alunos a
            JOIN turmas t ON a.turma_id = t.id
            WHERE (t.segmento_id = 1 OR EXISTS (
                SELECT 1 FROM serie_faixa_etaria sfe
                JOIN ano_series ans ON sfe.serie_id = ans.id
                WHERE ans.id = t.ano_serie_id AND sfe.faixa_etaria_id = 1
            )) AND EXISTS (
                SELECT 1 FROM avaliacoes_direitos WHERE aluno_matricula = a.matricula
            )
        """)
        alunos_avaliados = cur.fetchone()[0]
        
        progresso = (alunos_avaliados / total_alunos * 100) if total_alunos > 0 else 0
        
        dados_gerais = [
            ["Total de Escolas", total_escolas],
            ["Total de Turmas", total_turmas],
            ["Total de Alunos", total_alunos],
            ["Alunos Avaliados", alunos_avaliados],
            ["Progresso", f"{progresso:.1f}%"]
        ]
        
        tabela_geral = Table(dados_gerais, colWidths=[150, 100])
        tabela_geral.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), '#4472C4'),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6'))
        ]))
        story.append(tabela_geral)
        story.append(Spacer(1, 20))
        
        # Médias por Campo de Experiência (todas as escolas)
        cur.execute("""
            SELECT 
                ce.nome,
                AVG(ad.estagio_numero),
                COUNT(ad.estagio_numero)
            FROM avaliacoes_direitos ad
            JOIN direitos_aprendizagem d ON ad.direito_id = d.id
            JOIN campos_experiencia ce ON d.campo_experiencia_id = ce.id
            JOIN alunos a ON ad.aluno_matricula = a.matricula
            JOIN turmas t ON a.turma_id = t.id
            WHERE t.segmento_id = 1 OR EXISTS (
                SELECT 1 FROM serie_faixa_etaria sfe
                JOIN ano_series ans ON sfe.serie_id = ans.id
                WHERE ans.id = t.ano_serie_id AND sfe.faixa_etaria_id = 1
            )
            GROUP BY ce.nome
            ORDER BY ce.nome
        """)
        
        dados_radar = cur.fetchall()
        campos = [row[0] for row in dados_radar]
        medias = [float(row[1]) for row in dados_radar]
        
        if campos:
            is_primeira_escrita = "Primeira Escrita" in campos
            
            fig = plt.figure(figsize=(8, 8))
            ax = fig.add_subplot(111, polar=True)
            
            N = len(campos)
            angles = [n / float(N) * 2 * np.pi for n in range(N)]
            angles += angles[:1]
            valores = medias + [medias[0]]
            
            ax.plot(angles, valores, linewidth=2, color='#36a2eb')
            ax.fill(angles, valores, alpha=0.25, color='#36a2eb')
            ax.set_theta_offset(np.pi / 2)
            ax.set_theta_direction(-1)
            ax.set_thetagrids(np.degrees(angles[:-1]), labels=campos)
            
            for label, angle in zip(ax.get_xticklabels(), angles[:-1]):
                label.set_horizontalalignment('center')
                angle_deg = angle * 180/np.pi
                if angle_deg > 90:
                    angle_deg -= 180
                label.set_rotation(angle_deg)
                label.set_rotation_mode('anchor')
            
            if is_primeira_escrita:
                ax.set_ylim(0, 8)
                ax.set_yticks(range(0, 9))
                ax.set_yticklabels([str(i) for i in range(0, 9)])
            else:
                ax.set_ylim(0, 5)
                ax.set_yticks(range(0, 6))
                ax.set_yticklabels([str(i) for i in range(0, 6)])
            
            ax.grid(color=(0, 0, 0, 0.1), linestyle='-', linewidth=0.5)
            ax.set_facecolor('white')
            ax.spines['polar'].set_visible(False)
            ax.set_title('Média por Campo de Experiência (Todas as Escolas)', pad=20)
            
            img_buffer = BytesIO()
            plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', facecolor='white')
            plt.close(fig)
            
            story.append(Paragraph("<b>Desenvolvimento da Aprendizagem - Média Geral</b>", estilo_destaque))
            story.append(Spacer(1, 10))
            img_buffer.seek(0)
            img = Image(img_buffer)
            img.drawHeight = 300
            img.drawWidth = img.drawHeight * (1.0 * img.imageWidth / img.imageHeight)
            story.append(img)
            story.append(Spacer(1, 20))
            
            # Adicionar legenda sobre o destaque
            story.append(Paragraph(
                "<i>Nota: Será destacado em amarelo o direito em desenvovimento (maior concentração nos estágios iniciais)</i>", 
                styles['Italic']
            ))
            story.append(Spacer(1, 15))
            story.append(PageBreak())
            
            # Detalhamento por campo de experiência (consolidado)
            for i, campo in enumerate(campos):
                is_primeira_escrita = campo == "Primeira Escrita"
                
                # Título do Campo
                story.append(Paragraph(f"<b>CAMPO DE EXPERIÊNCIA: {campo}</b>", estilo_titulo))
                
                # Consulta SQL para obter os dados do campo
                cur.execute("""
                    SELECT 
                        v.nome as verbo,
                        d.descricao as direito,
                        ad.estagio_numero,
                        COUNT(*) as total
                    FROM avaliacoes_direitos ad
                    JOIN direitos_aprendizagem d ON ad.direito_id = d.id
                    JOIN verbos v ON d.verbo_id = v.id
                    JOIN campos_experiencia ce ON d.campo_experiencia_id = ce.id
                    JOIN alunos a ON ad.aluno_matricula = a.matricula
                    JOIN turmas t ON a.turma_id = t.id
                    WHERE t.segmento_id = 1 AND ce.nome = %s
                    GROUP BY v.nome, d.descricao, ad.estagio_numero
                    ORDER BY v.nome, d.descricao, ad.estagio_numero
                """, (campo,))
                
                dados_campo = cur.fetchall()
                
                verbos_data = {}
                direitos_por_verbo = {}
                for verbo, direito, estagio, total in dados_campo:
                    if verbo not in verbos_data:
                        if is_primeira_escrita:
                            verbos_data[verbo] = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0}
                        else:
                            verbos_data[verbo] = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
                        direitos_por_verbo[verbo] = direito
                    verbos_data[verbo][estagio] += total
                
                verbos = sorted(verbos_data.keys())
                
                # Identificar o verbo com maior dificuldade (APENAS para campos que não são Primeira Escrita)
                verbo_destaque = None
                if not is_primeira_escrita:
                    max_concentracao = 0
                    menor_estagio = float('inf')
                    
                    for verbo in verbos:
                        total_verbo = sum(verbos_data[verbo].values())
                        if total_verbo == 0:
                            continue
                        
                        concentracao = (verbos_data[verbo][1] + verbos_data[verbo][2]) / total_verbo
                        estagios_com_alunos = [e for e in verbos_data[verbo] if verbos_data[verbo][e] > 0]
                        min_estagio = min(estagios_com_alunos) if estagios_com_alunos else float('inf')
                        
                        if (concentracao > max_concentracao) or \
                        (concentracao == max_concentracao and min_estagio < menor_estagio):
                            max_concentracao = concentracao
                            verbo_destaque = verbo
                            menor_estagio = min_estagio
                
                # Preparar dados para gráfico
                dados_grafico = []
                for verbo in verbos:
                    total_verbo = sum(verbos_data[verbo].values())
                    if total_verbo == 0:
                        continue
                    
                    if is_primeira_escrita:
                        porcentagens = [
                            (verbos_data[verbo][1] / total_verbo * 100),
                            (verbos_data[verbo][2] / total_verbo * 100),
                            (verbos_data[verbo][3] / total_verbo * 100),
                            (verbos_data[verbo][4] / total_verbo * 100),
                            (verbos_data[verbo][5] / total_verbo * 100),
                            (verbos_data[verbo][6] / total_verbo * 100),
                            (verbos_data[verbo][7] / total_verbo * 100),
                            (verbos_data[verbo][8] / total_verbo * 100)
                        ]
                    else:
                        porcentagens = [
                            (verbos_data[verbo][1] / total_verbo * 100),
                            (verbos_data[verbo][2] / total_verbo * 100),
                            (verbos_data[verbo][3] / total_verbo * 100),
                            (verbos_data[verbo][4] / total_verbo * 100),
                            (verbos_data[verbo][5] / total_verbo * 100)
                        ]
                    dados_grafico.append({
                        'verbo': verbo,
                        'porcentagens': porcentagens,
                        'total': total_verbo,
                        'destaque': verbo == verbo_destaque,
                        'direito': direitos_por_verbo.get(verbo, '')
                    })
                
                if not dados_grafico:
                    continue
                
                # Criar gráfico de barras
                fig, ax = plt.subplots(figsize=(10, 6))
                
                if is_primeira_escrita:
                    cores = ['#ff6b6b', '#ff8f8f', '#ffb3b3', '#ffd7d7', '#d7e3ff', '#a3c8ff', '#6ea6ff', '#36a2eb']
                    labels_estagios = [f'Estágio {i}' for i in range(1, 9)]
                else:
                    cores = ['#ff6b6b', '#ffa3a3', '#a3d8ff', '#4da6ff', '#36a2eb']
                    labels_estagios = [f'Estágio {i}' for i in range(1, 6)]
                
                bottom = np.zeros(len(dados_grafico))
                for i in range(len(cores)):
                    valores = [d['porcentagens'][i] for d in dados_grafico]
                    barras = ax.bar(
                        [d['verbo'] for d in dados_grafico],
                        valores,
                        bottom=bottom,
                        color=cores[i],
                        label=labels_estagios[i],
                        edgecolor='white'
                    )
                    
                    # Aplicar destaque no gráfico (apenas para campos que não são Primeira Escrita)
                    if verbo_destaque and not is_primeira_escrita:
                        for j, bar in enumerate(barras):
                            if dados_grafico[j]['destaque']:
                                bar.set_edgecolor('#d63900')
                                bar.set_linewidth(2)
                    
                    for bar in barras:
                        height = bar.get_height()
                        if height > 10:
                            ax.text(
                                bar.get_x() + bar.get_width() / 2,
                                bar.get_y() + height / 2,
                                f'{height:.0f}%',
                                ha='center',
                                va='center',
                                color='white' if i < 2 else 'black',
                                fontsize=8
                            )
                    
                    bottom += valores
                
                ax.set_ylabel('Porcentagem de Alunos')
                ax.set_title(f'Distribuição por Estágio - {campo} (Todas as Escolas)')
                ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1))
                plt.xticks(rotation=45, ha='right')
                plt.tight_layout()
                
                img_buffer = BytesIO()
                plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight')
                plt.close(fig)
                
                # Adicionar mensagem de dificuldade (apenas para campos que não são Primeira Escrita)
                if verbo_destaque and not is_primeira_escrita:
                    direito_destaque = direitos_por_verbo.get(verbo_destaque, '')
                    story.append(Paragraph(
                        f"<font color='#003366'><b>Em desenvovimento: {verbo_destaque} ({direito_destaque}) - " +
                        f"{max_concentracao:.1%} dos alunos nos estágios iniciais</b></font>", 
                        estilo_normal
                    ))
                
                story.append(Spacer(1, 10))
                img_buffer.seek(0)
                img = Image(img_buffer)
                img.drawHeight = 300
                img.drawWidth = min(500, img.drawHeight * (1.0 * img.imageWidth / img.imageHeight))
                story.append(img)
                story.append(Spacer(1, 15))
                
                # Tabela com os dados
                if is_primeira_escrita:
                    headers = ["Verbo"] + [f'Estágio {i}' for i in range(1, 9)] 
                    col_widths = [160] + [50]*8  # Larguras ajustadas
                else:
                    headers = ["Verbo"] + [f'Estágio {i}' for i in range(1, 6)] 
                    col_widths = [160] + [50]*5  # Larguras ajustadas
                
                tabela_dados = [headers]
                for dado in dados_grafico:
                    linha = [dado['verbo']]
                    linha.extend([f"{p:.1f}%" for p in dado['porcentagens']])
                    tabela_dados.append(linha)
                
                tabela = Table(tabela_dados, colWidths=col_widths)
                estilo = [
                    ('BACKGROUND', (0, 0), (-1, 0), '#4472C4'),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 8),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
                    ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6')),
                ]
                
                # Aplicar destaque na tabela (apenas para campos que não são Primeira Escrita)
                if verbo_destaque and not is_primeira_escrita:
                    for idx, linha in enumerate(tabela_dados[1:], start=1):
                        if linha[0] == verbo_destaque:
                            estilo.extend([
                                ('BACKGROUND', (0, idx), (-1, idx), '#fff3bf'),
                                ('TEXTCOLOR', (0, idx), (-1, idx), '#d63900'),
                                ('FONTNAME', (0, idx), (-1, idx), 'Helvetica-Bold')
                            ])
                            break
                
                tabela.setStyle(TableStyle(estilo))
                story.append(tabela)
                story.append(PageBreak())
        
        # Dados por escola
        story.append(Paragraph("<b>DETALHAMENTO POR ESCOLA</b>", estilo_destaque))
        story.append(Spacer(1, 20))
        
        for escola_id, escola_nome in escolas:
            story.append(Paragraph(f"<b>ESCOLA: {escola_nome}</b>", estilo_titulo))
            story.append(Spacer(1, 10))
            
            # Dados gerais da escola
            cur.execute("""
                SELECT COUNT(DISTINCT t.id)
                FROM turmas t
                WHERE t.escola_id = %s AND (
                    t.segmento_id = 1 OR EXISTS (
                        SELECT 1 FROM serie_faixa_etaria sfe
                        JOIN ano_series ans ON sfe.serie_id = ans.id
                        WHERE ans.id = t.ano_serie_id AND sfe.faixa_etaria_id = 1
                    )
                )
            """, (escola_id,))
            turmas_escola = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(DISTINCT a.matricula)
                FROM alunos a
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.escola_id = %s AND (
                    t.segmento_id = 1 OR EXISTS (
                        SELECT 1 FROM serie_faixa_etaria sfe
                        JOIN ano_series ans ON sfe.serie_id = ans.id
                        WHERE ans.id = t.ano_serie_id AND sfe.faixa_etaria_id = 1
                    )
                )
            """, (escola_id,))
            alunos_escola = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(DISTINCT a.matricula)
                FROM alunos a
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.escola_id = %s AND (
                    t.segmento_id = 1 OR EXISTS (
                        SELECT 1 FROM serie_faixa_etaria sfe
                        JOIN ano_series ans ON sfe.serie_id = ans.id
                        WHERE ans.id = t.ano_serie_id AND sfe.faixa_etaria_id = 1
                    )
                ) AND EXISTS (
                    SELECT 1 FROM avaliacoes_direitos WHERE aluno_matricula = a.matricula
                )
            """, (escola_id,))
            avaliados_escola = cur.fetchone()[0]
            
            progresso_escola = (avaliados_escola / alunos_escola * 100) if alunos_escola > 0 else 0
            
            dados_escola = [
                ["Total de Turmas", turmas_escola],
                ["Total de Alunos", alunos_escola],
                ["Alunos Avaliados", avaliados_escola],
                ["Progresso", f"{progresso_escola:.1f}%"]
            ]
            
            tabela_escola = Table(dados_escola, colWidths=[150, 100])
            tabela_escola.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), '#4472C4'),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
                ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6'))
            ]))
            story.append(tabela_escola)
            story.append(Spacer(1, 20))
            
            # Médias por Campo de Experiência para esta escola
            cur.execute("""
                SELECT 
                    ce.nome,
                    AVG(ad.estagio_numero),
                    COUNT(ad.estagio_numero)
                FROM avaliacoes_direitos ad
                JOIN direitos_aprendizagem d ON ad.direito_id = d.id
                JOIN campos_experiencia ce ON d.campo_experiencia_id = ce.id
                JOIN alunos a ON ad.aluno_matricula = a.matricula
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.escola_id = %s AND (
                    t.segmento_id = 1 OR EXISTS (
                        SELECT 1 FROM serie_faixa_etaria sfe
                        JOIN ano_series ans ON sfe.serie_id = ans.id
                        WHERE ans.id = t.ano_serie_id AND sfe.faixa_etaria_id = 1
                    )
                )
                GROUP BY ce.nome
                ORDER BY ce.nome
            """, (escola_id,))
            
            dados_radar_escola = cur.fetchall()
            campos_escola = [row[0] for row in dados_radar_escola]
            medias_escola = [float(row[1]) for row in dados_radar_escola]
            
            if campos_escola:
                is_primeira_escrita = "Primeira Escrita" in campos_escola
                
                fig = plt.figure(figsize=(8, 8))
                ax = fig.add_subplot(111, polar=True)
                
                N = len(campos_escola)
                angles = [n / float(N) * 2 * np.pi for n in range(N)]
                angles += angles[:1]
                valores = medias_escola + [medias_escola[0]]
                
                ax.plot(angles, valores, linewidth=2, color='#36a2eb')
                ax.fill(angles, valores, alpha=0.25, color='#36a2eb')
                ax.set_theta_offset(np.pi / 2)
                ax.set_theta_direction(-1)
                ax.set_thetagrids(np.degrees(angles[:-1]), labels=campos_escola)
                
                for label, angle in zip(ax.get_xticklabels(), angles[:-1]):
                    label.set_horizontalalignment('center')
                    angle_deg = angle * 180/np.pi
                    if angle_deg > 90:
                        angle_deg -= 180
                    label.set_rotation(angle_deg)
                    label.set_rotation_mode('anchor')
                
                if is_primeira_escrita:
                    ax.set_ylim(0, 8)
                    ax.set_yticks(range(0, 9))
                    ax.set_yticklabels([str(i) for i in range(0, 9)])
                else:
                    ax.set_ylim(0, 5)
                    ax.set_yticks(range(0, 6))
                    ax.set_yticklabels([str(i) for i in range(0, 6)])
                
                ax.grid(color=(0, 0, 0, 0.1), linestyle='-', linewidth=0.5)
                ax.set_facecolor('white')
                ax.spines['polar'].set_visible(False)
                ax.set_title(f'Média por Campo de Experiência - {escola_nome}', pad=20)
                
                img_buffer = BytesIO()
                plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', facecolor='white')
                plt.close(fig)
                
                story.append(Paragraph("<b>Desenvolvimento da Aprendizagem</b>", estilo_destaque))
                story.append(Spacer(1, 10))
                img_buffer.seek(0)
                img = Image(img_buffer)
                img.drawHeight = 250
                img.drawWidth = img.drawHeight * (1.0 * img.imageWidth / img.imageHeight)
                story.append(img)
                story.append(Spacer(1, 20))
            
            # Quebra de página para próxima escola (exceto a última)
            if escola_id != escolas[-1][0]:
                story.append(PageBreak())
        
        doc.build(story)
        buffer.seek(0)

        nome_arquivo = f"relatorio_consolidado_infantil_{datetime.now().strftime('%Y%m%d')}.pdf"
        return send_file(
            buffer,
            as_attachment=True,
            download_name=nome_arquivo,
            mimetype='application/pdf'
        )
        
    except Exception as e:
        app.logger.error(f"Erro ao gerar PDF consolidado infantil: {str(e)}", exc_info=True)
        return render_template('error.html', error="Erro ao gerar relatório consolidado"), 500
    finally:
        if 'conn' in locals():
            conn.close()




@app.route('/gerar-pdf-todas-escolas-fundamental')
@admin_required
def gerar_pdf_todas_escolas_fundamental():
    try:
        # Configurações do PDF
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import numpy as np
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib import colors
        from io import BytesIO
        from datetime import datetime
        import re
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Buscar todas as escolas que têm turmas do fundamental
        cur.execute("""
            SELECT DISTINCT e.id, e.nome 
            FROM escolas e
            JOIN turmas t ON e.id = t.escola_id
            WHERE t.segmento_id = 2
            ORDER BY e.nome
        """)
        escolas = cur.fetchall()
        
        if not escolas:
            return render_template('error.html', error="Nenhuma escola com turmas do fundamental encontrada"), 400

        # Criar PDF
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter,
                              rightMargin=40, leftMargin=40,
                              topMargin=40, bottomMargin=40)
        styles = getSampleStyleSheet()
        story = []
        
        # Estilos
        estilo_titulo = styles['Title']
        estilo_titulo.alignment = 1
        estilo_normal = styles['Normal']
        estilo_normal.fontName = 'Helvetica'
        estilo_normal.fontSize = 10
        estilo_destaque = styles['Normal']
        estilo_destaque.fontName = 'Helvetica-Bold'
        estilo_destaque.textColor = colors.HexColor("#000000")
        
        # CABEÇALHO COM LOGOS
        logo_somos = 'static/logo_somos.png'
        logo_pref = 'static/prefeitura_sj.png'
        
        cabecalho_tabela = Table([
            [Image(logo_somos, width=60, height=30), 
             Paragraph("<b>Acompanhamento do Desenvolvimento da Aprendizagem - ENSINO FUNDAMENTAL</b>", estilo_titulo), 
             Image(logo_pref, width=60, height=30)]
        ], colWidths=[80, 360, 80])
        
        cabecalho_tabela.setStyle(TableStyle([
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('ALIGN', (1, 0), (1, 0), 'CENTER'),
            ('ALIGN', (2, 0), (2, 0), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ]))
        
        story.append(cabecalho_tabela)
        story.append(Spacer(1, 10))
        
        # Título e data
        #story.append(Paragraph("<b>RELATÓRIO REDE - ENSINO FUNDAMENTAL</b>", estilo_titulo))
        #data_emissao = datetime.now().strftime("%d/%m/%Y")
        #story.append(Paragraph(f"<i>Data de emissão: {data_emissao}</i>", styles['Italic']))
        #story.append(Spacer(1, 20))

        # Resumo geral
        story.append(Paragraph("<b>DADOS GERAIS</b>", estilo_destaque))
        story.append(Spacer(1, 10))
        
        # Contar total de escolas, turmas e alunos
        cur.execute("""
            SELECT COUNT(DISTINCT e.id)
            FROM escolas e
            JOIN turmas t ON e.id = t.escola_id
            WHERE t.segmento_id = 2
        """)
        total_escolas = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(DISTINCT t.id)
            FROM turmas t
            WHERE t.segmento_id = 2
        """)
        total_turmas = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(DISTINCT a.matricula)
            FROM alunos a
            JOIN turmas t ON a.turma_id = t.id
            WHERE t.segmento_id = 2
        """)
        total_alunos = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(DISTINCT a.matricula)
            FROM alunos a
            JOIN turmas t ON a.turma_id = t.id
            WHERE t.segmento_id = 2 AND EXISTS (
                SELECT 1 FROM avaliacoes_habilidades WHERE aluno_matricula = a.matricula
            )
        """)
        alunos_avaliados = cur.fetchone()[0]
        
        progresso = (alunos_avaliados / total_alunos * 100) if total_alunos > 0 else 0
        
        dados_gerais = [
            ["Total de Escolas", total_escolas],
            ["Total de Turmas", total_turmas],
            ["Total de Alunos", total_alunos],
            ["Alunos Avaliados", alunos_avaliados],
            ["Progresso", f"{progresso:.1f}%"]
        ]
        
        tabela_geral = Table(dados_gerais, colWidths=[150, 100])
        tabela_geral.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), '#4472C4'),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6'))
        ]))
        story.append(tabela_geral)
        story.append(Spacer(1, 20))
        
        # Médias por Disciplina (todas as escolas)
        cur.execute("""
            SELECT 
                d.nome,
                AVG(ah.estagio_numero),
                COUNT(ah.estagio_numero)
            FROM avaliacoes_habilidades ah
            JOIN habilidades h ON ah.habilidade_id = h.id
            JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
            JOIN disciplinas d ON ut.disciplina_id = d.id
            JOIN alunos a ON ah.aluno_matricula = a.matricula
            JOIN turmas t ON a.turma_id = t.id
            WHERE t.segmento_id = 2
            GROUP BY d.nome
            ORDER BY d.nome
        """)
        
        dados_radar = cur.fetchall()
        disciplinas = [row[0] for row in dados_radar]
        medias = [float(row[1]) for row in dados_radar]
        
        if disciplinas:
            fig = plt.figure(figsize=(8, 8))
            ax = fig.add_subplot(111, polar=True)
            
            N = len(disciplinas)
            angles = [n / float(N) * 2 * np.pi for n in range(N)]
            angles += angles[:1]
            valores = medias + [medias[0]]
            
            ax.plot(angles, valores, linewidth=2, color='#36a2eb')
            ax.fill(angles, valores, alpha=0.25, color='#36a2eb')
            ax.set_theta_offset(np.pi / 2)
            ax.set_theta_direction(-1)
            ax.set_thetagrids(np.degrees(angles[:-1]), labels=disciplinas)
            
            for label, angle in zip(ax.get_xticklabels(), angles[:-1]):
                label.set_horizontalalignment('center')
                angle_deg = angle * 180/np.pi
                if angle_deg > 90:
                    angle_deg -= 180
                label.set_rotation(angle_deg)
                label.set_rotation_mode('anchor')
            
            ax.set_ylim(0, 5)
            ax.set_yticks(range(0, 6))
            ax.set_yticklabels([str(i) for i in range(0, 6)])
            
            ax.grid(color=(0, 0, 0, 0.1), linestyle='-', linewidth=0.5)
            ax.set_facecolor('white')
            ax.spines['polar'].set_visible(False)
            ax.set_title('Média por Componente Curricular (Todas as Escolas)', pad=20)
            
            img_buffer = BytesIO()
            plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', facecolor='white')
            plt.close(fig)
            
            story.append(Paragraph("<b>Desenvolvimento da Aprendizagem - Média Geral</b>", estilo_destaque))
            story.append(Spacer(1, 10))
            img_buffer.seek(0)
            img = Image(img_buffer)
            img.drawHeight = 300
            img.drawWidth = img.drawHeight * (1.0 * img.imageWidth / img.imageHeight)
            story.append(img)
            story.append(Spacer(1, 20))
            
            # Adicionar legenda sobre o destaque
            story.append(Paragraph(
                "<i>Nota: Será destacado em amarelo a habilidade em desenvovimento (maior concentração nos estágios iniciais)</i>", 
                styles['Italic']
            ))
            story.append(Spacer(1, 15))
            story.append(PageBreak())
            
            # Análise detalhada por disciplina
            for disciplina in disciplinas:
                story.append(Paragraph(f"<b>COMPONENTE CURRICULAR: {disciplina}</b>", estilo_titulo))
                story.append(Spacer(1, 10))
                
                # Buscar unidades temáticas desta disciplina
                cur.execute("""
                    SELECT 
                        ut.nome,
                        AVG(ah.estagio_numero),
                        COUNT(ah.estagio_numero)
                    FROM avaliacoes_habilidades ah
                    JOIN habilidades h ON ah.habilidade_id = h.id
                    JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                    JOIN disciplinas d ON ut.disciplina_id = d.id
                    JOIN alunos a ON ah.aluno_matricula = a.matricula
                    JOIN turmas t ON a.turma_id = t.id
                    WHERE t.segmento_id = 2 AND d.nome = %s
                    GROUP BY ut.nome
                    ORDER BY ut.nome
                """, (disciplina,))
                
                unidades_tematicas = cur.fetchall()
                
                if not unidades_tematicas:
                    story.append(Paragraph("Nenhuma avaliação encontrada para esta disciplina.", estilo_normal))
                    story.append(Spacer(1, 20))
                    continue
                
                # Gráfico de barras por unidade temática
                unidades_nomes = [ut[0] for ut in unidades_tematicas]
                unidades_medias = [float(ut[1]) for ut in unidades_tematicas]
                
                fig, ax = plt.subplots(figsize=(10, 6))
                
                cores = []
                for media in unidades_medias:
                    if media < 1.5:
                        cores.append('#ff6b6b')  # Vermelho
                    elif media < 2.5:
                        cores.append('#ffa3a3')  # Vermelho claro
                    elif media < 3.5:
                        cores.append('#a3d8ff')  # Azul claro
                    elif media < 4.5:
                        cores.append('#4da6ff')  # Azul médio
                    else:
                        cores.append('#36a2eb')  # Azul
                
                barras = ax.bar(unidades_nomes, unidades_medias, color=cores)
                
                for bar, media in zip(barras, unidades_medias):
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                            f'{media:.1f}',
                            ha='center', va='bottom',
                            fontsize=10)
                
                ax.set_ylabel('Média dos Estágios')
                ax.set_title(f'Média por Unidade Temática - {disciplina}')
                plt.xticks(rotation=45, ha='right')
                plt.ylim(0, 5)
                plt.tight_layout()
                
                img_buffer = BytesIO()
                plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight')
                plt.close(fig)
                
                story.append(Paragraph("<b>Acompanhamento por Unidade Temática</b>", estilo_destaque))
                story.append(Spacer(1, 10))
                img_buffer.seek(0)
                img = Image(img_buffer)
                img.drawHeight = 250
                img.drawWidth = min(500, img.drawHeight * (1.0 * img.imageWidth / img.imageHeight))
                story.append(img)
                story.append(Spacer(1, 15))
                
                # Tabela com médias por unidade temática
                tabela_dados = [["Unidade Temática", "Média"]]
                for nome, media in zip(unidades_nomes, unidades_medias):
                    tabela_dados.append([nome, f"{media:.2f}"])
                
                tabela = Table(tabela_dados, colWidths=[300, 80])
                tabela.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), '#4472C4'),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
                    ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6')),
                ]))
                story.append(tabela)
                story.append(Spacer(1, 20))
                story.append(PageBreak())
                
                # Análise detalhada por unidade temática
                for unidade in unidades_nomes:
                    story.append(Paragraph(f"<b>UNIDADE TEMÁTICA: {unidade}</b>", estilo_destaque))
                    story.append(Spacer(1, 10))
                    
                    # Buscar habilidades e distribuição por estágio
                    cur.execute("""
                        SELECT 
                            h.descricao,
                            ah.estagio_numero,
                            COUNT(*) as total
                        FROM avaliacoes_habilidades ah
                        JOIN habilidades h ON ah.habilidade_id = h.id
                        JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                        JOIN disciplinas d ON ut.disciplina_id = d.id
                        JOIN alunos a ON ah.aluno_matricula = a.matricula
                        JOIN turmas t ON a.turma_id = t.id
                        WHERE t.segmento_id = 2 AND d.nome = %s AND ut.nome = %s
                        GROUP BY h.descricao, ah.estagio_numero
                        ORDER BY h.descricao, ah.estagio_numero
                    """, (disciplina, unidade))
                    
                    dados_habilidades = cur.fetchall()
                    
                    # Organizar dados por habilidade
                    habilidades_data = {}
                    for descricao, estagio, total in dados_habilidades:
                        codigo = re.search(r'\((.*?)\)', descricao)
                        codigo = codigo.group(1) if codigo else descricao
                        if codigo not in habilidades_data:
                            habilidades_data[codigo] = {
                                'descricao': descricao,
                                'estagios': {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
                            }
                        habilidades_data[codigo]['estagios'][estagio] = total
                    
                    if not habilidades_data:
                        story.append(Paragraph("Nenhuma avaliação encontrada para esta unidade temática.", estilo_normal))
                        story.append(Spacer(1, 20))
                        continue
                    
                    # Identificar habilidade com maior dificuldade
                    habilidade_destaque = None
                    max_concentracao = 0
                    menor_estagio = float('inf')
                    
                    if len(habilidades_data) > 1:
                        for codigo, dados in habilidades_data.items():
                            total = sum(dados['estagios'].values())
                            if total == 0:
                                continue
                            
                            concentracao = (dados['estagios'][1] + dados['estagios'][2]) / total
                            estagios_com_alunos = [e for e in dados['estagios'] if dados['estagios'][e] > 0]
                            min_estagio = min(estagios_com_alunos) if estagios_com_alunos else float('inf')
                            
                            if (concentracao > max_concentracao) or \
                               (concentracao == max_concentracao and min_estagio < menor_estagio):
                                max_concentracao = concentracao
                                habilidade_destaque = codigo
                                menor_estagio = min_estagio
                    
                    # Preparar dados para gráfico
                    habilidades_codigos = list(habilidades_data.keys())
                    habilidades_descricoes = [habilidades_data[c]['descricao'] for c in habilidades_codigos]
                    
                    # Cores para os estágios
                    cores_estagios = ['#ff6b6b', '#ffa3a3', '#a3d8ff', '#4da6ff', '#36a2eb']
                    
                    # Criar gráfico de barras empilhadas
                    fig, ax = plt.subplots(figsize=(10, 6))
                    bottom = np.zeros(len(habilidades_codigos))
                    
                    for estagio in range(1, 6):
                        valores = []
                        for codigo in habilidades_codigos:
                            total = sum(habilidades_data[codigo]['estagios'].values())
                            if total > 0:
                                valores.append(habilidades_data[codigo]['estagios'][estagio] / total * 100)
                            else:
                                valores.append(0)
                        
                        barras = ax.bar(habilidades_codigos, valores, bottom=bottom, 
                                       color=cores_estagios[estagio-1], 
                                       label=f'Estágio {estagio}',
                                       width=0.6)
                        
                        # Aplicar destaque no gráfico
                        if habilidade_destaque and len(habilidades_data) > 1:
                            for j, bar in enumerate(barras):
                                if habilidades_codigos[j] == habilidade_destaque:
                                    bar.set_edgecolor('#d63900')
                                    bar.set_linewidth(2)
                        
                        # Adicionar valores nas barras
                        for bar in barras:
                            height = bar.get_height()
                            if height > 5:  # Só mostra porcentagem se for maior que 5%
                                ax.text(bar.get_x() + bar.get_width()/2., 
                                        bar.get_y() + height/2,
                                        f'{height:.0f}%', 
                                        ha='center', va='center',
                                        color='white' if estagio <= 2 else 'black',
                                        fontsize=8)
                        
                        bottom += valores
                    
                    # Configurações do gráfico
                    ax.set_ylabel('Porcentagem de Alunos')
                    ax.set_title(f'Distribuição por Estágio - {unidade}')
                    ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1))
                    plt.xticks(rotation=45, ha='right')
                    plt.ylim(0, 100)
                    plt.tight_layout()
                    
                    # Salvar gráfico
                    img_buffer = BytesIO()
                    plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight')
                    plt.close(fig)
                    
                    # Adicionar mensagem sobre a habilidade destacada
                    if habilidade_destaque and len(habilidades_data) > 1:
                        descricao = habilidades_data[habilidade_destaque]['descricao']
                        story.append(Paragraph(
                            f"<font color='#000000'><b>Habilidade a desenvolver:</b> {descricao}</font>", 
                            estilo_normal
                        ))
                        story.append(Spacer(1, 10))
                    
                    # Adicionar gráfico ao PDF
                    img_buffer.seek(0)
                    img = Image(img_buffer)
                    img.drawHeight = 300
                    img.drawWidth = min(500, img.drawHeight * (1.0 * img.imageWidth / img.imageHeight))
                    story.append(img)
                    story.append(Spacer(1, 15))
                    
                    # Tabela com os dados
                    tabela_dados = [["Código", "Estágio 1", "Estágio 2", "Estágio 3", "Estágio 4", "Estágio 5"]]
                    
                    for codigo in habilidades_codigos:
                        dados = habilidades_data[codigo]
                        total = sum(dados['estagios'].values())
                        linha = [codigo]
                        for estagio in range(1, 6):
                            porcentagem = (dados['estagios'][estagio] / total * 100) if total > 0 else 0
                            linha.append(f"{porcentagem:.1f}%")
                        tabela_dados.append(linha)
                    
                    # Criar tabela
                    tabela = Table(tabela_dados, colWidths=[80] + [60]*5)
                    
                    # Estilo da tabela
                    estilo = [
                        ('BACKGROUND', (0, 0), (-1, 0), '#4472C4'),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, -1), 8),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
                        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6')),
                    ]
                    
                    # Aplicar destaque na tabela
                    if habilidade_destaque and len(habilidades_data) > 1:
                        idx_destaque = habilidades_codigos.index(habilidade_destaque) + 1
                        estilo.extend([
                            ('BACKGROUND', (0, idx_destaque), (-1, idx_destaque), '#fff3bf'),
                            ('TEXTCOLOR', (0, idx_destaque), (-1, idx_destaque), '#d63900'),
                            ('FONTNAME', (0, idx_destaque), (-1, idx_destaque), 'Helvetica-Bold')
                        ])
                    
                    tabela.setStyle(TableStyle(estilo))
                    story.append(tabela)
                    story.append(Spacer(1, 20))
                    story.append(PageBreak())
        
        # Dados por escola
        story.append(Paragraph("<b>DETALHAMENTO POR ESCOLA</b>", estilo_destaque))
        story.append(Spacer(1, 20))
        
        for escola_id, escola_nome in escolas:
            story.append(Paragraph(f"<b>ESCOLA: {escola_nome}</b>", estilo_titulo))
            story.append(Spacer(1, 10))
            
            # Dados gerais da escola
            cur.execute("""
                SELECT COUNT(DISTINCT t.id)
                FROM turmas t
                WHERE t.escola_id = %s AND t.segmento_id = 2
            """, (escola_id,))
            turmas_escola = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(DISTINCT a.matricula)
                FROM alunos a
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.escola_id = %s AND t.segmento_id = 2
            """, (escola_id,))
            alunos_escola = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(DISTINCT a.matricula)
                FROM alunos a
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.escola_id = %s AND t.segmento_id = 2 AND EXISTS (
                    SELECT 1 FROM avaliacoes_habilidades WHERE aluno_matricula = a.matricula
                )
            """, (escola_id,))
            avaliados_escola = cur.fetchone()[0]
            
            progresso_escola = (avaliados_escola / alunos_escola * 100) if alunos_escola > 0 else 0
            
            dados_escola = [
                ["Total de Turmas", turmas_escola],
                ["Total de Alunos", alunos_escola],
                ["Alunos Avaliados", avaliados_escola],
                ["Progresso", f"{progresso_escola:.1f}%"]
            ]
            
            tabela_escola = Table(dados_escola, colWidths=[150, 100])
            tabela_escola.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), '#4472C4'),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
                ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6'))
            ]))
            story.append(tabela_escola)
            story.append(Spacer(1, 20))
            
            # Médias por Disciplina para esta escola
            cur.execute("""
                SELECT 
                    d.nome,
                    AVG(ah.estagio_numero),
                    COUNT(ah.estagio_numero)
                FROM avaliacoes_habilidades ah
                JOIN habilidades h ON ah.habilidade_id = h.id
                JOIN unidades_tematicas ut ON h.unidade_tematica_id = ut.id
                JOIN disciplinas d ON ut.disciplina_id = d.id
                JOIN alunos a ON ah.aluno_matricula = a.matricula
                JOIN turmas t ON a.turma_id = t.id
                WHERE t.escola_id = %s AND t.segmento_id = 2
                GROUP BY d.nome
                ORDER BY d.nome
            """, (escola_id,))
            
            dados_radar_escola = cur.fetchall()
            disciplinas_escola = [row[0] for row in dados_radar_escola]
            medias_escola = [float(row[1]) for row in dados_radar_escola]
            
            if disciplinas_escola:
                fig = plt.figure(figsize=(8, 8))
                ax = fig.add_subplot(111, polar=True)
                
                N = len(disciplinas_escola)
                angles = [n / float(N) * 2 * np.pi for n in range(N)]
                angles += angles[:1]
                valores = medias_escola + [medias_escola[0]]
                
                ax.plot(angles, valores, linewidth=2, color='#36a2eb')
                ax.fill(angles, valores, alpha=0.25, color='#36a2eb')
                ax.set_theta_offset(np.pi / 2)
                ax.set_theta_direction(-1)
                ax.set_thetagrids(np.degrees(angles[:-1]), labels=disciplinas_escola)
                
                for label, angle in zip(ax.get_xticklabels(), angles[:-1]):
                    label.set_horizontalalignment('center')
                    angle_deg = angle * 180/np.pi
                    if angle_deg > 90:
                        angle_deg -= 180
                    label.set_rotation(angle_deg)
                    label.set_rotation_mode('anchor')
                
                ax.set_ylim(0, 5)
                ax.set_yticks(range(0, 6))
                ax.set_yticklabels([str(i) for i in range(0, 6)])
                
                ax.grid(color=(0, 0, 0, 0.1), linestyle='-', linewidth=0.5)
                ax.set_facecolor('white')
                ax.spines['polar'].set_visible(False)
                ax.set_title(f'Média por Componente Curricular - {escola_nome}', pad=20)
                
                img_buffer = BytesIO()
                plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', facecolor='white')
                plt.close(fig)
                
                story.append(Paragraph("<b>Desenvolvimento da Aprendizagem</b>", estilo_destaque))
                story.append(Spacer(1, 10))
                img_buffer.seek(0)
                img = Image(img_buffer)
                img.drawHeight = 250
                img.drawWidth = img.drawHeight * (1.0 * img.imageWidth / img.imageHeight)
                story.append(img)
                story.append(Spacer(1, 20))
            
            # Quebra de página para próxima escola (exceto a última)
            if escola_id != escolas[-1][0]:
                story.append(PageBreak())
        
        doc.build(story)
        buffer.seek(0)

        nome_arquivo = f"relatorio_consolidado_fundamental_{datetime.now().strftime('%Y%m%d')}.pdf"
        return send_file(
            buffer,
            as_attachment=True,
            download_name=nome_arquivo,
            mimetype='application/pdf'
        )
        
    except Exception as e:
        app.logger.error(f"Erro ao gerar PDF consolidado fundamental: {str(e)}", exc_info=True)
        return render_template('error.html', error="Erro ao gerar relatório consolidado"), 500
    finally:
        if 'conn' in locals():
            conn.close()




if __name__ == '__main__':
    app.run(debug=True)