# -*- coding: utf-8 -*-
"""
Created on Wed May 21 18:46:59 2025

@author: roger
"""


#DROP TABLE IF EXISTS alunos, professores, turmas, segmentos, escolas CASCADE;


import pandas as pd
import psycopg2
from psycopg2 import sql
'''
DB_CONFIG = {
    "host": "dpg-d0scu2adbo4c73ev9850-a.virginia-postgres.render.com",  # Host externo atualizado
    "port": "5432",
    "database": "somos_educa_2025_kzyt",  # Nome do banco atualizado
    "user": "somos_educa_2025",  # Usuário atualizado
    "password": "0shvmSkg0hHMKnwZr7SziKydYFAsuVGV",  # Nova senha
    "sslmode": "require"  # Mantido para conexão segura

}

'''
DB_CONFIG = {
    "host": "localhost",  # Host externo
    "port": "5432",
    "database": "postgres",
    "user": "postgres",
    "password": "RAEB449140",  # Senha revelada na URL
    "sslmode": "disable"  
}

TABELAS = {
    'escolas': """
        CREATE TABLE IF NOT EXISTS escolas (
            id SERIAL PRIMARY KEY,
            id_plurall VARCHAR(20) UNIQUE NOT NULL,
            nome VARCHAR(200) NOT NULL,
            UNIQUE(id_plurall, nome)
        )  -- <- Fechando corretamente
    """,

    
    'segmentos': """
        CREATE TABLE IF NOT EXISTS segmentos (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(50) NOT NULL UNIQUE
        )
    """,
    
    'ano_series': """
        CREATE TABLE IF NOT EXISTS ano_series (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(20) NOT NULL UNIQUE
        )
    """,
    
    'turmas': """
        CREATE TABLE IF NOT EXISTS turmas (
            id SERIAL PRIMARY KEY,
            escola_id INTEGER NOT NULL REFERENCES escolas(id),
            segmento_id INTEGER NOT NULL REFERENCES segmentos(id),
            ano_serie_id INTEGER NOT NULL REFERENCES ano_series(id),
            nome VARCHAR(50) NOT NULL,
            UNIQUE(escola_id, segmento_id, ano_serie_id, nome)
        )
    """,
    
    'alunos': """
        CREATE TABLE IF NOT EXISTS alunos (
            matricula VARCHAR(20) PRIMARY KEY,
            nome VARCHAR(200) NOT NULL,
            id_plurall VARCHAR(20) UNIQUE NOT NULL,
            turma_id INTEGER NOT NULL REFERENCES turmas(id)
        )
    """,
    
    'professores': """
        CREATE TABLE IF NOT EXISTS professores (
            id_plurall VARCHAR(20) PRIMARY KEY,
            nome VARCHAR(200) NOT NULL,
            segmento_id INTEGER REFERENCES segmentos(id)
        )
    """,
    
    'professor_escola': """
        CREATE TABLE IF NOT EXISTS professor_escola (
            professor_id VARCHAR(20) NOT NULL REFERENCES professores(id_plurall),
            escola_id INTEGER NOT NULL REFERENCES escolas(id),
            PRIMARY KEY (professor_id, escola_id)
        )
    """,
    'verbos': """
        CREATE TABLE IF NOT EXISTS verbos (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(255) NOT NULL UNIQUE
        )
    """,
    
    'faixas_etarias': """
        CREATE TABLE IF NOT EXISTS faixas_etarias (
            id SERIAL PRIMARY KEY,
            descricao VARCHAR(255) NOT NULL UNIQUE
        )
    """,
    
    'campos_experiencia': """
        CREATE TABLE IF NOT EXISTS campos_experiencia (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(255) NOT NULL UNIQUE
        )
    """,
    
    'direitos_aprendizagem': """
        CREATE TABLE IF NOT EXISTS direitos_aprendizagem (
            id SERIAL PRIMARY KEY,
            verbo_id INTEGER NOT NULL REFERENCES verbos(id),
            faixa_etaria_id INTEGER NOT NULL REFERENCES faixas_etarias(id),
            campo_experiencia_id INTEGER NOT NULL REFERENCES campos_experiencia(id),
            descricao TEXT NOT NULL,
            UNIQUE(verbo_id, faixa_etaria_id, campo_experiencia_id, descricao)
        )
    """,
    
    'estagios': """
        CREATE TABLE IF NOT EXISTS estagios (
            id SERIAL PRIMARY KEY,
            direito_id INTEGER NOT NULL REFERENCES direitos_aprendizagem(id),
            numero_estagio INTEGER NOT NULL CHECK (numero_estagio BETWEEN 1 AND 5),
            descricao TEXT NOT NULL,
            UNIQUE(direito_id, numero_estagio)
        )
    """,

    'serie_faixa_etaria': """  
        CREATE TABLE IF NOT EXISTS serie_faixa_etaria (
            serie_id INTEGER NOT NULL REFERENCES ano_series(id),
            faixa_etaria_id INTEGER NOT NULL REFERENCES faixas_etarias(id),
            PRIMARY KEY (serie_id, faixa_etaria_id)
        )
    """,
    'avaliacoes': """
        CREATE TABLE IF NOT EXISTS avaliacoes (
            aluno_matricula VARCHAR(20) REFERENCES alunos(matricula),
            direito_id INTEGER REFERENCES direitos_aprendizagem(id),
            estagio_numero INTEGER NOT NULL CHECK (estagio_numero BETWEEN 1 AND 5),
            avaliador_id VARCHAR(20) REFERENCES professores(id_plurall),
            data_avaliacao TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (aluno_matricula, direito_id)
        )
    """,
    'disciplinas': """
        CREATE TABLE IF NOT EXISTS disciplinas (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(255) NOT NULL,
            segmento_id INTEGER NOT NULL REFERENCES segmentos(id),
            ano_serie_id INTEGER NOT NULL REFERENCES ano_series(id),
            UNIQUE(nome, segmento_id, ano_serie_id)
        )
    """,
    
    'unidades_tematicas': """
        CREATE TABLE IF NOT EXISTS unidades_tematicas (
            id SERIAL PRIMARY KEY,
            disciplina_id INTEGER NOT NULL REFERENCES disciplinas(id),
            nome VARCHAR(255) NOT NULL,
            praticas_linguagem TEXT,
            objeto_conhecimento TEXT,
            UNIQUE(disciplina_id, nome)
        )
    """,
    
    'habilidades': """
        CREATE TABLE IF NOT EXISTS habilidades (
            id SERIAL PRIMARY KEY,
            unidade_tematica_id INTEGER NOT NULL REFERENCES unidades_tematicas(id),
            descricao TEXT NOT NULL,
            UNIQUE(unidade_tematica_id, descricao)
        )
    """,
    
    'estagios_habilidades': """
        CREATE TABLE IF NOT EXISTS estagios_habilidades (
            id SERIAL PRIMARY KEY,
            habilidade_id INTEGER NOT NULL REFERENCES habilidades(id),
            numero_estagio INTEGER NOT NULL CHECK (numero_estagio BETWEEN 1 AND 5),
            descricao TEXT NOT NULL,
            UNIQUE(habilidade_id, numero_estagio)
        )
    """
}

def criar_tabela(nome_tabela):
    """Cria uma tabela específica"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute(TABELAS[nome_tabela])
            conn.commit()
            print(f"✅ Tabela {nome_tabela} criada/verificada")
    except Exception as e:
        print(f"❌ Erro ao criar {nome_tabela}: {str(e)}")
    finally:
        if conn: conn.close()


def relacionar_series_faixas():
    """Relaciona séries com faixas etárias conforme a estrutura oficial"""
    mapeamento = {
        # Série: Faixa etária correspondente
        'Berçário': '0 a 1 ano',
        'Mini Maternal': '2 anos',
        'Nível 1': '3 anos',      # Maternal
        'Nível 3': '3 anos',      # Maternal (alternativo)
        'Nível 2': '4 anos',      # Fase I
        'Nível 4': '4 anos',      # Fase I (alternativo)
        'Nível 5': '5 anos'       # Fase II
    }
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # 1. Garantir que todas as faixas etárias existam
        faixas_unicas = list(set(mapeamento.values()))
        for faixa in faixas_unicas:
            cur.execute("""
                INSERT INTO faixas_etarias (descricao)
                VALUES (%s) 
                ON CONFLICT (descricao) DO NOTHING
            """, (faixa,))
        
        # 2. Garantir que todas as séries existam
        series_unicas = list(mapeamento.keys())
        for serie in series_unicas:
            cur.execute("""
                INSERT INTO ano_series (nome)
                VALUES (%s)
                ON CONFLICT (nome) DO NOTHING
            """, (serie,))
        
        # 3. Criar os relacionamentos
        contador = 0
        for serie, faixa in mapeamento.items():
            # Obter ID da série
            cur.execute("SELECT id FROM ano_series WHERE nome = %s", (serie,))
            serie_id = cur.fetchone()
            
            # Obter ID da faixa etária
            cur.execute("SELECT id FROM faixas_etarias WHERE descricao = %s", (faixa,))
            faixa_id = cur.fetchone()
            
            if serie_id and faixa_id:
                cur.execute("""
                    INSERT INTO serie_faixa_etaria (serie_id, faixa_etaria_id)
                    VALUES (%s, %s)
                    ON CONFLICT (serie_id, faixa_etaria_id) DO NOTHING
                """, (serie_id[0], faixa_id[0]))
                contador += cur.rowcount
        
        conn.commit()
        #print(f"✅ {contador} relacionamentos criados/atualizados")
        #print("📊 Mapeamento aplicado:")
        #for serie, faixa in mapeamento.items():
         #   print(f"  {serie.ljust(15)} → {faixa}")
            
    except Exception as e:
        print(f"❌ Erro ao criar relacionamentos: {str(e)}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()





def importar_escolas(caminho_arquivo):
    """Importa escolas da aba Alunos"""
    try:
        df = pd.read_excel(caminho_arquivo, sheet_name='Alunos')
        escolas = df[['ID Escola', 'Escola']].drop_duplicates()

        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            for _, row in escolas.iterrows():
                cur.execute("""
                    INSERT INTO escolas (id_plurall, nome)
                    VALUES (%s, %s)
                    ON CONFLICT (id_plurall, nome) DO NOTHING
                """, (str(row['ID Escola']), row['Escola']))
            conn.commit()
            print(f"🎉 {len(escolas)} escolas importadas")
    except Exception as e:
        print(f"❌ Erro importar escolas: {str(e)}")
    finally:
        if conn: conn.close()
def importar_avaliacoes(caminho_arquivo):
    """Importa dados de avaliação do arquivo Somos com logs detalhados"""
    try:
        print("\n=== INÍCIO DA IMPORTAÇÃO DE AVALIAÇÕES ===")
        df = pd.read_excel(caminho_arquivo)
        print(f"📦 Total de linhas no arquivo: {len(df)}")
        
        # Limpeza de cabeçalhos
        df.columns = [col.strip().replace('"', '').replace('\n', '') for col in df.columns]
        print("🔍 Cabeçalhos processados:", df.columns.tolist())
        
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            for index, row in df.iterrows():
               # print(f"\n📝 Processando linha {index + 1}/{len(df)}")
                
                # Verificar dados obrigatórios
                required_fields = ['Verbo', 'Faixa Etária', 'Campo de experiência']
                missing = [field for field in required_fields if pd.isna(row[field])]
                
                if missing:
                    print(f"⛔ Dados obrigatórios faltando: {missing} - Linha ignorada")
                    continue
                
                # Converter valores para string
                verbo = str(row['Verbo']).strip()
                faixa = str(row['Faixa Etária']).strip()
                campo = str(row['Campo de experiência']).strip()
                
                #print(f"🔸 Verbo: '{verbo}'")
                #print(f"🔸 Faixa: '{faixa}'")
               # print(f"🔸 Campo: '{campo}'")
                
                # Processar Verbo
                try:
                    cur.execute(
                        "INSERT INTO verbos (nome) VALUES (%s) ON CONFLICT (nome) DO NOTHING RETURNING id",
                        (verbo,)
                    )
                    result = cur.fetchone()
                    if result:
                        verbo_id = result[0]
                       # print(f"  ✅ Verbo inserido - ID: {verbo_id}")
                    else:
                        cur.execute("SELECT id FROM verbos WHERE nome = %s", (verbo,))
                        verbo_id = cur.fetchone()[0]
                       # print(f"  🔄 Verbo existente - ID: {verbo_id}")
                except Exception as e:
                    print(f"❌ Erro no processamento de verbo: {str(e)}")
                    raise
                
                # Processar Faixa Etária
                try:
                    cur.execute(
                        "INSERT INTO faixas_etarias (descricao) VALUES (%s) ON CONFLICT (descricao) DO NOTHING RETURNING id",
                        (faixa,)
                    )
                    result = cur.fetchone()
                    if result:
                        faixa_id = result[0]
                       # print(f"  ✅ Faixa inserida - ID: {faixa_id}")
                    else:
                        cur.execute("SELECT id FROM faixas_etarias WHERE descricao = %s", (faixa,))
                        faixa_id = cur.fetchone()[0]
                        #print(f"  🔄 Faixa existente - ID: {faixa_id}")
                except Exception as e:
                    print(f"❌ Erro no processamento de faixa etária: {str(e)}")
                    raise
                
                # Processar Campo de Experiência
                try:
                    cur.execute(
                        "INSERT INTO campos_experiencia (nome) VALUES (%s) ON CONFLICT (nome) DO NOTHING RETURNING id",
                        (campo,)
                    )
                    result = cur.fetchone()
                    if result:
                        campo_id = result[0]
                        #print(f"  ✅ Campo inserido - ID: {campo_id}")
                    else:
                        cur.execute("SELECT id FROM campos_experiencia WHERE nome = %s", (campo,))
                        campo_id = cur.fetchone()[0]
                        #print(f"  🔄 Campo existente - ID: {campo_id}")
                except Exception as e:
                    print(f"❌ Erro no processamento de campo: {str(e)}")
                    raise
                
                # Processar Direito de Aprendizagem
                direito_desc = str(row['Direitos de Aprendizagem']).strip() if pd.notna(row['Direitos de Aprendizagem']) else ''
                #print(f"🔸 Direito: '{direito_desc}'")
                
                try:
                    cur.execute(
                        """INSERT INTO direitos_aprendizagem 
                        (verbo_id, faixa_etaria_id, campo_experiencia_id, descricao)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (verbo_id, faixa_etaria_id, campo_experiencia_id, descricao) 
                        DO NOTHING RETURNING id""",
                        (verbo_id, faixa_id, campo_id, direito_desc)
                    )
                    result = cur.fetchone()
                    if result:
                        direito_id = result[0]
                       # print(f"  ✅ Direito inserido - ID: {direito_id}")
                    else:
                        cur.execute(
                            """SELECT id FROM direitos_aprendizagem 
                            WHERE verbo_id = %s AND faixa_etaria_id = %s 
                            AND campo_experiencia_id = %s AND descricao = %s""",
                            (verbo_id, faixa_id, campo_id, direito_desc)
                        )
                        direito_id = cur.fetchone()[0]
                        #print(f"  🔄 Direito existente - ID: {direito_id}")
                except Exception as e:
                    print(f"❌ Erro no processamento de direito: {str(e)}")
                    raise
                
                # Processar Estágios
                #print("🔸 Processando estágios:")
                for col in ['Estágio 5', 'Estágio 4', 'Estágio 3', 'Estágio 2', 'Estágio 1']:
                    if pd.notna(row[col]):
                        numero = int(col.split()[1])
                        descricao = str(row[col]).strip()
                        try:
                            cur.execute(
                                """INSERT INTO estagios 
                                (direito_id, numero_estagio, descricao)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (direito_id, numero_estagio) 
                                DO UPDATE SET descricao = EXCLUDED.descricao""",
                                (direito_id, numero, descricao)
                            )
                           # print(f"  ✅ Estágio {numero}: {'Inserido' if cur.rowcount > 0 else 'Atualizado'}")
                        except Exception as e:
                            print(f"❌ Erro no estágio {numero}: {str(e)}")
                            raise
            
            conn.commit()
            print(f"\n✅✅✅ Importação concluída! {len(df)} linhas processadas ✅✅✅")
            
    except Exception as e:
        print(f"\n❌❌❌ ERRO CRÍTICO: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
    finally:
        if 'conn' in locals() and conn:
            conn.close()
        print("=== FIM DA IMPORTAÇÃO ===")
def importar_segmentos(caminho_arquivo):
    """Importa segmentos da aba Alunos"""
    try:
        df = pd.read_excel(caminho_arquivo, sheet_name='Alunos')
        segmentos = df['Segmento'].drop_duplicates().dropna()

        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            for segmento in segmentos:
                cur.execute("""
                    INSERT INTO segmentos (nome)
                    VALUES (%s)
                    ON CONFLICT (nome) DO NOTHING
                """, (segmento.strip(),))
            conn.commit()
            print(f"🎉 {len(segmentos)} segmentos importados")
    except Exception as e:
        print(f"❌ Erro importar segmentos: {str(e)}")
    finally:
        if conn: conn.close()
def importar_turmas(caminho_arquivo):
    """Importa turmas com tratamento para ano/série"""
    try:
        df = pd.read_excel(caminho_arquivo, sheet_name='Alunos')
        turmas = df[['ID Escola', 'Segmento', 'Ano/Série', 'Turma']].drop_duplicates()

        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            # Primeiro processa os anos/séries
            anos_series = df['Ano/Série'].astype(str).unique()
            for ano in anos_series:
                cur.execute(
                    "INSERT INTO ano_series (nome) VALUES (%s) ON CONFLICT (nome) DO NOTHING",
                    (ano.strip(),)
                )
            
            # Agora processa as turmas
            for _, row in turmas.iterrows():
                try:
                    # Obter IDs
                    cur.execute(
                        "SELECT id FROM escolas WHERE id_plurall = %s", 
                        (str(row['ID Escola']),)  # <-- Corrigido aqui
                    )
                    escola_id = cur.fetchone()[0]
                    
                    cur.execute(
                        "SELECT id FROM segmentos WHERE nome = %s", 
                        (row['Segmento'],)  # <-- Corrigido aqui
                    )
                    segmento_id = cur.fetchone()[0]

                    cur.execute(
                        "SELECT id FROM ano_series WHERE nome = %s", 
                        (str(row['Ano/Série']),)  # <-- Corrigido aqui
                    )
                    ano_serie_id = cur.fetchone()[0]

                    # Inserir turma
                    cur.execute(
                        """INSERT INTO turmas 
                        (escola_id, segmento_id, ano_serie_id, nome)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (escola_id, segmento_id, ano_serie_id, nome) DO NOTHING""",
                        (escola_id, segmento_id, ano_serie_id, row['Turma'])
                    )
                    
                except Exception as e:
                    print(f"⚠️ Erro na linha {_}: {str(e)}")
                    continue
            
            conn.commit()
            print(f"🎉 {len(turmas)} turmas processadas | {len(anos_series)} anos/séries catalogados")
            
    except Exception as e:
        print(f"❌ Erro importar turmas: {str(e)}")
    finally:
        if conn: 
            conn.close()

def importar_alunos(caminho_arquivo):
    """Importa alunos da aba Alunos"""
    try:
        df = pd.read_excel(caminho_arquivo, sheet_name='Alunos')
        print(f"📂 Planilha carregada com {len(df)} alunos")

        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            for index, row in df.iterrows():
#                print(f"🔍 Processando aluno {index + 1}/{len(df)}: {row['Aluno']}")

                # Query corrigida com JOIN na tabela ano_series
                cur.execute("""
                    SELECT t.id 
                    FROM turmas t
                    JOIN escolas e ON t.escola_id = e.id
                    JOIN segmentos s ON t.segmento_id = s.id
                    JOIN ano_series a ON t.ano_serie_id = a.id
                    WHERE e.id_plurall = %s
                    AND s.nome = %s
                    AND a.nome = %s
                    AND t.nome = %s
                """, (
                    str(row['ID Escola']), 
                    row['Segmento'], 
                    str(row['Ano/Série']), 
                    row['Turma']
                ))
                
                turma_id = cur.fetchone()
                
                if not turma_id:
                    print(f"⚠️ Turma não encontrada para {row['Aluno']}: {row['ID Escola']} - {row['Segmento']} - {row['Ano/Série']} - {row['Turma']}")
                    continue
                
                turma_id = turma_id[0]

                # Inserir aluno
                cur.execute("""
                    INSERT INTO alunos (matricula, nome, id_plurall, turma_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (matricula) DO UPDATE SET
                        nome = EXCLUDED.nome,
                        id_plurall = EXCLUDED.id_plurall,
                        turma_id = EXCLUDED.turma_id
                """, (
                    row['Matrícula'], 
                    row['Aluno'], 
                    row['ID Plurall'], 
                    turma_id
                ))
            
            conn.commit()
            print(f"✅ {len(df)} alunos processados")
    except Exception as e:
        print(f"❌ Erro importar alunos: {str(e)}")
    finally:
        if conn: 
            conn.close()
def importar_professores(caminho_arquivo):
    """Importa professores com relação muitos-para-muitos com escolas"""
    try:
        df = pd.read_excel(caminho_arquivo, sheet_name='Professores')
        
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                # Verificar se a escola existe
                cur.execute("SELECT id FROM escolas WHERE id_plurall = %s", (str(row['ID Escola']),))
                if not (escola := cur.fetchone()):
                    continue  # Pula professores de escolas não cadastradas
                
                escola_id = escola[0]
                
                # Obter segmento_id se existir
                segmento_id = None
                if pd.notna(row['Segmento']):
                    cur.execute("SELECT id FROM segmentos WHERE nome = %s", (row['Segmento'],))
                    if (seg := cur.fetchone()):
                        segmento_id = seg[0]

                # Inserir/atualizar professor (sem escola_id agora)
                cur.execute("""
                    INSERT INTO professores (id_plurall, nome, segmento_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id_plurall) DO UPDATE SET
                        nome = EXCLUDED.nome,
                        segmento_id = EXCLUDED.segmento_id
                """, (row['ID Plurall'], row['Professor'], segmento_id))
                
                # Criar relação professor-escola
                cur.execute("""
                    INSERT INTO professor_escola (professor_id, escola_id)
                    VALUES (%s, %s)
                    ON CONFLICT (professor_id, escola_id) DO NOTHING
                """, (row['ID Plurall'], escola_id))
            
            conn.commit()
            print(f"🎉 {len(df)} professores processados com relação a escolas")
    except Exception as e:
        print(f"❌ Erro importar professores: {str(e)}")
    finally:
        if conn: conn.close()

#fundamental 
def importar_fundamental1(caminho_arquivo):
    """Importa dados do Fundamental I com tratamento robusto de dados"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Verificar pré-requisitos
        cur.execute("SELECT id FROM segmentos WHERE id = 2")
        if not cur.fetchone():
            raise Exception("Segmento ID 2 (Fundamental) não encontrado!")
            
        cur.execute("SELECT id FROM ano_series WHERE id = 4")
        if not cur.fetchone():
            raise Exception("Ano/Série ID 4 não encontrado!")

        xls = pd.ExcelFile(caminho_arquivo)
        #print(f"\n🔍 Arquivo aberto: {len(xls.sheet_names)} abas encontradas")
        
        for sheet_name in xls.sheet_names:
            if sheet_name.lower() in ['alunos', 'professores']:
                print(f"⏩ Aba {sheet_name} ignorada")
                continue
                
            print(f"\n📘 Processando aba: {sheet_name}")
            
            try:
                df = pd.read_excel(xls, sheet_name=sheet_name).fillna('')
                df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
                #print(f"🔍 Colunas detectadas: {df.columns.tolist()}")
                
                # Verificar colunas obrigatórias
                required_columns = ['unidade_temática', 'habilidades']
                if not all(col in df.columns for col in required_columns):
                    missing = [col for col in required_columns if col not in df.columns]
                    print(f"⛔ Colunas obrigatórias faltando: {missing} - Aba ignorada")
                    continue
                    
                # Registrar disciplina
                cur.execute("""
                    INSERT INTO disciplinas (nome, segmento_id, ano_serie_id)
                    VALUES (%s, 2, 4)
                    ON CONFLICT (nome, segmento_id, ano_serie_id) 
                    DO UPDATE SET nome = EXCLUDED.nome
                    RETURNING id
                """, (sheet_name,))
                disciplina_id = cur.fetchone()[0]
                #print(f"📝 Disciplina ID: {disciplina_id}")
                
                success_count = 0
                error_count = 0
                
                for idx, row in df.iterrows():
                    try:
                        # Extrair e validar dados
                        unidade = str(row['unidade_temática']).strip()
                        habilidade = str(row['habilidades']).strip()
                        
                        if not unidade or not habilidade:
                            print(f"⚠️ Linha {idx+1}: Unidade ou Habilidade vazia")
                            error_count += 1
                            continue
                            
                        pratica = str(row.get('práticas_de_linguagens', '')).strip() or None
                        objeto = str(row.get('objeto_de_conhecimento', '')).strip() or None
                        
                        # Debug detalhado
                        #print(f"\n📝 Linha {idx+1}")
                        #print(f"  Unidade: {unidade}")
                        #print(f"  Habilidade: {habilidade[:50]}...")  # Mostra parte inicial
                        
                        # Unidade Temática
                        cur.execute("""
                            INSERT INTO unidades_tematicas 
                            (disciplina_id, nome, praticas_linguagem, objeto_conhecimento)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (disciplina_id, nome) 
                            DO UPDATE SET
                                praticas_linguagem = COALESCE(EXCLUDED.praticas_linguagem, unidades_tematicas.praticas_linguagem),
                                objeto_conhecimento = COALESCE(EXCLUDED.objeto_conhecimento, unidades_tematicas.objeto_conhecimento)
                            RETURNING id
                        """, (disciplina_id, unidade, pratica, objeto))
                        unidade_id = cur.fetchone()[0]
                        
                        # Habilidade
                        cur.execute("""
                            INSERT INTO habilidades (unidade_tematica_id, descricao)
                            VALUES (%s, %s)
                            ON CONFLICT (unidade_tematica_id, descricao) 
                            DO UPDATE SET descricao = EXCLUDED.descricao
                            RETURNING id
                        """, (unidade_id, habilidade))
                        hab_id = cur.fetchone()[0]
                        
                        # Estágios
                        for num in range(1,6):
                            col_name = f'estágio_{num}'
                            if col_name in df.columns:
                                estagio = str(row[col_name]).strip()
                                if estagio:
                                    cur.execute("""
                                        INSERT INTO estagios_habilidades 
                                        (habilidade_id, numero_estagio, descricao)
                                        VALUES (%s, %s, %s)
                                        ON CONFLICT (habilidade_id, numero_estagio) 
                                        DO UPDATE SET descricao = EXCLUDED.descricao
                                    """, (hab_id, num, estagio))
                                    #print(f"  ✅ Estágio {num}: {estagio[:30]}...")
                                    
                        success_count += 1
                        
                    except Exception as e:
                        error_count += 1
                        print(f"❌ Erro linha {idx+1}: {str(e)}")
                        conn.rollback()  # Rollback apenas da linha
                        
                conn.commit()
                #print(f"\n✅ {sheet_name}: {success_count} linhas importadas | {error_count} erros")
                
            except Exception as e:
                print(f"❌ Erro crítico na aba: {str(e)}")
                conn.rollback()
            
    except Exception as e:
        print(f"\n❌ ERRO GLOBAL: {str(e)}")
        if 'conn' in locals(): conn.rollback()
    finally:
        if 'conn' in locals(): conn.close()


if __name__ == "__main__":
    tabelas_ordenadas = [
        # Tabelas base
        'escolas', 'segmentos', 'ano_series', 'faixas_etarias',
        
        # Tabelas dependentes de escolas/segmentos
        'turmas', 'alunos', 'professores', 'professor_escola',
        
        # Tabelas curriculares
        'verbos', 'campos_experiencia', 
        'direitos_aprendizagem', 'estagios',
        'avaliacoes', 'serie_faixa_etaria',
        'disciplinas',
        'unidades_tematicas',
        'habilidades',
        'estagios_habilidades'
        
        
    ]
    
    # Crie todas as tabelas primeiro (DESCOMENTE ESTE BLOCO)
    for tabela in tabelas_ordenadas:
        criar_tabela(tabela)
    
    # Importar dados
    importar_escolas('dados_atualizados.xlsx')
    importar_segmentos('dados_atualizados.xlsx')
    importar_turmas('dados_atualizados.xlsx')
    importar_alunos('dados_atualizados.xlsx')
    importar_professores('dados_atualizados.xlsx')
    importar_avaliacoes('Rubricas Ed Infantil V2 2025.xlsx')  # Novo arquivo
    # Após criar todas as tabelas:
    
    relacionar_series_faixas()  # Executar após criar tabelas
    # Após criar todas as tabelas:
    importar_fundamental1('Rubricas 1º ano.xlsx')
  
   