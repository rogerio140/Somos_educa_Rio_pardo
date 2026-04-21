# -*- coding: utf-8 -*-
"""
Created on Tue May 20 16:27:34 2025

@author: roger
"""

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


def drop_all_tables():
    try:
        conn = None  # Inicializa a variável conn como None
        # Conecta ao banco de dados
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True  # Necessário para executar DROP TABLE
        cursor = conn.cursor()

        # 1. Lista todas as tabelas do banco
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = cursor.fetchall()

        if not tables:
            print("Nenhuma tabela encontrada no banco de dados.")
            return

        # 2. Gera e executa os comandos DROP TABLE para cada tabela
        for table in tables:
            table_name = table[0]
            drop_query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                sql.Identifier(table_name)
            )
            cursor.execute(drop_query)
            print(f"Tabela '{table_name}' apagada com sucesso.")

        print("\nTodas as tabelas foram removidas!")

    except Exception as e:
        print(f"Erro ao apagar tabelas: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

# Executa a função
drop_all_tables()