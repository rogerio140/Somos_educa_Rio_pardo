import psycopg2
from psycopg2.extras import execute_values

RENDER_CONFIG = {
   "host": "dpg-d0qcaaripnbc73e8l5ag-a.virginia-postgres.render.com",  # Host externo
   "port": "5432",
   "database": "somos_educa_2",
   "user": "somos_educa_2",
   "password": "Ta1s7uQ2pcB3RENMdsHskriJYJMKJK6j",
   "sslmode": "require"
}

LOCAL_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "postgres",
    "user": "postgres",
    "password": "RAEB449140",
    "sslmode": "disable"
}

def copy_database():
    with psycopg2.connect(**RENDER_CONFIG) as conn_remote, \
         psycopg2.connect(**LOCAL_CONFIG) as conn_local:

        cur_remote = conn_remote.cursor()
        cur_local = conn_local.cursor()

        # Lista as tabelas do schema public
        cur_remote.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_type='BASE TABLE';
        """)
        tables = cur_remote.fetchall()

        for (table_name,) in tables:
            print(f"Copiando tabela: {table_name}")

            # Obtém a estrutura da tabela (DDL)
            cur_remote.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'")
            columns_info = cur_remote.fetchall()

            column_names = [col for col, _ in columns_info]
            columns_str = ", ".join(column_names)

            # Cria a tabela localmente (drop se já existir)
            cur_local.execute(f'DROP TABLE IF EXISTS {table_name} CASCADE;')

            col_defs = ", ".join([f"{name} {dtype_mapping(dtype)}" for name, dtype in columns_info])
            create_sql = f"CREATE TABLE {table_name} ({col_defs});"
            cur_local.execute(create_sql)

            # Busca os dados
            cur_remote.execute(f"SELECT {columns_str} FROM {table_name}")
            rows = cur_remote.fetchall()

            if rows:
                placeholders = "(" + ", ".join(["%s"] * len(column_names)) + ")"
                insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"
                execute_values(cur_local, insert_sql, rows)

            conn_local.commit()

        print("Cópia finalizada com sucesso.")

def dtype_mapping(pg_type):
    # Mapear os tipos mais comuns
    mapping = {
        'integer': 'INTEGER',
        'text': 'TEXT',
        'character varying': 'VARCHAR',
        'boolean': 'BOOLEAN',
        'timestamp without time zone': 'TIMESTAMP',
        'timestamp with time zone': 'TIMESTAMPTZ',
        'double precision': 'DOUBLE PRECISION',
        'real': 'REAL',
        'numeric': 'NUMERIC',
        'date': 'DATE',
        'time without time zone': 'TIME'
    }
    return mapping.get(pg_type, 'TEXT')  # default para tipos desconhecidos

if __name__ == "__main__":
    copy_database()
