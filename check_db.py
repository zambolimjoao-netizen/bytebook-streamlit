import sqlite3

def check_table_and_count_records(db_name="bytebook.db", table_name="pecas"):
    """
    Conecta ao banco de dados e verifica se a tabela existe,
    retornando a contagem de registros.
    """
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Verifica se a tabela existe
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if cursor.fetchone() is not None:
            print(f"A tabela '{table_name}' existe no banco de dados '{db_name}'.")

            # Conta os registros na tabela
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"Total de registros na tabela '{table_name}': {count}")

            # Opcional: Mostra os 5 primeiros registros
            print("\n--- Primeiros 5 Registros ---")
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
            records = cursor.fetchall()
            for record in records:
                print(record)
        else:
            print(f"A tabela '{table_name}' NÃO foi encontrada no banco de dados '{db_name}'.")

    except sqlite3.Error as e:
        print(f"Ocorreu um erro no banco de dados: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    check_table_and_count_records()