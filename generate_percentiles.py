import pandas as pd
import duckdb
import os
import re
import json
import time

# =============================================================================
# CONFIGURAÇÃO
# =============================================================================
SOURCE_SOLUTIONS_DIR = 'generated_solutions'
OUTPUT_DIR = 'docs/data'
DATABASES_OUTPUT_DIR = 'databases'
MAIN_DB_PATH = os.path.join(DATABASES_OUTPUT_DIR, 'percentiles.duckdb')

STAT_COLUMNS = [
    "total_houses", "total_girls", "total_boys", "total_dogs",
    "total_agents", "total_captured_aliens",
    "total_curves", "total_tiles_without_roads", "total_roads",
    "max_aliens_running_towards_agent", "max_hamburgers_in_front_of_alien",
    "max_agents_on_one_road", "max_aliens_on_one_road", "max_aliens_between_two_agents",
    "total_food_chain_sets", "longest_road_size", "max_roads_of_same_length",
    "aliens_times_ufos", "aliens_times_hamburgers", "citizen_dog_pairs",
    "largest_dog_group", "largest_house_group", "largest_citizen_group",
    "largest_safe_zone_size"
]

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def find_latest_solution_file(directory, base_name="tiling_solutions", extension="parquet"):
    if not os.path.isdir(directory):
        return None, f"Diretório '{directory}' não encontrado."
    pattern = re.compile(rf"{base_name}_(\d+)\.{extension}")
    highest_index = -1
    latest_file_path = None
    for filename in os.listdir(directory):
        match = pattern.match(filename)
        if match:
            index = int(match.group(1))
            if index > highest_index:
                highest_index = index
                latest_file_path = os.path.join(directory, filename)
    if latest_file_path:
        return latest_file_path, None
    else:
        return None, f"Nenhum arquivo encontrado em '{directory}'."

# =============================================================================
# PROCESSAMENTO DE DADOS
# =============================================================================

def create_db_from_parquet(parquet_file_path):
    os.makedirs(DATABASES_OUTPUT_DIR, exist_ok=True)
    con = duckdb.connect(MAIN_DB_PATH)
    con.execute("PRAGMA memory_limit='20GB';")
    con.execute(f"PRAGMA threads={os.cpu_count()};")
    con.execute("PRAGMA enable_progress_bar=true;")

    print(f"🚀 Criando/Atualizando VIEW virtual para '{parquet_file_path}'...")
    
    select_clauses = [f'CAST("{col}" AS UTINYINT) AS "{col}"' for col in STAT_COLUMNS]
    
    create_view_query = f"""
        CREATE OR REPLACE VIEW solutions AS
        SELECT {", ".join(select_clauses)}
        FROM read_parquet('{parquet_file_path}');
    """
    
    con.execute(create_view_query)
    print("✅ View 'solutions' atualizada com sucesso.")
    con.close()

def calculate_percentiles():
    print("\n🚀 Calculando classificações percentis...")
    con = duckdb.connect(MAIN_DB_PATH, read_only=False)
    con.execute("PRAGMA memory_limit='20GB';")
    con.execute(f"PRAGMA threads={os.cpu_count()};")
    con.execute("PRAGMA enable_progress_bar=true;")
    
    con.execute("CREATE OR REPLACE TABLE stat_percentiles (stat_name VARCHAR, stat_value UTINYINT, frequency UBIGINT, percentile REAL);")

    union_queries = []
    
    for stat in STAT_COLUMNS:
        query = f"""
        SELECT 
            '{stat}' AS stat_name,
            "{stat}" AS stat_value,
            COUNT(*) AS frequency,
            CAST(
                (SUM(COUNT(*)) OVER (ORDER BY "{stat}" ASC) * 100.0) 
                / 
                SUM(COUNT(*)) OVER ()
            AS REAL) AS percentile
        FROM solutions
        GROUP BY "{stat}"
        """
        union_queries.append(query)

    full_union_query = "\nUNION ALL\n".join(union_queries)
    
    insert_query = f"""
        INSERT INTO stat_percentiles
        {full_union_query};
    """
    
    con.execute(insert_query)
    con.close()
    print("✅ Tabela 'stat_percentiles' calculada e salva.")

def export_percentiles_to_json():
    print("\n🚀 Exportando classificações percentis para JSON...")
    con = duckdb.connect(MAIN_DB_PATH, read_only=True)
    percentiles_df = con.execute("SELECT stat_name, stat_value, percentile FROM stat_percentiles").fetchdf()
    
    percentiles_json = {
        stat: dict(zip(group['stat_value'].astype(str), group['percentile']))
        for stat, group in percentiles_df.groupby('stat_name')
    }
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    json_path = os.path.join(OUTPUT_DIR, 'percentiles.json')
    with open(json_path, 'w') as f:
        json.dump(percentiles_json, f, indent=2)
    print(f"✅ Arquivo salvo em '{json_path}'.")
    con.close()

# =============================================================================
# EXECUTOR PRINCIPAL
# =============================================================================
def main():
    start_time = time.time()
    print("=" * 50)
    print("ETAPA 1: GERAÇÃO DE CLASSIFICAÇÕES PERCENTIS")
    print("=" * 50)

    parquet_file, error = find_latest_solution_file(SOURCE_SOLUTIONS_DIR)
    if error:
        print(f"❌ ERRO: {error}")
        return

    create_db_from_parquet(parquet_file)
    calculate_percentiles()
    export_percentiles_to_json()

    print("\n" + "=" * 50)
    print(f"✅ Processo concluído em {(time.time() - start_time) / 60:.2f} minutos.")

if __name__ == "__main__":
    main()