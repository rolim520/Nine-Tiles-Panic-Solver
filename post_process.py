import pandas as pd
import duckdb
import itertools
import os, re
import json
import sys
import time
# A importação da PIL não é mais necessária

# =============================================================================
# CONFIGURAÇÃO (ATUALIZADA)
# =============================================================================
GAME_CARDS_PATH = 'game/cards/cards.json'
SOURCE_SOLUTIONS_DIR = 'generated_solutions'

# O diretório de saída para os arquivos JSON finais
SOLUTIONS_OUTPUT_DIR = 'solutions'
DATABASES_OUTPUT_DIR = 'databases'
TEMP_DIR = 'temp'

# O único arquivo de banco de dados usado para todo o processamento
MAIN_DB_PATH = os.path.join(DATABASES_OUTPUT_DIR, 'solutions.duckdb')

# Os caminhos para os arquivos de DB separados foram removidos

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
# FUNÇÕES AUXILIARES (sem a parte de imagem)
# =============================================================================

def find_latest_solution_file(directory, base_name="tiling_solutions", extension="parquet"):
    if not os.path.isdir(directory):
        return None, f"Diretório de soluções '{directory}' não encontrado."
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
        return None, f"Nenhum arquivo de solução (ex: '{base_name}_1.{extension}') encontrado em '{directory}'."

# Funções relacionadas à geração de imagem foram removidas
# (sanitize_folder_name, init_worker, load_and_rotate_tile_image, etc.)

# =============================================================================
# FUNÇÕES DE PROCESSAMENTO DE DADOS (ATUALIZADAS)
# =============================================================================

def create_db_from_parquet(parquet_file_path):
    os.makedirs(DATABASES_OUTPUT_DIR, exist_ok=True)
    con = duckdb.connect(MAIN_DB_PATH)
    con.execute("PRAGMA memory_limit='20GB';")
    con.execute(f"PRAGMA threads={os.cpu_count()};")
    con.execute("PRAGMA enable_progress_bar=true;")

    try:
        views_df = con.execute("SELECT view_name FROM duckdb_views();").fetchdf()
        view_exists = 'solutions' in views_df['view_name'].values
    except duckdb.Error:
        view_exists = False

    if view_exists:
        print(f"✅ View 'solutions' já existe em '{MAIN_DB_PATH}'. Pulando a criação.")
        con.close()
        return

    print(f"🚀 Criando uma VIEW otimizada 'solutions' para '{parquet_file_path}'...")
    all_parquet_columns = STAT_COLUMNS[:]
    for r in range(3):
        for c in range(3):
            pos = f"{r}{c}"
            all_parquet_columns.extend([f"piece_{pos}", f"side_{pos}", f"orient_{pos}"])
    
    select_clauses = ["ROW_NUMBER() OVER () AS solution_id"]
    for col in all_parquet_columns:
        select_clauses.append(f'CAST("{col}" AS UTINYINT) AS "{col}"')
    
    select_clauses_sql = ", ".join(select_clauses)
    
    create_view_query = f"""
        CREATE OR REPLACE VIEW solutions AS
        SELECT {select_clauses_sql}
        FROM read_parquet('{parquet_file_path}');
    """
    
    try:
        con.execute(create_view_query)
        print("✅ View 'solutions' criada com sucesso.")
    except Exception as e:
        print(f"❌ ERRO durante a criação da view: {e}")
        con.close()
        sys.exit(1)
        
    con.close()

def calculate_percentiles():
    print("\n🚀 Calculando percentis e salvando em 'solutions.duckdb'...")
    # Conecta ao DB principal em modo de leitura e escrita
    con = duckdb.connect(MAIN_DB_PATH, read_only=False)
    con.execute("PRAGMA memory_limit='20GB';")
    con.execute(f"PRAGMA threads={os.cpu_count()};")
    con.execute("PRAGMA enable_progress_bar=true;")
    
    # Cria a tabela de percentis DENTRO do banco de dados principal
    con.execute("CREATE OR REPLACE TABLE stat_percentiles (stat_name VARCHAR, stat_value UTINYINT, frequency UBIGINT, percentile REAL);")

    stat_columns_list = ", ".join([f'"{col}"' for col in STAT_COLUMNS])

    unpivot_query = f"""
        INSERT INTO stat_percentiles
        WITH ValueCounts AS (
            SELECT stat_name, stat_value, COUNT(*) as frequency
            FROM (UNPIVOT solutions ON {stat_columns_list} INTO NAME stat_name VALUE stat_value) AS unpivoted_data
            GROUP BY stat_name, stat_value
        )
        SELECT
            stat_name,
            CAST(stat_value AS UTINYINT),
            CAST(frequency AS UBIGINT),
            CAST((PERCENT_RANK() OVER (PARTITION BY stat_name ORDER BY stat_value)) * 100 AS REAL) as percentile
        FROM ValueCounts;
    """
    
    con.execute(unpivot_query)
    con.close()
    print(f"✅ Tabela 'stat_percentiles' salva em '{MAIN_DB_PATH}'.")

def precompute_all_scores(game_cards):
    print("\n🚀 Pré-calculando scores e salvando em 'solutions.duckdb'...")
    con = duckdb.connect(MAIN_DB_PATH, read_only=False)
    con.execute("PRAGMA memory_limit='20GB';")
    con.execute(f"PRAGMA threads={os.cpu_count()};")
    con.execute("PRAGMA enable_progress_bar=true;")
    os.makedirs(TEMP_DIR, exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{TEMP_DIR}';")
    
    # Não precisa mais de ATTACH, a tabela já está no banco de dados principal
    con.execute("DROP TABLE IF EXISTS solution_scores;")
    scorable_cards = [card for card in game_cards if card.get('key')]
    
    select_clauses = ["s.solution_id"]
    join_clauses = []
    for i, card in enumerate(scorable_cards):
        key, card_num, card_type = card['key'], card['number'], card['type']
        alias = f"p{i}"
        score_logic = f"{alias}.percentile" if card_type == 'max' else f"100.0 - {alias}.percentile"
        select_clauses.append(f"CAST({score_logic} AS REAL) AS card_{card_num}_score")
        # A referência ao banco de dados 'percentiles_db' foi removida
        join_clauses.append(f"""LEFT JOIN stat_percentiles AS {alias} ON s."{key}" = {alias}.stat_value AND {alias}.stat_name = '{key}'""")
        
    select_sql = ", ".join(select_clauses)
    join_sql = "\n".join(join_clauses)
    score_columns = [f"card_{card['number']}_score" for card in scorable_cards]
    num_scorable_cards = len(score_columns)
    
    clean_scores = [f"GREATEST(0, COALESCE({col}, 0))" for col in score_columns]
    zero_check_sql = " OR ".join([f"{cs} <= 0.0001" for cs in clean_scores]) # Adicionado <= para maior robustez
    log_sum_sql = " + ".join([f"LN({cs})" for cs in clean_scores])
    stable_geometric_mean_sql = f"EXP(({log_sum_sql}) / {float(num_scorable_cards)})"
    
    super_score_sql = f"CASE WHEN {zero_check_sql} THEN 0.0 ELSE {stable_geometric_mean_sql} END"

    full_query = f"""
        CREATE TABLE solution_scores AS
        WITH ScoredSolutions AS (SELECT {select_sql} FROM solutions s {join_sql})
        SELECT *, CAST({super_score_sql} AS REAL) AS super_score FROM ScoredSolutions;
    """
    
    print("  -> Executando query para criar a tabela 'solution_scores'. Isso pode levar um tempo...")
    con.execute(full_query)
    con.close()
    print("✅ Tabela 'solution_scores' criada com sucesso em 'solutions.duckdb'.")

def _solutions_df_to_json_dict(df, key_cols):
    """Converte um DataFrame de soluções para o formato de dicionário JSON desejado."""
    solutions_dict = {}
    for _, row in df.iterrows():
        card_keys = sorted([int(row[c]) for c in key_cols])
        key = "_".join(map(str, card_keys))
        
        solution_data = {f"p{r}{c}": [int(row[f"piece_{r}{c}"]), int(row[f"side_{r}{c}"]), int(row[f"orient_{r}{c}"])] for r in range(3) for c in range(3)}
        solutions_dict[key] = solution_data
    return solutions_dict

def find_and_export_best_solutions_as_json(game_cards):
    """Encontra as melhores soluções e exporta os resultados como arquivos JSON."""
    print("\n🚀 Encontrando melhores soluções e preparando para exportação JSON...")
    
    con = duckdb.connect(MAIN_DB_PATH, read_only=True)
    con.execute("PRAGMA memory_limit='20GB';")
    con.execute(f"PRAGMA threads={os.cpu_count()};")
    con.execute("PRAGMA enable_progress_bar=true;")

    scorable_card_ids = sorted([card['number'] for card in game_cards if card.get('key')])
    layout_columns_str = ", ".join([f's."piece_{r}{c}", s."side_{r}{c}", s."orient_{r}{c}"' for r in range(3) for c in range(3)])
    
    all_solutions_json = {}

    # --- 1. Melhores soluções para SINGLE cards ---
    print("  -> Processando single cards...")
    max_by_clauses = [f"max_by(ss.solution_id, ss.card_{cid}_score) AS id_{cid}" for cid in scorable_card_ids]
    best_ids_df = con.execute(f"SELECT {', '.join(max_by_clauses)} FROM solution_scores ss").fetchdf()
    unique_ids = set(best_ids_df.iloc[0].values)
    details_df = con.execute(f"SELECT s.solution_id, {layout_columns_str} FROM solutions s WHERE s.solution_id IN ({','.join(map(str, unique_ids))})").fetchdf()
    
    rows = []
    for card_id in scorable_card_ids:
        best_id = best_ids_df[f'id_{card_id}'][0]
        row_data = details_df[details_df['solution_id'] == best_id].iloc[0].to_dict()
        row_data['card_id'] = card_id
        rows.append(row_data)
    all_solutions_json.update(_solutions_df_to_json_dict(pd.DataFrame(rows), ['card_id']))

    # --- 2. Melhores soluções para PARES de cartões ---
    print("  -> Processando card pairs...")
    card_pairs = list(itertools.combinations(scorable_card_ids, 2))
    if card_pairs:
        max_by_clauses = [f"max_by(ss.solution_id, pow(ss.card_{c1}_score * ss.card_{c2}_score, 1/2.0)) AS id_{c1}_{c2}" for c1, c2 in card_pairs]
        best_ids_df = con.execute(f"SELECT {', '.join(max_by_clauses)} FROM solution_scores ss").fetchdf()
        unique_ids = set(best_ids_df.iloc[0].values)
        details_df = con.execute(f"SELECT s.solution_id, {layout_columns_str} FROM solutions s WHERE s.solution_id IN ({','.join(map(str, unique_ids))})").fetchdf()

        rows = []
        for c1, c2 in card_pairs:
            best_id = best_ids_df[f'id_{c1}_{c2}'][0]
            row_data = details_df[details_df['solution_id'] == best_id].iloc[0].to_dict()
            row_data['card_id_1'], row_data['card_id_2'] = c1, c2
            rows.append(row_data)
        all_solutions_json.update(_solutions_df_to_json_dict(pd.DataFrame(rows), ['card_id_1', 'card_id_2']))
    
    # --- 3. Melhores soluções para TRIOS de cartões ---
    print("  -> Processando card trios...")
    card_trios = list(itertools.combinations(scorable_card_ids, 3))
    if card_trios:
        max_by_clauses = [f"max_by(ss.solution_id, pow(ss.card_{c1}_score * ss.card_{c2}_score * ss.card_{c3}_score, 1/3.0)) AS id_{c1}_{c2}_{c3}" for c1, c2, c3 in card_trios]
        best_ids_df = con.execute(f"SELECT {', '.join(max_by_clauses)} FROM solution_scores ss").fetchdf()
        unique_ids = set(best_ids_df.iloc[0].values)
        details_df = con.execute(f"SELECT s.solution_id, {layout_columns_str} FROM solutions s WHERE s.solution_id IN ({','.join(map(str, unique_ids))})").fetchdf()

        rows = []
        for c1, c2, c3 in card_trios:
            best_id = best_ids_df[f'id_{c1}_{c2}_{c3}'][0]
            row_data = details_df[details_df['solution_id'] == best_id].iloc[0].to_dict()
            row_data['card_id_1'], row_data['card_id_2'], row_data['card_id_3'] = c1, c2, c3
            rows.append(row_data)
        all_solutions_json.update(_solutions_df_to_json_dict(pd.DataFrame(rows), ['card_id_1', 'card_id_2', 'card_id_3']))
    
    # --- Exportar best_solutions.json ---
    os.makedirs(SOLUTIONS_OUTPUT_DIR, exist_ok=True)
    json_path = os.path.join(SOLUTIONS_OUTPUT_DIR, 'best_solutions.json')
    with open(json_path, 'w') as f:
        json.dump(all_solutions_json, f)
    print(f"✅ Arquivo 'best_solutions.json' salvo em '{json_path}'.")

    # --- Exportar percentiles.json ---
    print("  -> Exportando percentis para JSON...")
    percentiles_df = con.execute("SELECT stat_name, stat_value, percentile FROM stat_percentiles").fetchdf()
    percentiles_json = {
        stat: dict(zip(group['stat_value'].astype(str), group['percentile']))
        for stat, group in percentiles_df.groupby('stat_name')
    }
    json_path = os.path.join(SOLUTIONS_OUTPUT_DIR, 'percentiles.json')
    with open(json_path, 'w') as f:
        json.dump(percentiles_json, f, indent=2)
    print(f"✅ Arquivo 'percentiles.json' salvo em '{json_path}'.")

    con.close()

# =============================================================================
# EXECUTOR PRINCIPAL (ATUALIZADO)
# =============================================================================
def main():
    """Orquestra todo o processo de pós-processamento."""
    start_time = time.time()
    print("=" * 50)
    print("INICIANDO SCRIPT DE PÓS-PROCESSAMENTO DE SOLUÇÕES")
    print("=" * 50)

    parquet_file, error = find_latest_solution_file(SOURCE_SOLUTIONS_DIR)
    if error:
        print(f"❌ ERRO: {error}")
        return
    print(f"✅ Arquivo de soluções encontrado: '{parquet_file}'")

    try:
        with open(GAME_CARDS_PATH, 'r') as f:
            game_cards = json.load(f)
        print(f"Carregados {len(game_cards)} cartões de '{GAME_CARDS_PATH}'.")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"ERRO ao carregar o arquivo de cartões: {e}")
        return

    create_db_from_parquet(parquet_file)
    calculate_percentiles()
    precompute_all_scores(game_cards)
    find_and_export_best_solutions_as_json(game_cards)

    end_time = time.time()
    print("\n" + "=" * 50)
    print("✅ Processo concluído com sucesso!")
    print(f"  -> Duração total: {(end_time - start_time) / 3600:.2f} horas.")
    print(f"  -> Banco de dados de análise salvo em: '{DATABASES_OUTPUT_DIR}/'")
    print(f"  -> Arquivos JSON de resultados salvos em: '{SOLUTIONS_OUTPUT_DIR}/'")
    print("=" * 50)

if __name__ == "__main__":
    main()