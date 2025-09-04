import multiprocessing
import pandas as pd
import duckdb
import itertools
import os, re
import json
import sys
import math
import time
from PIL import Image

# =============================================================================
# CONFIGURAÇÃO
# =============================================================================
GAME_CARDS_PATH = 'game/cards/cards.json'
TILE_IMAGES_DIR = 'game/tiles/tile_images'
SOURCE_SOLUTIONS_DIR = 'generated_solutions'

SOLUTIONS_OUTPUT_DIR = 'solutions'
DATABASES_OUTPUT_DIR = 'databases'
TEMP_DIR = 'temp' # Diretório para arquivos temporários do DuckDB

MAIN_DB_PATH = os.path.join(DATABASES_OUTPUT_DIR, 'solutions.duckdb')
PERCENTILES_DB_PATH = os.path.join(DATABASES_OUTPUT_DIR, 'percentiles.duckdb')
BEST_SOLUTIONS_DB_PATH = os.path.join(DATABASES_OUTPUT_DIR, 'best_solutions.duckdb')

MAPS_OUTPUT_DIR = SOLUTIONS_OUTPUT_DIR

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
# FUNÇÕES AUXILIARES (sem alterações)
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

def sanitize_folder_name(name):
    name = name.replace(' ', '_').replace('-', '_')
    return "".join(c for c in name if c.isalnum() or c == '_')

worker_tile_cache = {}

def init_worker():
    global worker_tile_cache
    for side in range(2):
        for piece in range(9):
            try:
                path = os.path.join(TILE_IMAGES_DIR, f"{side}_{piece}.png")
                img = Image.open(path).convert("RGBA")
                worker_tile_cache[(piece, side)] = img
            except Exception:
                worker_tile_cache[(piece, side)] = None

def load_and_rotate_tile_image(piece, side, orientation, tile_size):
    base_img = worker_tile_cache.get((piece, side))
    if base_img is None:
        return Image.new('RGBA', (tile_size, tile_size), (0, 0, 0, 255))
    resized_img = base_img.resize((tile_size, tile_size), resample=Image.LANCZOS)
    rotated_img = resized_img.rotate(orientation * -90, expand=False, resample=Image.BICUBIC)
    return rotated_img

def generate_tiling_image(solution_row, output_path):
    TILE_SIZE = 128
    BOARD_SIZE = TILE_SIZE * 3
    board_image = Image.new('RGBA', (BOARD_SIZE, BOARD_SIZE), (0, 0, 0, 0))
    for r in range(3):
        for c in range(3):
            pos_str = f"{r}{c}"
            piece = solution_row[f"piece_{pos_str}"]
            side = solution_row[f"side_{pos_str}"]
            orientation = solution_row[f"orient_{pos_str}"]
            tile_img = load_and_rotate_tile_image(piece, side, orientation, TILE_SIZE)
            board_image.paste(tile_img, (c * TILE_SIZE, r * TILE_SIZE), tile_img)
    board_image.save(output_path, optimize=True, quality=85)
    
def generate_image_task_wrapper(args):
    """Wrapper para desempacotar os argumentos para a função de geração de imagem."""
    generate_tiling_image(args[0], args[1])

# =============================================================================
# FUNÇÕES DE PROCESSAMENTO DE DADOS (CORRIGIDAS)
# =============================================================================

def create_db_from_parquet(parquet_file_path):
    """
    Creates a VIEW pointing to the Parquet file instead of importing the data.
    This is much faster and saves over 5GB of disk space and memory.
    """
    os.makedirs(DATABASES_OUTPUT_DIR, exist_ok=True)
    con = duckdb.connect(MAIN_DB_PATH)
    con.execute("PRAGMA memory_limit='20GB';")
    con.execute(f"PRAGMA threads={os.cpu_count()};")
    con.execute("PRAGMA enable_progress_bar=true;")

    try:
        views_df = con.execute("SELECT view_name FROM duckdb_views();").fetchdf()
        view_exists = 'solutions' in views_df['view_name'].values
    except duckdb.Error:
        # This can fail if no views exist yet, which is fine.
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
        
        # 🔥 CORREÇÃO: A linha 'ANALYZE' foi removida.
        # Não é possível analisar uma VIEW, e não é necessário para Parquet.

    except Exception as e:
        print(f"❌ ERRO durante a criação da view: {e}")
        con.close()
        sys.exit(1)
        
    con.close()

def calculate_percentiles():
    """
    Calculates percentiles using a single UNPIVOT query.
    This is much more efficient than the previous UNION ALL approach.
    """
    print("\n🚀 Calculando frequências e percentis para todas as estatísticas (Modo UNPIVOT)...")
    main_con = duckdb.connect(MAIN_DB_PATH, read_only=True)
    main_con.execute("PRAGMA memory_limit='20GB';")
    main_con.execute(f"PRAGMA threads={os.cpu_count()};")
    main_con.execute("PRAGMA enable_progress_bar=true;")
    
    percentiles_con = duckdb.connect(PERCENTILES_DB_PATH)
    percentiles_con.execute("CREATE OR REPLACE TABLE stat_percentiles (stat_name VARCHAR, stat_value UTINYINT, frequency UBIGINT, percentile REAL);")

    # Build a single, efficient UNPIVOT query
    stat_columns_list = ", ".join([f'"{col}"' for col in STAT_COLUMNS])

    unpivot_query = f"""
        WITH ValueCounts AS (
            -- First, get the frequency of each value for each statistic in a single pass.
            SELECT
                stat_name,
                stat_value,
                COUNT(*) as frequency
            FROM (
                UNPIVOT solutions
                ON {stat_columns_list}
                INTO NAME stat_name VALUE stat_value
            ) AS unpivoted_data
            GROUP BY stat_name, stat_value
        )
        -- Now, calculate the percentile rank based on the aggregated counts.
        SELECT
            stat_name,
            CAST(stat_value AS UTINYINT),
            CAST(frequency AS UBIGINT),
            CAST((PERCENT_RANK() OVER (PARTITION BY stat_name ORDER BY stat_value)) * 100 AS REAL) as percentile
        FROM ValueCounts;
    """
    
    df = main_con.execute(unpivot_query).fetchdf()
    percentiles_con.execute("INSERT INTO stat_percentiles SELECT * FROM df")
    
    percentiles_con.close()
    main_con.close()
    print(f"✅ Frequências e percentis salvos em '{PERCENTILES_DB_PATH}'.")

def precompute_all_scores(game_cards):
    print("\n🚀 Pre-calculating scores (using Geometric Mean for super_score)...")
    con = duckdb.connect(MAIN_DB_PATH, read_only=False)
    con.execute("PRAGMA memory_limit='20GB';")
    con.execute(f"PRAGMA threads={os.cpu_count()};")
    con.execute("PRAGMA enable_progress_bar=true;")
    
    os.makedirs(TEMP_DIR, exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{TEMP_DIR}';")
    
    try:
        con.execute(f"ATTACH '{PERCENTILES_DB_PATH}' AS percentiles_db (READ_ONLY);")
    except Exception as e:
        print(f"❌ Falha ao anexar o banco de dados de percentis: {e}")
        con.close()
        return
        
    con.execute("DROP TABLE IF EXISTS solution_scores;")
    scorable_cards = [card for card in game_cards if card.get('key')]
    
    select_clauses = ["s.solution_id"]
    join_clauses = []
    for i, card in enumerate(scorable_cards):
        key = card['key']
        card_num = card['number']
        card_type = card['type']
        alias = f"p{i}"
        score_logic = f"{alias}.percentile" if card_type == 'max' else f"100.0 - {alias}.percentile"
        select_clauses.append(f"CAST({score_logic} AS REAL) AS card_{card_num}_score")
        join_clauses.append(f"""LEFT JOIN percentiles_db.stat_percentiles AS {alias} ON s."{key}" = {alias}.stat_value AND {alias}.stat_name = '{key}'""")
        
    select_sql = ",\n       ".join(select_clauses)
    join_sql = "\n".join(join_clauses)
    score_columns = [f"card_{card['number']}_score" for card in scorable_cards]
    num_scorable_cards = len(score_columns)
    
    clean_scores = [f"GREATEST(0, COALESCE({col}, 0))" for col in score_columns]
    zero_check_sql = " OR ".join([f"{cs} = 0" for cs in clean_scores])
    log_sum_sql = " + ".join([f"LN({cs})" for cs in clean_scores])
    stable_geometric_mean_sql = f"EXP(({log_sum_sql}) / {float(num_scorable_cards)})"
    
    super_score_sql = f"""
        CASE
            WHEN {zero_check_sql} THEN 0.0
            ELSE {stable_geometric_mean_sql}
        END
    """

    full_query = f"""
        CREATE TABLE solution_scores AS
        WITH ScoredSolutions AS (SELECT {select_sql} FROM solutions s {join_sql})
        SELECT *, CAST({super_score_sql} AS REAL) AS super_score FROM ScoredSolutions;
    """
    
    print("  -> Executing query with multiple JOINs and stable geometric mean. This may take a while...")
    con.execute(full_query)
    con.close()
    print("✅ Table 'solution_scores' created successfully.")
    
def _add_image_task(row, combo_type, card_lookup, image_tasks, base_dirs):
    score = row.get('best_score', row.get('super_score', 0))
    score_str = f"score_{int(score)}" if pd.notna(score) else "score_NA"
    
    path_parts = []
    filename_parts = []

    if combo_type == 'single':
        card_id = int(row['card_id'])
        card_name = sanitize_folder_name(card_lookup.get(card_id, f'card_{card_id}'))
        folder_name = f"{card_id:02d}_{card_name}"
        path_parts = [base_dirs['single'], folder_name]
        filename_parts = [f"{card_id:02d}", score_str]
    
    elif combo_type == 'pair':
        c1, c2 = int(row['c1']), int(row['c2'])
        c1_name = sanitize_folder_name(card_lookup.get(c1, ''))
        c2_name = sanitize_folder_name(card_lookup.get(c2, ''))
        path_parts = [base_dirs['pair'], f"{c1:02d}_{c1_name}", f"{c2:02d}_{c2_name}"]
        filename_parts = [f"{c1:02d}", f"{c2:02d}", score_str]

    elif combo_type == 'trio':
        c1, c2, c3 = int(row['c1']), int(row['c2']), int(row['c3'])
        names = [sanitize_folder_name(card_lookup.get(c, '')) for c in [c1, c2, c3]]
        path_parts = [base_dirs['trio'], f"{c1:02d}_{names[0]}", f"{c2:02d}_{names[1]}", f"{c3:02d}_{names[2]}"]
        filename_parts = [f"{c1:02d}", f"{c2:02d}", f"{c3:02d}", score_str]

    elif combo_type == 'overall':
        num_cards = len([c for c in card_lookup if card_lookup.get(c)])
        path_parts = [base_dirs['overall']]
        filename_parts = [f"all_{num_cards}_cards", score_str]

    if not path_parts:
        return

    output_dir = os.path.join(*path_parts)
    os.makedirs(output_dir, exist_ok=True)
    
    filename = "_".join(filename_parts) + ".png"
    image_path = os.path.join(output_dir, filename)
    image_tasks.append((row, image_path))

def find_best_solutions_and_generate_maps(game_cards):
    """
    Finds the best solutions using the Geometric Mean to reward balanced combinations,
    followed by a single batch-fetch query and robust DataFrame inserts.
    """
    print("\n🚀 Finding best solutions (using Geometric Mean)...")

    # --- Setup (no changes) ---
    best_con = duckdb.connect(BEST_SOLUTIONS_DB_PATH)
    best_con.execute("PRAGMA memory_limit='20GB';")
    best_con.execute(f"PRAGMA threads={os.cpu_count()};")
    best_con.execute("PRAGMA enable_progress_bar=true;")
    
    main_con_read_path = MAIN_DB_PATH.replace('\\', '/')
    best_con.execute(f"ATTACH '{main_con_read_path}' AS main_db (READ_ONLY);")
    best_con.execute(f"PRAGMA temp_directory='{TEMP_DIR}';")

    layout_columns_list = [f's."piece_{r}{c}"' for r in range(3) for c in range(3)] + \
                          [f's."side_{r}{c}"' for r in range(3) for c in range(3)] + \
                          [f's."orient_{r}{c}"' for r in range(3) for c in range(3)]
    layout_definitions = [f'"{col.split(".")[1][1:-1]}" UTINYINT' for col in layout_columns_list]
    layout_columns_str = ", ".join(layout_columns_list)
    layout_definitions_str = ", ".join(layout_definitions)
    
    clean_layout_columns = [col.split(".")[1][1:-1] for col in layout_columns_list]

    card_lookup = {card['number']: card['name'] for card in game_cards}
    scorable_card_ids = sorted([card['number'] for card in game_cards if card.get('key')])
    
    maps_1_card_dir = os.path.join(MAPS_OUTPUT_DIR, '1_card')
    maps_2_cards_dir = os.path.join(MAPS_OUTPUT_DIR, '2_cards')
    maps_3_cards_dir = os.path.join(MAPS_OUTPUT_DIR, '3_cards')
    maps_all_cards_dir = os.path.join(MAPS_OUTPUT_DIR, 'all_cards')
    base_dirs = {'single': maps_1_card_dir, 'pair': maps_2_cards_dir, 'trio': maps_3_cards_dir, 'overall': maps_all_cards_dir}
    for d in base_dirs.values(): os.makedirs(d, exist_ok=True)

    image_tasks = []

    # --- 1. Melhores soluções para SINGLE cards ---
    print("  -> Step 1/4: Finding best solutions for single cards...")
    best_con.execute(f"CREATE OR REPLACE TABLE best_single_card (card_id UTINYINT, solution_id UBIGINT, best_score REAL, {layout_definitions_str});")
    
    max_by_clauses_single = [f"max_by(solution_id, card_{cid}_score) AS id_{cid}" for cid in scorable_card_ids]
    single_scan_query = f"SELECT {', '.join(max_by_clauses_single)} FROM main_db.solution_scores;"
    best_ids_df_single = best_con.execute(single_scan_query).fetchdf()
    
    unique_best_ids = set(best_ids_df_single.iloc[0].values)
    ids_str = ','.join(map(str, unique_best_ids))
    details_query = f"SELECT s.solution_id, {layout_columns_str}, ss.* FROM main_db.solutions s JOIN main_db.solution_scores ss ON s.solution_id = ss.solution_id WHERE s.solution_id IN ({ids_str});"
    all_details_df = best_con.execute(details_query).fetchdf()

    rows_to_insert = []
    for card_id in scorable_card_ids:
        best_solution_id = best_ids_df_single[f'id_{card_id}'][0]
        details_row = all_details_df[all_details_df['solution_id'] == best_solution_id].iloc[0]
        
        best_score = details_row[f'card_{card_id}_score']
        new_row_data = {'card_id': card_id, 'solution_id': best_solution_id, 'best_score': best_score}
        new_row_data.update(details_row[clean_layout_columns])
        rows_to_insert.append(new_row_data)

        task_row = details_row.copy(); task_row['card_id'] = card_id; task_row['best_score'] = best_score 
        _add_image_task(task_row, 'single', card_lookup, image_tasks, base_dirs)
    
    if rows_to_insert:
        df_to_insert = pd.DataFrame(rows_to_insert)[['card_id', 'solution_id', 'best_score'] + clean_layout_columns]
        best_con.execute("INSERT INTO best_single_card SELECT * FROM df_to_insert")

    # --- 2. Melhores soluções para PARES de cartões ---
    print("  -> Step 2/4: Finding best solutions for card pairs...")
    best_con.execute(f"CREATE OR REPLACE TABLE best_card_pairs (card_id_1 UTINYINT, card_id_2 UTINYINT, solution_id UBIGINT, best_score REAL, {layout_definitions_str});")
    card_pairs = list(itertools.combinations(scorable_card_ids, 2))
    
    if card_pairs:
        # 🔥 CHANGE: Using Geometric Mean for scoring
        max_by_clauses_pairs = [f"max_by(solution_id, pow(card_{c1}_score * card_{c2}_score, 1/2.0)) AS id_{c1}_{c2}" for c1, c2 in card_pairs]
        pair_scan_query = f"SELECT {', '.join(max_by_clauses_pairs)} FROM main_db.solution_scores;"
        best_ids_df_pairs = best_con.execute(pair_scan_query).fetchdf()

        unique_best_ids_pairs = set(best_ids_df_pairs.iloc[0].values)
        ids_str_pairs = ','.join(map(str, unique_best_ids_pairs))
        details_query_pairs = f"SELECT s.solution_id, {layout_columns_str}, ss.* FROM main_db.solutions s JOIN main_db.solution_scores ss ON s.solution_id = ss.solution_id WHERE s.solution_id IN ({ids_str_pairs});"
        all_details_df_pairs = best_con.execute(details_query_pairs).fetchdf()

        rows_to_insert_pairs = []
        for c1, c2 in card_pairs:
            best_solution_id = best_ids_df_pairs[f'id_{c1}_{c2}'][0]
            details_row = all_details_df_pairs[all_details_df_pairs['solution_id'] == best_solution_id].iloc[0]
            # 🔥 CHANGE: Using Geometric Mean for scoring
            score = (details_row[f'card_{c1}_score'] * details_row[f'card_{c2}_score']) ** (1/2.0)
            
            new_row_data = {'card_id_1': c1, 'card_id_2': c2, 'solution_id': best_solution_id, 'best_score': score}
            new_row_data.update(details_row[clean_layout_columns])
            rows_to_insert_pairs.append(new_row_data)
            
            task_row = details_row.copy(); task_row['c1'], task_row['c2'], task_row['best_score'] = c1, c2, score
            _add_image_task(task_row, 'pair', card_lookup, image_tasks, base_dirs)

        if rows_to_insert_pairs:
            df_to_insert = pd.DataFrame(rows_to_insert_pairs)[['card_id_1', 'card_id_2', 'solution_id', 'best_score'] + clean_layout_columns]
            best_con.execute("INSERT INTO best_card_pairs SELECT * FROM df_to_insert")

    # --- 3. Melhores soluções para TRIOS de cartões ---
    print("  -> Step 3/4: Finding best solutions for card trios...")
    best_con.execute(f"CREATE OR REPLACE TABLE best_card_trios (card_id_1 UTINYINT, card_id_2 UTINYINT, card_id_3 UTINYINT, solution_id UBIGINT, best_score REAL, {layout_definitions_str});")
    card_trios = list(itertools.combinations(scorable_card_ids, 3))

    if card_trios:
        # 🔥 CHANGE: Using Geometric Mean for scoring
        max_by_clauses_trios = [f"max_by(solution_id, pow(card_{c1}_score * card_{c2}_score * card_{c3}_score, 1/3.0)) AS id_{c1}_{c2}_{c3}" for c1, c2, c3 in card_trios]
        trio_scan_query = f"SELECT {', '.join(max_by_clauses_trios)} FROM main_db.solution_scores;"
        best_ids_df_trios = best_con.execute(trio_scan_query).fetchdf()

        unique_best_ids_trios = set(best_ids_df_trios.iloc[0].values)
        ids_str_trios = ','.join(map(str, unique_best_ids_trios))
        details_query_trios = f"SELECT s.solution_id, {layout_columns_str}, ss.* FROM main_db.solutions s JOIN main_db.solution_scores ss ON s.solution_id = ss.solution_id WHERE s.solution_id IN ({ids_str_trios});"
        all_details_df_trios = best_con.execute(details_query_trios).fetchdf()

        rows_to_insert_trios = []
        for c1, c2, c3 in card_trios:
            best_solution_id = best_ids_df_trios[f'id_{c1}_{c2}_{c3}'][0]
            details_row = all_details_df_trios[all_details_df_trios['solution_id'] == best_solution_id].iloc[0]
            # 🔥 CHANGE: Using Geometric Mean for scoring
            score = (details_row[f'card_{c1}_score'] * details_row[f'card_{c2}_score'] * details_row[f'card_{c3}_score']) ** (1/3.0)
            
            new_row_data = {'card_id_1': c1, 'card_id_2': c2, 'card_id_3': c3, 'solution_id': best_solution_id, 'best_score': score}
            new_row_data.update(details_row[clean_layout_columns])
            rows_to_insert_trios.append(new_row_data)
            
            task_row = details_row.copy(); task_row['c1'], task_row['c2'], task_row['c3'], task_row['best_score'] = c1, c2, c3, score
            _add_image_task(task_row, 'trio', card_lookup, image_tasks, base_dirs)

        if rows_to_insert_trios:
            df_to_insert = pd.DataFrame(rows_to_insert_trios)[['card_id_1', 'card_id_2', 'card_id_3', 'solution_id', 'best_score'] + clean_layout_columns]
            best_con.execute("INSERT INTO best_card_trios SELECT * FROM df_to_insert")

    # --- 4. Melhor solução geral ---
    print("  -> Step 4/4: Finding the best overall solution...")
    best_con.execute(f"CREATE OR REPLACE TABLE best_overall_solution (solution_id UBIGINT, best_score REAL, {layout_definitions_str});")
    
    overall_query = "SELECT max_by(solution_id, super_score) as best_id FROM main_db.solution_scores"
    best_overall_id = best_con.execute(overall_query).fetchdf()['best_id'][0]

    details_query_overall = f"SELECT s.solution_id, {layout_columns_str}, ss.* FROM main_db.solutions s JOIN main_db.solution_scores ss ON s.solution_id = ss.solution_id WHERE s.solution_id = {best_overall_id};"
    details_df_overall = best_con.execute(details_query_overall).fetchdf()

    if not details_df_overall.empty:
        details_row = details_df_overall.iloc[0]
        score = details_row['super_score']
        
        new_row_data = {'solution_id': best_overall_id, 'best_score': score}
        new_row_data.update(details_row[clean_layout_columns])
        
        df_to_insert = pd.DataFrame([new_row_data])[['solution_id', 'best_score'] + clean_layout_columns]
        best_con.execute("INSERT INTO best_overall_solution SELECT * FROM df_to_insert")
        
        task_row = details_row.copy(); task_row['best_score'] = score
        _add_image_task(task_row, 'overall', card_lookup, image_tasks, base_dirs)

    # --- Cleanup and Image Generation ---
    best_con.close()
    print(f"✅ Best solutions saved to '{BEST_SOLUTIONS_DB_PATH}'.")

    if not image_tasks:
        print("\nNo images to generate.")
        return

    if __name__ == '__main__':
        cpu_cores = os.cpu_count() or 1
        print(f"\n🚀 Generating {len(image_tasks)} map images in parallel using {cpu_cores} cores...")
        with multiprocessing.Pool(cpu_cores, initializer=init_worker) as pool:
            try:
                from tqdm import tqdm
                for _ in tqdm(pool.imap_unordered(generate_image_task_wrapper, image_tasks), total=len(image_tasks)):
                    pass
            except ImportError:
                pool.map(generate_image_task_wrapper, image_tasks)
        print("✅ All images were generated successfully.")

# =============================================================================
# EXECUTOR PRINCIPAL
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
    find_best_solutions_and_generate_maps(game_cards)

    end_time = time.time()
    print("\n" + "=" * 50)
    print("✅ Processo concluído com sucesso!")
    print(f"  -> Duração total: {(end_time - start_time) / 3600:.2f} horas.")
    print(f"  -> Bancos de dados de análise salvos em: '{DATABASES_OUTPUT_DIR}/'")
    print(f"  -> Mapas das melhores soluções salvos em: '{SOLUTIONS_OUTPUT_DIR}/'")
    print("=" * 50)

if __name__ == "__main__":
    main()