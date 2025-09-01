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
# FUNÇÕES DE PROCESSAMENTO DE DADOS (sem alterações)
# =============================================================================

def create_db_from_parquet(parquet_file_path):
    os.makedirs(DATABASES_OUTPUT_DIR, exist_ok=True)
    con = duckdb.connect(MAIN_DB_PATH)
    con.execute("PRAGMA memory_limit='24GB';")
    tables = con.execute("SHOW TABLES;").fetchdf()
    if 'solutions' in tables['name'].values:
        print(f"✅ Tabela 'solutions' já existe em '{MAIN_DB_PATH}'. Pulando a ingestão.")
        con.close()
        return
    print(f"🚀 Iniciando a ingestão otimizada de '{parquet_file_path}' para '{MAIN_DB_PATH}'...")
    all_parquet_columns = STAT_COLUMNS[:]
    for r in range(3):
        for c in range(3):
            pos = f"{r}{c}"
            all_parquet_columns.extend([f"piece_{pos}", f"side_{pos}", f"orient_{pos}"])
    definitions = ["solution_id UBIGINT"]
    select_clauses = ["ROW_NUMBER() OVER () AS solution_id"]
    for col in all_parquet_columns:
        col_type = 'UTINYINT'
        definitions.append(f'"{col}" {col_type}')
        select_clauses.append(f'CAST("{col}" AS {col_type}) AS "{col}"')
    definitions_sql = ", ".join(definitions)
    select_clauses_sql = ", ".join(select_clauses)
    create_table_query = f"CREATE TABLE solutions ({definitions_sql});"
    insert_data_query = f"INSERT INTO solutions SELECT {select_clauses_sql} FROM read_parquet('{parquet_file_path}');"
    try:
        con.execute(create_table_query)
        con.execute(insert_data_query)
        print("✅ Banco de dados principal e tabela 'solutions' criados com sucesso.")
    except Exception as e:
        print(f"❌ ERRO durante a ingestão: {e}")
        con.close()
        sys.exit(1)
    con.close()

def calculate_percentiles():
    print("\n🚀 Calculando frequências e percentis para todas as estatísticas...")
    main_con = duckdb.connect(MAIN_DB_PATH, read_only=True)
    main_con.execute("PRAGMA memory_limit='24GB';")
    percentiles_con = duckdb.connect(PERCENTILES_DB_PATH)
    percentiles_con.execute("CREATE OR REPLACE TABLE stat_percentiles (stat_name VARCHAR, stat_value UTINYINT, frequency UBIGINT, percentile REAL);")
    union_queries = [f"SELECT '{stat}' AS stat_name, \"{stat}\" AS stat_value, COUNT(*) AS frequency FROM solutions GROUP BY \"{stat}\"" for stat in STAT_COLUMNS]
    full_query = "\nUNION ALL\n".join(union_queries)
    final_query = f"WITH AllValueCounts AS ({full_query}) SELECT stat_name, CAST(stat_value AS UTINYINT), CAST(frequency AS UBIGINT), CAST((PERCENT_RANK() OVER (PARTITION BY stat_name ORDER BY stat_value)) * 100 AS REAL) FROM AllValueCounts;"
    df = main_con.execute(final_query).fetchdf()
    percentiles_con.execute("INSERT INTO stat_percentiles SELECT * FROM df")
    percentiles_con.close()
    main_con.close()
    print(f"✅ Frequências e percentis salvos em '{PERCENTILES_DB_PATH}'.")

def precompute_all_scores(game_cards):
    print("\n🚀 Pré-calculando scores para todas as soluções...")
    con = duckdb.connect(MAIN_DB_PATH, read_only=False)
    con.execute("PRAGMA memory_limit='24GB';")
    os.makedirs(TEMP_DIR, exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{TEMP_DIR}';")
    try:
        con.execute(f"ATTACH '{PERCENTILES_DB_PATH}' AS percentiles_db (READ_ONLY);")
    except Exception as e:
        print(f"❌ Falha ao anexar o banco de dados de percentis: {e}")
        con.close()
        return
    
    con.execute("DROP TABLE IF EXISTS solution_scores;")
    
    pivot_aggregates = []
    scorable_cards = [card for card in game_cards if card.get('key')]
    
    for card in scorable_cards:
        score_logic = "u.percentile" if card.get('type') == 'max' else "100.0 - u.percentile"
        
        # <<< CORREÇÃO DEFINITIVA: Trocando FIRST() por MAX() >>>
        # MAX() é a função de agregação correta e mais robusta para este tipo de pivoteamento manual.
        aggregation = f"MAX(CASE WHEN u.stat_name = '{card['key']}' THEN {score_logic} ELSE NULL END)"
        
        pivot_aggregates.append(f"CAST({aggregation} AS REAL) AS card_{card['number']}_score")

    pivot_sql = ",\n            ".join(pivot_aggregates)
    super_score_sql = " + ".join([f"card_{card['number']}_score" for card in scorable_cards])
    unpivot_cols_sql = ", ".join(f'"{col}"' for col in STAT_COLUMNS)
    
    full_query = f"""
        CREATE TABLE solution_scores AS
        WITH unpivoted_solutions AS (
            UNPIVOT solutions ON {unpivot_cols_sql} INTO NAME stat_name VALUE stat_value
        ),
        scored_stats AS (
            SELECT u.solution_id, u.stat_name, p.percentile
            FROM unpivoted_solutions u
            JOIN percentiles_db.stat_percentiles p ON u.stat_name = p.stat_name AND u.stat_value = p.stat_value
        ),
        pivoted_scores AS (
            SELECT solution_id, {pivot_sql} 
            FROM scored_stats u 
            GROUP BY solution_id
        )
        SELECT *, CAST({super_score_sql} AS REAL) AS super_score
        FROM pivoted_scores;
    """
    
    print("  -> Executando a consulta de pivoteamento para criar scores...")
    con.execute(full_query)
    
    print("  -> Criando índice em super_score para desempate rápido...")
    con.execute("CREATE INDEX IF NOT EXISTS idx_super_score ON solution_scores (super_score DESC);")
    
    con.close()
    print("✅ Tabela 'solution_scores' criada e indexada com sucesso.")
    
# <<< MELHORIA 4 >>>
# Função centralizada para adicionar tarefas de geração de imagem.
# Isso evita a repetição de código e facilita a manutenção.
def _add_image_task(row, combo_type, card_lookup, image_tasks, base_dirs):
    """
    Função auxiliar para criar nomes de arquivo/diretório e adicionar
    uma tarefa de geração de imagem à lista de tarefas.
    """
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

# =============================================================================
# FUNÇÃO PRINCIPAL DE BUSCA (VERSÃO CORRIGIDA E ROBUSTA)
# =============================================================================
def find_best_solutions_and_generate_maps(game_cards):
    """
    Encontra as melhores soluções processando uma combinação de cada vez para
    garantir a estabilidade e evitar o esgotamento do disco em hardware local.
    """
    print("\n🚀 Encontrando melhores soluções (Modo Robusto de Memória de Disco)...")

    best_con = duckdb.connect(BEST_SOLUTIONS_DB_PATH)
    best_con.execute("PRAGMA memory_limit='24GB';")
    
    main_con_read = duckdb.connect(MAIN_DB_PATH, read_only=True)
    main_con_read.execute("PRAGMA memory_limit='24GB';")
    main_con_read.execute(f"PRAGMA temp_directory='{TEMP_DIR}';")

    # --- Setup ---
    # <<< CORREÇÃO: A linha problemática foi removida e a definição de 'layout_columns' está no lugar certo. >>>
    layout_columns = [f's."piece_{r}{c}"' for r in range(3) for c in range(3)] + \
                     [f's."side_{r}{c}"' for r in range(3) for c in range(3)] + \
                     [f's."orient_{r}{c}"' for r in range(3) for c in range(3)]
    layout_definitions = [f'"{col.split(".")[1][1:-1]}" UTINYINT' for col in layout_columns]
    layout_columns_str = ", ".join(layout_columns)
    layout_definitions_str = ", ".join(layout_definitions)
    
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
    print("  -> Processando cartões individuais...")
    best_con.execute(f"CREATE OR REPLACE TABLE best_single_card (card_id UTINYINT, solution_id UBIGINT, best_score REAL, {layout_definitions_str});")
    for card_id in scorable_card_ids:
        if best_con.execute(f"SELECT count(*) FROM best_single_card WHERE card_id={card_id}").fetchone()[0] > 0:
            print(f"    -> Pulando cartão {card_id}, já processado.")
            continue
        print(f"    - Processando cartão individual: {card_id}")
        query = f"""
            WITH BestSolutionID AS (
                SELECT solution_id, card_{card_id}_score as score
                FROM solution_scores WHERE card_{card_id}_score IS NOT NULL
                ORDER BY score DESC, super_score DESC LIMIT 1
            )
            SELECT {card_id} as card_id, bsi.solution_id, bsi.score as best_score, {layout_columns_str}
            FROM BestSolutionID bsi JOIN solutions s ON bsi.solution_id = s.solution_id;
        """
        result_df = main_con_read.execute(query).fetchdf()
        if not result_df.empty:
            best_con.execute("INSERT INTO best_single_card SELECT * FROM result_df")
            for _, row in result_df.iterrows():
                _add_image_task(row, 'single', card_lookup, image_tasks, base_dirs)


    # --- 2. Melhores soluções para PARES de cartões ---
    print("  -> Processando pares de cartões...")
    best_con.execute(f"CREATE OR REPLACE TABLE best_card_pairs (card_id_1 UTINYINT, card_id_2 UTINYINT, solution_id UBIGINT, best_score REAL, {layout_definitions_str});")
    card_pairs = list(itertools.combinations(scorable_card_ids, 2))

    for c1, c2 in card_pairs:
        if best_con.execute(f"SELECT count(*) FROM best_card_pairs WHERE card_id_1={c1} AND card_id_2={c2}").fetchone()[0] > 0:
            print(f"    -> Pulando par {c1}-{c2}, já processado.")
            continue
        print(f"    - Processando par: {c1}-{c2}")
        
        score_col1, score_col2 = f"card_{c1}_score", f"card_{c2}_score"
        query = f"""
            WITH BestSolutionID AS (
                SELECT solution_id, ({score_col1} + {score_col2}) / 2.0 AS score
                FROM solution_scores
                WHERE {score_col1} IS NOT NULL AND {score_col2} IS NOT NULL
                ORDER BY score DESC, super_score DESC
                LIMIT 1
            )
            SELECT {c1} as c1, {c2} as c2, bsi.solution_id, bsi.score as best_score, {layout_columns_str}
            FROM BestSolutionID bsi JOIN solutions s ON bsi.solution_id = s.solution_id;
        """
        result_df = main_con_read.execute(query).fetchdf()
        if not result_df.empty:
            result_df_insert = result_df.rename(columns={'c1': 'card_id_1', 'c2': 'card_id_2'})
            best_con.execute("INSERT INTO best_card_pairs SELECT * FROM result_df_insert")
            for _, row in result_df.iterrows():
                row_copy = row.copy()
                row_copy['c1'], row_copy['c2'] = c1, c2
                _add_image_task(row_copy, 'pair', card_lookup, image_tasks, base_dirs)

    # --- 3. Melhores soluções para TRIOS de cartões ---
    print("  -> Processando trios de cartões...")
    best_con.execute(f"CREATE OR REPLACE TABLE best_card_trios (card_id_1 UTINYINT, card_id_2 UTINYINT, card_id_3 UTINYINT, solution_id UBIGINT, best_score REAL, {layout_definitions_str});")
    card_trios = list(itertools.combinations(scorable_card_ids, 3))

    for c1, c2, c3 in card_trios:
        if best_con.execute(f"SELECT count(*) FROM best_card_trios WHERE card_id_1={c1} AND card_id_2={c2} AND card_id_3={c3}").fetchone()[0] > 0:
            print(f"    -> Pulando trio {c1}-{c2}-{c3}, já processado.")
            continue
        print(f"    - Processando trio: {c1}-{c2}-{c3}")
        
        score_col1, score_col2, score_col3 = f"card_{c1}_score", f"card_{c2}_score", f"card_{c3}_score"
        query = f"""
            WITH BestSolutionID AS (
                SELECT solution_id, ({score_col1} + {score_col2} + {score_col3}) / 3.0 AS score
                FROM solution_scores
                WHERE {score_col1} IS NOT NULL AND {score_col2} IS NOT NULL AND {score_col3} IS NOT NULL
                ORDER BY score DESC, super_score DESC
                LIMIT 1
            )
            SELECT {c1} as c1, {c2} as c2, {c3} as c3, bsi.solution_id, bsi.score as best_score, {layout_columns_str}
            FROM BestSolutionID bsi JOIN solutions s ON bsi.solution_id = s.solution_id;
        """
        result_df = main_con_read.execute(query).fetchdf()
        if not result_df.empty:
            result_df_insert = result_df.rename(columns={'c1': 'card_id_1', 'c2': 'card_id_2', 'c3': 'card_id_3'})
            best_con.execute("INSERT INTO best_card_trios SELECT * FROM result_df_insert")
            for _, row in result_df.iterrows():
                row_copy = row.copy()
                row_copy['c1'], row_copy['c2'], row_copy['c3'] = c1, c2, c3
                _add_image_task(row_copy, 'trio', card_lookup, image_tasks, base_dirs)
    
    # --- 4. Melhor solução geral ---
    # (A lógica aqui não foi alterada)
    print(f"  -> Processando a melhor solução geral...")
    best_con.execute(f"CREATE OR REPLACE TABLE best_overall_solution (solution_id UBIGINT, best_score REAL, {layout_definitions_str});")
    if best_con.execute("SELECT count(*) FROM best_overall_solution").fetchone()[0] == 0:
        least_columns = ", ".join([f"card_{cid}_score" for cid in scorable_card_ids])
        query = f"""
            WITH bsi AS (
                SELECT solution_id, super_score / {float(len(scorable_card_ids))} AS best_score
                FROM solution_scores ORDER BY best_score DESC, LEAST({least_columns}) DESC LIMIT 1
            )
            SELECT s.solution_id, bsi.best_score, {layout_columns_str}
            FROM solutions s JOIN bsi ON s.solution_id = bsi.solution_id;
        """
        result_df = main_con_read.execute(query).fetchdf()

        if not result_df.empty:
            best_con.execute("INSERT INTO best_overall_solution SELECT * FROM result_df")
            _add_image_task(result_df.iloc[0], 'overall', card_lookup, image_tasks, base_dirs)
    else:
        print("    -> Pulando, solução geral já calculada.")

    main_con_read.close()
    best_con.close()
    print(f"✅ Melhores soluções salvas em '{BEST_SOLUTIONS_DB_PATH}'.")

    # --- Geração de imagens em paralelo ---
    if not image_tasks:
        print("\nNenhuma imagem para gerar.")
        return

    if __name__ == '__main__':
        cpu_cores = os.cpu_count() or 1
        print(f"\n🚀 Gerando {len(image_tasks)} imagens de mapa em paralelo usando {cpu_cores} núcleos...")
        with multiprocessing.Pool(cpu_cores, initializer=init_worker) as pool:
            try:
                from tqdm import tqdm
                for _ in tqdm(pool.imap_unordered(generate_image_task_wrapper, image_tasks), total=len(image_tasks)):
                    pass
            except ImportError:
                pool.map(generate_image_task_wrapper, image_tasks)
        print("✅ Todas as imagens foram geradas com sucesso.")

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