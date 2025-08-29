import multiprocessing
import duckdb
import itertools
import os, re
import json
from PIL import Image

# =============================================================================
# CONFIGURAÇÃO (SEÇÃO MODIFICADA)
# =============================================================================
# A variável SOLUTIONS_PARQUET_PATH foi removida. O caminho será encontrado dinamicamente.
GAME_CARDS_PATH = 'game/cards/cards.json'
TILE_IMAGES_DIR = 'game/tiles/tile_images'
SOURCE_SOLUTIONS_DIR = 'generated_solutions' # Diretório onde o Parquet final está

# --- Novos Diretórios de Saída ---
SOLUTIONS_OUTPUT_DIR = 'solutions'
DATABASES_OUTPUT_DIR = 'databases'
# ------------------------------------

# Caminhos dos arquivos de banco de dados
MAIN_DB_PATH = os.path.join(DATABASES_OUTPUT_DIR, 'solutions.duckdb')
PERCENTILES_DB_PATH = os.path.join(DATABASES_OUTPUT_DIR, 'percentiles.duckdb')
BEST_SOLUTIONS_DB_PATH = os.path.join(DATABASES_OUTPUT_DIR, 'best_solutions.duckdb')

# A pasta raiz para os mapas
MAPS_OUTPUT_DIR = SOLUTIONS_OUTPUT_DIR

# ... (STAT_COLUMNS e caches permanecem inalterados) ...
STAT_COLUMNS = [
    "total_houses", "total_ufos", "total_girls", "total_boys", "total_dogs",
    "total_hamburgers", "total_aliens", "total_agents", "total_captured_aliens",
    "total_curves", "total_tiles_without_roads", "total_roads",
    "max_aliens_running_towards_agent", "max_hamburgers_in_front_of_alien",
    "max_agents_on_one_road", "max_aliens_on_one_road", "max_aliens_between_two_agents",
    "total_food_chain_sets", "longest_road_size", "max_roads_of_same_length",
    "aliens_times_ufos", "aliens_times_hamburgers", "citizen_dog_pairs",
    "largest_dog_group", "largest_house_group", "largest_citizen_group",
    "largest_safe_zone_size"
]

# =============================================================================
# FUNÇÕES AUXILIARES DE IMAGEM (VERSÃO CORRIGIDA)
# =============================================================================

def generate_image_task(task_args):
    """Função auxiliar para ser usada pelo multiprocessing.Pool."""
    solution_row, output_path = task_args
    generate_tiling_image(solution_row, output_path)
    return output_path # Retorna o caminho para podermos rastrear

def find_latest_solution_file(directory, base_name="tiling_solutions", extension="parquet"):
    """
    Encontra o arquivo de solução com o maior índice em um diretório.
    """
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

# ... (sanitize_folder_name, load_and_rotate_tile_image, generate_tiling_image, etc. permanecem inalterados) ...
def sanitize_folder_name(name):
    name = name.replace(' ', '_').replace('-', '_')
    return "".join(c for c in name if c.isalnum() or c == '_')

# Global variable for the cache
worker_tile_cache = {}

def init_worker():
    """Initializer for each worker process in the pool."""
    print(f"Process {os.getpid()} initializing its image cache.")
    global worker_tile_cache
    # Pre-load all 18 base images into a dict for this specific worker
    for side in range(2):
        for piece in range(9):
            try:
                path = os.path.join(TILE_IMAGES_DIR, f"{side}_{piece}.png")
                img = Image.open(path).convert("RGBA")
                worker_tile_cache[(piece, side)] = img
            except Exception:
                worker_tile_cache[(piece, side)] = None

def load_and_rotate_tile_image(piece, side, orientation, tile_size):
    """Modified to use the pre-loaded cache of the worker."""
    # This now uses the global cache populated by init_worker
    base_img = worker_tile_cache.get((piece, side))
    if base_img is None:
        return Image.new('RGBA', (tile_size, tile_size), (0, 0, 0, 255))
    
    # Resizing and rotating is still done on-demand, but file I/O is eliminated
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

# =============================================================================
# FUNÇÃO 1: CRIAR O BANCO DE DADOS PRINCIPAL
# =============================================================================
def create_db_from_parquet(parquet_file_path):
    """Ingere o arquivo Parquet em um banco de dados DuckDB para processamento rápido."""
    os.makedirs(DATABASES_OUTPUT_DIR, exist_ok=True)
    
    con = duckdb.connect(MAIN_DB_PATH)
    
    # --- ADICIONADO: Define um limite de memória para a ingestão ---
    # Ajuste este valor para ~70% da sua RAM total (ex: '12GB', '24GB')
    con.execute("PRAGMA memory_limit='16GB';") 
    # -----------------------------------------------------------

    tables = con.execute("SHOW TABLES;").fetchdf()
    
    if 'solutions' in tables['name'].values:
        print(f"✅ Tabela 'solutions' já existe em '{MAIN_DB_PATH}'. Pulando a ingestão.")
        con.close()
        return

    print(f"🚀 Iniciando a ingestão de '{parquet_file_path}' para '{MAIN_DB_PATH}'...")
    print("Isso pode levar um tempo considerável dependendo do tamanho do arquivo.")
    
    try:
        con.execute(f"""
            CREATE TABLE solutions AS
            SELECT ROW_NUMBER() OVER () AS solution_id, *
            FROM read_parquet('{parquet_file_path}');
        """)
        print(f"✅ Banco de dados principal e tabela 'solutions' criados com sucesso.")
    except Exception as e:
        print(f"❌ ERRO durante a ingestão do arquivo Parquet: {e}")
    
    con.close()

# =============================================================================
# FUNÇÃO 2: CALCULAR PERCENTIS E FREQUÊNCIAS
# =============================================================================
def calculate_percentiles():
    """Calcula frequência e percentil para cada estatística em uma única consulta."""
    print("\n🚀 Calculando frequências e percentis para todas as estatísticas (versão otimizada)...")
    
    main_con = duckdb.connect(MAIN_DB_PATH, read_only=True)
    main_con.execute("PRAGMA memory_limit='16GB';")
    
    percentiles_con = duckdb.connect(PERCENTILES_DB_PATH)
    percentiles_con.execute("""
        CREATE OR REPLACE TABLE stat_percentiles (
            stat_name VARCHAR, stat_value INTEGER, frequency BIGINT, percentile DOUBLE
        );
    """)

    # 1. Monta uma subconsulta para cada estatística
    union_queries = []
    for stat in STAT_COLUMNS:
        query_part = f"""
            SELECT '{stat}' AS stat_name, "{stat}" AS stat_value, COUNT(*) AS frequency
            FROM solutions
            GROUP BY "{stat}"
        """
        union_queries.append(query_part)
    
    # 2. Une todas elas em uma única grande consulta com UNION ALL
    full_query = "\nUNION ALL\n".join(union_queries)

    # 3. Adiciona o cálculo de percentil sobre o resultado consolidado
    final_query = f"""
        WITH AllValueCounts AS (
            {full_query}
        )
        SELECT 
            stat_name, 
            stat_value, 
            frequency,
            (PERCENT_RANK() OVER (PARTITION BY stat_name ORDER BY stat_value)) * 100 AS percentile
        FROM AllValueCounts;
    """
    
    print("  -> Executando a consulta consolidada. Isso pode levar um tempo...")
    df = main_con.execute(final_query).fetchdf()
    
    print("  -> Inserindo resultados e criando o dicionário em memória...")
    percentiles_con.execute("INSERT INTO stat_percentiles SELECT * FROM df")

    percentiles_con.close()
    main_con.close()
    
    print(f"✅ Frequências e percentis salvos em '{PERCENTILES_DB_PATH}'.")

# =============================================================================
# FUNÇÃO 3: PRÉ-CALCULAR E ARMAZENAR TODOS OS SCORES (VERSÃO OTIMIZADA)
# =============================================================================
# REFACTORED FUNCTION 3
def precompute_all_scores(game_cards):
    """
    Generates and stores scores for all solutions using a more efficient,
    SQL-native UNPIVOT -> JOIN -> PIVOT strategy.
    """
    print("\n🚀 Pré-calculando scores para todas as soluções (versão SQL-native)...")
    
    con = duckdb.connect(MAIN_DB_PATH)
    con.execute("PRAGMA memory_limit='24GB';") # Give DuckDB plenty of memory

    try:
        count_check = con.execute("SELECT COUNT(*) FROM solution_scores").fetchone()
        if count_check and count_check[0] > 0:
            print("✅ Tabela 'solution_scores' já calculada. Pulando.")
            con.close()
            return
    except duckdb.CatalogException:
        pass # Table doesn't exist, proceed with calculation.

    # Build the PIVOT aggregation statement dynamically from game_cards
    pivot_aggregates = []
    score_sums = []
    card_type_map = {card['key']: card['type'] for card in game_cards if 'key' in card}

    for i, card in enumerate(game_cards):
        card_num = i + 1
        card_key = card.get('key')
        if not card_key: continue
        
        # The score logic is now inside the SQL aggregation
        score_logic = "p.percentile" if card.get('type') == 'max' else "100.0 - p.percentile"
        
        pivot_aggregates.append(
            f"card_{card_num}_score AS FIRST({score_logic} WHERE u.stat_name = '{card_key}')"
        )
        score_sums.append(f"card_{card_num}_score")

    pivot_sql = ",\n        ".join(pivot_aggregates)
    super_score_sql = " + ".join(score_sums)

    # This single query replaces the entire chunking logic.
    # DuckDB is extremely efficient at executing this pattern.
    full_query = f"""
        CREATE OR REPLACE TABLE solution_scores AS
        WITH unpivoted_solutions AS (
            -- Step 1: Unpivot the solutions table
            UNPIVOT solutions
            ON {STAT_COLUMNS}
            INTO
                NAME stat_name
                VALUE stat_value
        ),
        scored_stats AS (
            -- Step 2: Join with percentiles to get the base score for each stat
            SELECT
                u.solution_id,
                u.stat_name,
                p.percentile
            FROM unpivoted_solutions u
            JOIN stat_percentiles p
              ON u.stat_name = p.stat_name AND u.stat_value = p.stat_value
        )
        -- Step 3: Pivot the data back, calculating the final scores
        SELECT
            solution_id,
            {pivot_sql},
            {super_score_sql} AS super_score
        FROM scored_stats
        GROUP BY solution_id;
    """
    
    print("  -> Executando a consulta UNPIVOT/JOIN/PIVOT. DuckDB vai otimizar isso...")
    con.execute(full_query)

    print("  -> Criando índice em super_score para desempate rápido...")
    con.execute("CREATE INDEX IF NOT EXISTS idx_super_score ON solution_scores (super_score DESC);")
    
    con.close()
    print("✅ Tabela 'solution_scores' criada e indexada com sucesso.")


# =============================================================================
# FUNÇÃO 4: ENCONTRAR AS MELHORES SOLUÇÕES E GERAR MAPAS (VERSÃO COM NOME DE ARQUIVO SIMPLIFICADO)
# =============================================================================
def find_best_solutions_and_generate_maps(game_cards):
    """
    Finds the best balanced solutions using a few, powerful SQL queries instead of thousands of small ones.
    """
    print("\n🚀 Finding best solutions with consolidated SQL queries...")

    main_con = duckdb.connect(MAIN_DB_PATH, read_only=True)
    best_con = duckdb.connect(BEST_SOLUTIONS_DB_PATH)

    # --- Setup ---
    layout_columns, layout_definitions = [], []
    for r in range(3):
        for c in range(3):
            pos = f"{r}{c}"
            layout_columns.extend([f's."piece_{pos}"', f's."side_{pos}"', f's."orient_{pos}"'])
            layout_definitions.extend([f'"piece_{pos}" UTINYINT', f'"side_{pos}" UTINYINT', f'"orient_{pos}" UTINYINT'])
    
    layout_columns_str = ", ".join(layout_columns)
    layout_definitions_str = ", ".join(layout_definitions)
    
    card_lookup = {card['number']: card['name'] for card in game_cards}
    scorable_card_ids = sorted([card['number'] for card in game_cards if card.get('key')]) # Sorted for consistent folder names
    num_scorable_cards = len(scorable_card_ids)
    print(f"  -> Found {num_scorable_cards} scorable cards.")
    
    # --- Create output directories ---
    maps_1_card_dir = os.path.join(MAPS_OUTPUT_DIR, '1_card')
    maps_2_cards_dir = os.path.join(MAPS_OUTPUT_DIR, '2_cards')
    maps_3_cards_dir = os.path.join(MAPS_OUTPUT_DIR, '3_cards')
    maps_all_cards_dir = os.path.join(MAPS_OUTPUT_DIR, 'all_cards')
    os.makedirs(maps_1_card_dir, exist_ok=True)
    os.makedirs(maps_2_cards_dir, exist_ok=True)
    os.makedirs(maps_3_cards_dir, exist_ok=True)
    os.makedirs(maps_all_cards_dir, exist_ok=True)

    image_tasks = []

    # --- 1. Best solutions for SINGLE cards (ONE QUERY) ---
    print("  -> Processing all single cards in one query...")
    best_con.execute(f"CREATE OR REPLACE TABLE best_single_card (card_id UTINYINT, best_solution_id BIGINT, best_score DOUBLE, {layout_definitions_str});")
    
    unioned_singles = " UNION ALL ".join([f"SELECT {cid} as card_id, solution_id, card_{cid}_score as score, super_score FROM solution_scores" for cid in scorable_card_ids])
    single_card_query = f"""
        WITH RankedSolutions AS (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY card_id ORDER BY score DESC, super_score DESC) as rn
            FROM ({unioned_singles})
        )
        SELECT rs.card_id, rs.solution_id, rs.score as best_score, {layout_columns_str}
        FROM RankedSolutions rs JOIN solutions s ON rs.solution_id = s.solution_id
        WHERE rs.rn = 1;
    """
    all_best_singles_df = main_con.execute(single_card_query).fetchdf()
    if not all_best_singles_df.empty:
        best_con.execute("INSERT INTO best_single_card SELECT * FROM all_best_singles_df")
        for _, row in all_best_singles_df.iterrows():
            card_id = row['card_id']
            card_name = sanitize_folder_name(card_lookup.get(card_id, f'card_{card_id}'))
            folder_name = f"{card_id:02d}_{card_name}"
            filename = f"{card_id:02d}_score_{int(row['best_score'])}.png"
            card_dir = os.path.join(maps_1_card_dir, folder_name)
            os.makedirs(card_dir, exist_ok=True)
            image_path = os.path.join(card_dir, filename)
            image_tasks.append((row, image_path))
            print(f"    - Task added for card {card_id}")

    # --- 2. Best solutions for PAIRS of cards (ONE QUERY) ---
    print("  -> Processing all card pairs in one query...")
    best_con.execute(f"CREATE OR REPLACE TABLE best_card_pairs (card_id_1 UTINYINT, card_id_2 UTINYINT, best_solution_id BIGINT, best_score DOUBLE, {layout_definitions_str});")
    
    card_pairs = list(itertools.combinations(scorable_card_ids, 2))
    values_list = ", ".join([f"({c1}, {c2})" for c1, c2 in card_pairs])
    
    pairs_query = f"""
        WITH AllPairs(c1, c2) AS (VALUES {values_list}),
        RankedSolutions AS (
            SELECT
                p.c1, p.c2, s.solution_id,
                ROW_NUMBER() OVER (
                    PARTITION BY p.c1, p.c2
                    ORDER BY (s.card_{{c1}}_score + s.card_{{c2}}_score) DESC, LEAST(s.card_{{c1}}_score, s.card_{{c2}}_score) DESC, s.super_score DESC
                ) as rn
            FROM solution_scores s CROSS JOIN AllPairs p
        )
        SELECT rs.c1, rs.c2, rs.solution_id, (ss.card_{{c1}}_score + ss.card_{{c2}}_score) / 2.0 AS best_score, {layout_columns_str}
        FROM RankedSolutions rs
        JOIN solutions s ON rs.solution_id = s.solution_id
        JOIN solution_scores ss ON rs.solution_id = ss.solution_id
        WHERE rs.rn = 1;
    """
    formatted_pairs_query = pairs_query.format(c1='p.c1', c2='p.c2')
    all_best_pairs_df = main_con.execute(formatted_pairs_query).fetchdf()

    if not all_best_pairs_df.empty:
        best_con.execute("INSERT INTO best_card_pairs SELECT * FROM all_best_pairs_df")
        for _, row in all_best_pairs_df.iterrows():
            c1, c2 = row['c1'], row['c2']
            c1_name = sanitize_folder_name(card_lookup.get(c1, ''))
            c2_name = sanitize_folder_name(card_lookup.get(c2, ''))
            folder1_name = f"{c1:02d}_{c1_name}"
            folder2_name = f"{c2:02d}_{c2_name}"
            filename = f"{c1:02d}_{c2:02d}_score_{int(row['best_score'])}.png"
            card1_dir = os.path.join(maps_2_cards_dir, folder1_name)
            card2_dir = os.path.join(card1_dir, folder2_name)
            os.makedirs(card2_dir, exist_ok=True)
            image_path = os.path.join(card2_dir, filename)
            image_tasks.append((row, image_path))
            print(f"    - Task added for pair {c1}-{c2}")

    # --- 3. Best solutions for TRIOS of cards (ONE QUERY) ---
    print("  -> Processing all card trios in one query...")
    best_con.execute(f"CREATE OR REPLACE TABLE best_card_trios (card_id_1 UTINYINT, card_id_2 UTINYINT, card_id_3 UTINYINT, best_solution_id BIGINT, best_score DOUBLE, {layout_definitions_str});")
    
    card_trios = list(itertools.combinations(scorable_card_ids, 3))
    values_list_trios = ", ".join([f"({c1}, {c2}, {c3})" for c1, c2, c3 in card_trios])
    
    trios_query = f"""
        WITH AllTrios(c1, c2, c3) AS (VALUES {values_list_trios}),
        RankedSolutions AS (
            SELECT
                p.c1, p.c2, p.c3, s.solution_id,
                ROW_NUMBER() OVER (
                    PARTITION BY p.c1, p.c2, p.c3
                    ORDER BY (s.card_{{c1}}_score + s.card_{{c2}}_score + s.card_{{c3}}_score) DESC, LEAST(s.card_{{c1}}_score, s.card_{{c2}}_score, s.card_{{c3}}_score) DESC, s.super_score DESC
                ) as rn
            FROM solution_scores s CROSS JOIN AllTrios p
        )
        SELECT rs.c1, rs.c2, rs.c3, rs.solution_id, (ss.card_{{c1}}_score + ss.card_{{c2}}_score + ss.card_{{c3}}_score) / 3.0 AS best_score, {layout_columns_str}
        FROM RankedSolutions rs
        JOIN solutions s ON rs.solution_id = s.solution_id
        JOIN solution_scores ss ON rs.solution_id = ss.solution_id
        WHERE rs.rn = 1;
    """
    formatted_trios_query = trios_query.format(c1='p.c1', c2='p.c2', c3='p.c3')
    all_best_trios_df = main_con.execute(formatted_trios_query).fetchdf()

    if not all_best_trios_df.empty:
        best_con.execute("INSERT INTO best_card_trios SELECT * FROM all_best_trios_df")
        for _, row in all_best_trios_df.iterrows():
            c1, c2, c3 = row['c1'], row['c2'], row['c3']
            score_str = f"score_{int(row['best_score'])}"
            c1_name, c2_name, c3_name = [sanitize_folder_name(card_lookup.get(c, '')) for c in [c1, c2, c3]]
            folder1_name, folder2_name, folder3_name = f"{c1:02d}_{c1_name}", f"{c2:02d}_{c2_name}", f"{c3:02d}_{c3_name}"
            filename = f"{c1:02d}_{c2:02d}_{c3:02d}_{score_str}.png"
            card1_dir = os.path.join(maps_3_cards_dir, folder1_name)
            card2_dir = os.path.join(card1_dir, folder2_name)
            card3_dir = os.path.join(card2_dir, folder3_name)
            os.makedirs(card3_dir, exist_ok=True)
            image_path = os.path.join(card3_dir, filename)
            image_tasks.append((row, image_path))
            print(f"    - Task added for trio {c1}-{c2}-{c3}")

    # --- 4. Best overall solution (This was already a single query and is fine) ---
    print(f"  -> Processing all {num_scorable_cards} scorable cards...")
    best_con.execute(f"CREATE OR REPLACE TABLE best_overall_solution (best_solution_id BIGINT, best_score DOUBLE, {layout_definitions_str});")
    least_columns = ", ".join([f"card_{cid}_score" for cid in scorable_card_ids])
    query = f"""
        WITH bsi AS (
            SELECT solution_id, super_score / {num_scorable_cards}.0 AS best_score, LEAST({least_columns}) AS min_score
            FROM solution_scores ORDER BY best_score DESC, min_score DESC LIMIT 1
        )
        SELECT s.solution_id, bsi.best_score, {layout_columns_str}
        FROM solutions s JOIN bsi ON s.solution_id = bsi.solution_id;
    """
    result_df = main_con.execute(query).fetchdf()
    if not result_df.empty:
        best_con.execute("INSERT INTO best_overall_solution SELECT * FROM result_df")
        solution_row = result_df.iloc[0]
        score_str = f"score_{int(solution_row['best_score'])}"
        filename = f"all_{num_scorable_cards}_cards_{score_str}.png"
        image_path = os.path.join(maps_all_cards_dir, filename)
        image_tasks.append((solution_row, image_path))
        print("    - Task added for all cards")

    main_con.close()
    best_con.close()
    print(f"✅ Best solutions saved to '{BEST_SOLUTIONS_DB_PATH}'.")

    # --- Parallel image generation ---
    if not image_tasks:
        print("\nNo images to generate.")
        return

    if __name__ == '__main__':
        cpu_cores = os.cpu_count() or 1
        print(f"\n🚀 Generating {len(image_tasks)} map images in parallel using {cpu_cores} cores...")
        with multiprocessing.Pool(cpu_cores, initializer=init_worker) as pool:
            try:
                from tqdm import tqdm
                for _ in tqdm(pool.imap_unordered(generate_image_task, image_tasks), total=len(image_tasks)):
                    pass
            except ImportError:
                pool.map(generate_image_task, image_tasks)
        print("✅ All images generated successfully.")

# =============================================================================
# EXECUTOR PRINCIPAL
# =============================================================================
def main():
    """Orquestra todo o processo de pós-processamento."""
    print("=" * 50)
    print("INICIANDO SCRIPT DE PÓS-PROCESSAMENTO DE SOLUÇÕES")
    print("=" * 50)

    print(f"Procurando pelo arquivo de soluções Parquet mais recente em '{SOURCE_SOLUTIONS_DIR}'...")
    parquet_file, error = find_latest_solution_file(SOURCE_SOLUTIONS_DIR, extension="parquet")

    if error:
        print(f"❌ ERRO: {error}")
        return
    
    print(f"✅ Arquivo de soluções encontrado: '{parquet_file}'")

    try:
        with open(GAME_CARDS_PATH, 'r') as f:
            game_cards = json.load(f)
        print(f"Carregados {len(game_cards)} cartões de '{GAME_CARDS_PATH}'.")
    except FileNotFoundError:
        print(f"ERRO: Arquivo de cartões '{GAME_CARDS_PATH}' não encontrado.")
        return
    except json.JSONDecodeError:
        print(f"ERRO: O arquivo de cartões '{GAME_CARDS_PATH}' não é um JSON válido.")
        return

    # Passo 1: Ingerir o Parquet (sem alterações)
    create_db_from_parquet(parquet_file)

    # Passo 2: Calcular percentis (função foi modificada)
    calculate_percentiles() # << CORREÇÃO: Não precisa mais capturar o resultado.

    # Passo 3: Pré-calcular todos os scores (função foi modificada)
    precompute_all_scores(game_cards) # << CORREÇÃO: Não passa mais o dicionário de percentis.

    # Passo 4: Encontrar melhores soluções e gerar mapas (sem alterações na chamada)
    find_best_solutions_and_generate_maps(game_cards)

    # Mensagens finais
    print("\n" + "=" * 50)
    print("✅ Processo concluído com sucesso!")
    print(f"  -> Bancos de dados de análise salvos em: '{DATABASES_OUTPUT_DIR}/'")
    print(f"  -> Mapas das melhores soluções salvos em: '{SOLUTIONS_OUTPUT_DIR}/'")
    print("=" * 50)


if __name__ == "__main__":
    main()