import duckdb
import itertools
import json
import os
import re # Adicionado import
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
TILE_IMAGE_CACHE = {}
RESIZED_IMAGE_CACHE = {}

# =============================================================================
# FUNÇÕES AUXILIARES DE IMAGEM (VERSÃO CORRIGIDA)
# =============================================================================

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

def load_and_rotate_tile_image(piece, side, orientation, tile_size):
    base_image_path = os.path.join(TILE_IMAGES_DIR, f"{int(side)}_{int(piece)}.png")
    cache_key = (piece, side, orientation, tile_size)
    if cache_key in TILE_IMAGE_CACHE: return TILE_IMAGE_CACHE[cache_key]
    try:
        img = Image.open(base_image_path).convert("RGBA")
        resized_img = img.resize((tile_size, tile_size), resample=Image.LANCZOS)
        angle = orientation * -90
        rotated_img = resized_img.rotate(angle, expand=False, resample=Image.BICUBIC)
        TILE_IMAGE_CACHE[cache_key] = rotated_img
        return rotated_img
    except Exception as e:
        print(f"ERRO ao processar tile (p:{piece}, s:{side}): {e}")
        return Image.new('RGBA', (tile_size, tile_size), (0, 0, 0, 255))

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

def preload_resized_images(tile_size):
    print("🚀 Pré-carregando e redimensionando todas as imagens base dos tiles...")
    for side in range(2):
        for piece in range(9):
            cache_key = (piece, side)
            try:
                base_image_path = os.path.join(TILE_IMAGES_DIR, f"{int(side)}_{int(piece)}.png")
                img = Image.open(base_image_path).convert("RGBA")
                resized_img = img.resize((tile_size, tile_size), resample=Image.LANCZOS)
                RESIZED_IMAGE_CACHE[cache_key] = resized_img
            except Exception as e:
                print(f"ERRO durante o pré-carregamento do tile (p:{piece}, s:{side}): {e}")
                RESIZED_IMAGE_CACHE[cache_key] = None
    print("✅ Cache de imagens redimensionadas criado.")

# =============================================================================
# FUNÇÃO 1: CRIAR O BANCO DE DADOS PRINCIPAL
# =============================================================================
def create_db_from_parquet(parquet_file_path):
    """Ingere o arquivo Parquet em um banco de dados DuckDB para processamento rápido."""
    os.makedirs(DATABASES_OUTPUT_DIR, exist_ok=True)
    
    con = duckdb.connect(MAIN_DB_PATH)
    
    # --- ADICIONADO: Define um limite de memória para a ingestão ---
    # Ajuste este valor para ~70% da sua RAM total (ex: '12GB', '24GB')
    con.execute("PRAGMA memory_limit='12GB';") 
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
# =============================================================================
# FUNÇÃO 2: CALCULAR PERCENTIS E FREQUÊNCIAS
# =============================================================================
def calculate_percentiles():
    """Calcula frequência e percentil para cada valor de cada estatística."""
    print("\n🚀 Calculando frequências e percentis para todas as estatísticas...")
    
    main_con = duckdb.connect(MAIN_DB_PATH, read_only=True)
    main_con.execute("PRAGMA memory_limit='12GB';")
    
    os.makedirs(DATABASES_OUTPUT_DIR, exist_ok=True)
    
    percentiles_con = duckdb.connect(PERCENTILES_DB_PATH)
    percentiles_con.execute("""
        CREATE OR REPLACE TABLE stat_percentiles (
            stat_name VARCHAR, stat_value INTEGER, frequency BIGINT, percentile DOUBLE
        );
    """)

    all_percentiles_data = {}

    for i, stat in enumerate(STAT_COLUMNS):
        print(f"  -> Processando estatística {i+1}/{len(STAT_COLUMNS)}: {stat}...")
        
        # --- CORREÇÃO APLICADA AQUI: Consulta SQL completa ---
        query = f"""
            WITH ValueCounts AS (
                SELECT "{stat}" AS stat_value, COUNT(*) AS frequency
                FROM solutions GROUP BY "{stat}"
            )
            SELECT 
                '{stat}' AS stat_name, 
                stat_value, 
                frequency,
                (PERCENT_RANK() OVER (ORDER BY stat_value)) * 100 AS percentile
            FROM ValueCounts;
        """
        # --------------------------------------------------------
        
        df = main_con.execute(query).fetchdf()
        percentiles_con.execute("INSERT INTO stat_percentiles SELECT * FROM df")
        all_percentiles_data[stat] = df.set_index('stat_value')['percentile'].to_dict()

    percentiles_con.close()
    main_con.close()
    
    print(f"✅ Frequências e percentis salvos em '{PERCENTILES_DB_PATH}'.")
    return all_percentiles_data

# =============================================================================
# FUNÇÃO 3: PRÉ-CALCULAR E ARMAZENAR TODOS OS SCORES
# =============================================================================
def precompute_all_scores(stat_percentiles, game_cards):
    """
    Gera e armazena scores para todas as soluções, processando em lotes
    para controlar o uso de memória em datasets muito grandes.
    """
    print("\n🚀 Pré-calculando scores para todas as soluções. Este é o passo mais longo...")
    
    con = duckdb.connect(MAIN_DB_PATH)
    
    # Define um limite de memória seguro para CADA operação de lote.
    # Ajuste para ~70% da sua RAM total (ex: '12GB', '24GB').
    con.execute("PRAGMA memory_limit='12GB';")

    try:
        con.execute("SELECT card_1_score FROM solution_scores LIMIT 1;")
        print("✅ Tabela 'solution_scores' já existe. Pulando o cálculo de scores.")
        con.close()
        return
    except duckdb.CatalogException:
        print("  -> Tabela 'solution_scores' não encontrada. Iniciando o cálculo em lotes.")
        pass

    # Lógica para construir a string da consulta (inalterada)
    select_clauses = ["solution_id"]
    score_sums = []
    for i, card in enumerate(game_cards):
        card_num = i + 1
        card_key = card.get('key')
        if not card_key or card_key not in stat_percentiles: continue
        card_type = card.get('type')
        percentiles_map = stat_percentiles[card_key]
        case_sql = f"\n    CAST((CASE \"{card_key}\""
        for value, percentile in percentiles_map.items():
            score = percentile if card_type == 'max' else 100.0 - percentile
            case_sql += f"\n        WHEN {value} THEN {score}"
        case_sql += f"\n        ELSE 0.0\n    END) AS DOUBLE) AS card_{card_num}_score"
        select_clauses.append(case_sql)
        score_sums.append(f"card_{card_num}_score")
    super_score_sql = " + ".join(score_sums)
    select_clauses.append(f"({super_score_sql}) AS super_score")
    full_select_query = ",\n".join(select_clauses)

    # Passo 1: Criar a tabela de destino VAZIA com a estrutura correta
    print("  -> Criando a estrutura da tabela 'solution_scores'...")
    con.execute(f"CREATE TABLE solution_scores AS SELECT {full_select_query} FROM solutions WHERE 1=0;")

    # Passo 2: Processar e inserir os dados em lotes
    total_rows = con.execute("SELECT COUNT(*) FROM solutions").fetchone()[0]
    CHUNK_SIZE = 50_000_000  # Processar 50 milhões de linhas por vez (pode ajustar)
    
    print(f"  -> Processando {total_rows:,} linhas em lotes de {CHUNK_SIZE:,}...")

    for offset in range(0, total_rows, CHUNK_SIZE):
        print(f"    - Processando lote: {offset:,} a {offset + CHUNK_SIZE:,}...")
        
        batch_query = f"""
            INSERT INTO solution_scores
            SELECT {full_select_query}
            FROM solutions
            LIMIT {CHUNK_SIZE} OFFSET {offset};
        """
        con.execute(batch_query)

    print("  -> Criando índice em super_score para desempate rápido...")
    con.execute("CREATE INDEX idx_super_score ON solution_scores (super_score DESC);")
    
    con.close()
    print("✅ Tabela 'solution_scores' criada e indexada com sucesso.")


# =============================================================================
# FUNÇÃO 4: ENCONTRAR AS MELHORES SOLUÇÕES E GERAR MAPAS (VERSÃO COM NOME DE ARQUIVO SIMPLIFICADO)
# =============================================================================
def find_best_solutions_and_generate_maps(game_cards):
    """
    Finds the best balanced solutions for card combinations, stores their layouts,
    normalizes the score, and generates map images.
    """
    print("\n🚀 Encontrando as melhores soluções equilibradas para combinações de cartas...")

    main_con = duckdb.connect(MAIN_DB_PATH, read_only=True)
    best_con = duckdb.connect(BEST_SOLUTIONS_DB_PATH)

    # --- Setup ---
    layout_columns = []
    layout_definitions = []
    for r in range(3):
        for c in range(3):
            pos = f"{r}{c}"
            layout_columns.extend([f'"piece_{pos}"', f'"side_{pos}"', f'"orient_{pos}"'])
            layout_definitions.extend([f'"piece_{pos}" UTINYINT', f'"side_{pos}" UTINYINT', f'"orient_{pos}" UTINYINT'])
    
    layout_columns_str = ", ".join(layout_columns)
    layout_definitions_str = ", ".join(layout_definitions)
    
    card_lookup = {card['number']: card['name'] for card in game_cards}
    scorable_card_ids = [card['number'] for card in game_cards if card.get('key')]
    num_scorable_cards = len(scorable_card_ids)
    print(f"  -> Encontradas {num_scorable_cards} cartas pontuáveis de um total de {len(game_cards)}.")
    
    maps_1_card_dir = os.path.join(MAPS_OUTPUT_DIR, '1_card')
    maps_2_cards_dir = os.path.join(MAPS_OUTPUT_DIR, '2_cards')
    maps_3_cards_dir = os.path.join(MAPS_OUTPUT_DIR, '3_cards')
    maps_all_cards_dir = os.path.join(MAPS_OUTPUT_DIR, 'all_cards')
    os.makedirs(maps_1_card_dir, exist_ok=True)
    os.makedirs(maps_2_cards_dir, exist_ok=True)
    os.makedirs(maps_3_cards_dir, exist_ok=True)
    os.makedirs(maps_all_cards_dir, exist_ok=True)

    # --- 1. Best solutions for single cards (no balancing needed) ---
    print("  -> Processando 1 carta por vez...")
    best_con.execute(f"CREATE OR REPLACE TABLE best_single_card (card_id UTINYINT, best_solution_id BIGINT, best_score DOUBLE, {layout_definitions_str});")
    for card_id in scorable_card_ids:
        query = f"""
            WITH bsi AS (
                SELECT solution_id, card_{card_id}_score AS best_score
                FROM solution_scores ORDER BY best_score DESC, super_score DESC LIMIT 1
            )
            SELECT {card_id}, s.solution_id, bsi.best_score, {layout_columns_str}
            FROM solutions s JOIN bsi ON s.solution_id = bsi.solution_id;
        """
        result_df = main_con.execute(query).fetchdf()
        if not result_df.empty:
            best_con.execute("INSERT INTO best_single_card SELECT * FROM result_df")
            solution_row = result_df.iloc[0]
            score_str = f"score_{int(solution_row['best_score'])}"
            card_name = sanitize_folder_name(card_lookup.get(card_id, ''))
            folder_name = f"{card_id}_{card_name}"
            filename = f"{card_id}_{score_str}.png"
            card_dir = os.path.join(maps_1_card_dir, folder_name)
            os.makedirs(card_dir, exist_ok=True)
            image_path = os.path.join(card_dir, filename)
            generate_tiling_image(solution_row, image_path)
            print(f"    - Mapa gerado para carta {card_id}: {image_path}")

    # --- 2. Best solutions for pairs of cards (with balancing) ---
    print("  -> Processando 2 cartas por vez...")
    best_con.execute(f"CREATE OR REPLACE TABLE best_card_pairs (card_id_1 UTINYINT, card_id_2 UTINYINT, best_solution_id BIGINT, best_score DOUBLE, {layout_definitions_str});")
    for c1, c2 in itertools.combinations(scorable_card_ids, 2):
        query = f"""
            WITH bsi AS (
                SELECT 
                    solution_id, 
                    (card_{c1}_score + card_{c2}_score) / 2.0 AS best_score,
                    LEAST(card_{c1}_score, card_{c2}_score) AS min_score
                FROM solution_scores 
                ORDER BY best_score DESC, min_score DESC, super_score DESC 
                LIMIT 1
            )
            SELECT {c1}, {c2}, s.solution_id, bsi.best_score, {layout_columns_str}
            FROM solutions s JOIN bsi ON s.solution_id = bsi.solution_id;
        """
        result_df = main_con.execute(query).fetchdf()
        if not result_df.empty:
            best_con.execute("INSERT INTO best_card_pairs SELECT * FROM result_df")
            solution_row = result_df.iloc[0]
            score_str = f"score_{int(solution_row['best_score'])}"
            c1_name = sanitize_folder_name(card_lookup.get(c1, ''))
            c2_name = sanitize_folder_name(card_lookup.get(c2, ''))
            folder1_name = f"{c1}_{c1_name}"
            folder2_name = f"{c2}_{c2_name}"
            filename = f"{c1}_{c2}_{score_str}.png"
            card1_dir = os.path.join(maps_2_cards_dir, folder1_name)
            card2_dir = os.path.join(card1_dir, folder2_name)
            os.makedirs(card2_dir, exist_ok=True)
            image_path = os.path.join(card2_dir, filename)
            generate_tiling_image(solution_row, image_path)
            print(f"    - Mapa gerado para cartas {c1}_{c2}: {image_path}")

    # --- 3. Best solutions for trios of cards (with balancing) ---
    print("  -> Processando 3 cartas por vez...")
    best_con.execute(f"CREATE OR REPLACE TABLE best_card_trios (card_id_1 UTINYINT, card_id_2 UTINYINT, card_id_3 UTINYINT, best_solution_id BIGINT, best_score DOUBLE, {layout_definitions_str});")
    for c1, c2, c3 in itertools.combinations(scorable_card_ids, 3):
        query = f"""
            WITH bsi AS (
                SELECT 
                    solution_id, 
                    (card_{c1}_score + card_{c2}_score + card_{c3}_score) / 3.0 AS best_score,
                    LEAST(card_{c1}_score, card_{c2}_score, card_{c3}_score) AS min_score
                FROM solution_scores 
                ORDER BY best_score DESC, min_score DESC, super_score DESC 
                LIMIT 1
            )
            SELECT {c1}, {c2}, {c3}, s.solution_id, bsi.best_score, {layout_columns_str}
            FROM solutions s JOIN bsi ON s.solution_id = bsi.solution_id;
        """
        result_df = main_con.execute(query).fetchdf()
        if not result_df.empty:
            best_con.execute("INSERT INTO best_card_trios SELECT * FROM result_df")
            solution_row = result_df.iloc[0]
            score_str = f"score_{int(solution_row['best_score'])}"
            c1_name = sanitize_folder_name(card_lookup.get(c1, ''))
            c2_name = sanitize_folder_name(card_lookup.get(c2, ''))
            c3_name = sanitize_folder_name(card_lookup.get(c3, ''))
            folder1_name = f"{c1}_{c1_name}"
            folder2_name = f"{c2}_{c2_name}"
            folder3_name = f"{c3}_{c3_name}"
            filename = f"{c1}_{c2}_{c3}_{score_str}.png"
            card1_dir = os.path.join(maps_3_cards_dir, folder1_name)
            card2_dir = os.path.join(card1_dir, folder2_name)
            card3_dir = os.path.join(card2_dir, folder3_name)
            os.makedirs(card3_dir, exist_ok=True)
            image_path = os.path.join(card3_dir, filename)
            generate_tiling_image(solution_row, image_path)
            print(f"    - Mapa gerado para cartas {c1}_{c2}_{c3}: {image_path}")

    # --- 4. Best overall solution (with balancing) ---
    print(f"  -> Processando todas as {num_scorable_cards} cartas pontuáveis...")
    best_con.execute(f"CREATE OR REPLACE TABLE best_overall_solution (best_solution_id BIGINT, best_score DOUBLE, {layout_definitions_str});")
    least_columns = ", ".join([f"card_{cid}_score" for cid in scorable_card_ids])
    query = f"""
        WITH bsi AS (
            SELECT 
                solution_id, 
                super_score / {num_scorable_cards}.0 AS best_score,
                LEAST({least_columns}) AS min_score
            FROM solution_scores 
            ORDER BY best_score DESC, min_score DESC
            LIMIT 1
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
        generate_tiling_image(solution_row, image_path)
        print(f"    - Mapa gerado para todas as cartas: {image_path}")

    main_con.close()
    best_con.close()
    print(f"✅ Melhores soluções equilibradas salvas em '{BEST_SOLUTIONS_DB_PATH}'.")

# =============================================================================
# EXECUTOR PRINCIPAL
# =============================================================================
def main():
    """Orquestra todo o processo de pós-processamento."""
    print("=" * 50)
    print("INICIANDO SCRIPT DE PÓS-PROCESSAMENTO DE SOLUÇÕES")
    print("=" * 50)

    # --- MODIFICADO: Encontrar o arquivo de soluções mais recente ---
    print(f"Procurando pelo arquivo de soluções Parquet mais recente em '{SOURCE_SOLUTIONS_DIR}'...")
    parquet_file, error = find_latest_solution_file(SOURCE_SOLUTIONS_DIR, extension="parquet")

    if error:
        print(f"❌ ERRO: {error}")
        return
    
    print(f"✅ Arquivo de soluções encontrado: '{parquet_file}'")
    # ----------------------------------------------------------------

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

    # Passo 1: Passa o caminho do arquivo encontrado
    create_db_from_parquet(parquet_file)

    # Passo 2: Calcular percentis
    stat_percentiles_data = calculate_percentiles()
    
    # Passo 3: Pré-calcular todos os scores
    precompute_all_scores(stat_percentiles_data, game_cards)

    # Passo 4: Encontrar e salvar as melhores soluções E gerar os mapas
    find_best_solutions_and_generate_maps(game_cards)

    # Mensagens finais atualizadas
    print("\n" + "=" * 50)
    print("✅ Processo concluído com sucesso!")
    print(f"  -> Bancos de dados de análise salvos em: '{DATABASES_OUTPUT_DIR}/'")
    print(f"  -> Mapas das melhores soluções salvos em: '{SOLUTIONS_OUTPUT_DIR}/'")
    print("=" * 50)


if __name__ == "__main__":
    main()