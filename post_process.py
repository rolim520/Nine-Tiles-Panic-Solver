import duckdb
import itertools
import json
import os
from PIL import Image # Importa a biblioteca Pillow

# =============================================================================
# CONFIGURAÇÃO
# =============================================================================
# Altere estes caminhos para corresponder aos seus arquivos
SOLUTIONS_PARQUET_PATH = 'solutions_0.parquet'
GAME_CARDS_PATH = 'game/cards/cards.json'
TILE_IMAGES_DIR = 'game/tiles/tile_images' # Novo: Caminho para as imagens dos tiles

# Arquivos de banco de dados que serão gerados por este script
MAIN_DB_PATH = 'generated_solutions/solutions.duckdb'
PERCENTILES_DB_PATH = 'generated_solutions/percentiles.duckdb'
BEST_SOLUTIONS_DB_PATH = 'generated_solutions/best_solutions.duckdb'

# Novo: Pasta raiz para os mapas das soluções
MAPS_OUTPUT_DIR = 'generated_solutions/maps'

# Lista de colunas de estatísticas para calcular os percentis.
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

# Cache para imagens de tiles carregadas para evitar recarregar
TILE_IMAGE_CACHE = {}

# =============================================================================
# FUNÇÕES AUXILIARES DE IMAGEM (VERSÃO CORRIGIDA)
# =============================================================================

def load_and_rotate_tile_image(piece, side, orientation, tile_size):
    """Carrega, REDIMENSIONA, rotaciona e armazena em cache a imagem de um tile."""
    # A ordem 'side', 'piece' está correta conforme a última correção
    base_image_path = os.path.join(TILE_IMAGES_DIR, f"{int(side)}_{int(piece)}.png") #
    
    cache_key = (piece, side, orientation, tile_size)

    if cache_key in TILE_IMAGE_CACHE:
        return TILE_IMAGE_CACHE[cache_key]

    try:
        img = Image.open(base_image_path).convert("RGBA") #
        
        resized_img = img.resize((tile_size, tile_size), resample=Image.LANCZOS) #
        
        # --- CORREÇÃO APLICADA AQUI ---
        # Retorna ao ângulo negativo para forçar a rotação no sentido HORÁRIO.
        angle = orientation * -90 #
        # --------------------------------
        
        rotated_img = resized_img.rotate(angle, expand=False, resample=Image.BICUBIC) #
        
        TILE_IMAGE_CACHE[cache_key] = rotated_img #
        return rotated_img
    except FileNotFoundError:
        print(f"ERRO: Imagem do tile não encontrada: {base_image_path}") #
        return Image.new('RGBA', (tile_size, tile_size), (0, 0, 0, 255)) #
    except Exception as e:
        print(f"ERRO ao carregar/redimensionar/rotacionar tile (p:{piece}, s:{side}) com orientação {orientation}: {e}") #
        return Image.new('RGBA', (tile_size, tile_size), (0, 0, 0, 255)) #


def generate_tiling_image(solution_row, output_path):
    """
    Gera a imagem de um tabuleiro 3x3 a partir de uma linha de solução.
    """
    TILE_SIZE = 128 
    BOARD_SIZE = TILE_SIZE * 3

    board_image = Image.new('RGBA', (BOARD_SIZE, BOARD_SIZE), (0, 0, 0, 0))

    for r in range(3):
        for c in range(3):
            pos_str = f"{r}{c}"
            piece = solution_row[f"piece_{pos_str}"]
            side = solution_row[f"side_{pos_str}"]
            orientation = solution_row[f"orient_{pos_str}"]

            # --- CORREÇÃO APLICADA AQUI: Passa o TILE_SIZE para a função auxiliar ---
            tile_img = load_and_rotate_tile_image(piece, side, orientation, TILE_SIZE)
            
            board_image.paste(tile_img, (c * TILE_SIZE, r * TILE_SIZE), tile_img)
            
    board_image.save(output_path, optimize=True, quality=85)

# =============================================================================
# FUNÇÃO 1: CRIAR O BANCO DE DADOS PRINCIPAL
# =============================================================================
def create_db_from_parquet():
    """Ingere o arquivo Parquet em um banco de dados DuckDB para processamento rápido."""
    if os.path.exists(MAIN_DB_PATH):
        print(f"✅ Banco de dados principal '{MAIN_DB_PATH}' já existe. Pulando a ingestão.")
        return

    print(f"🚀 Iniciando a ingestão de '{SOLUTIONS_PARQUET_PATH}' para '{MAIN_DB_PATH}'...")
    print("Isso pode levar um tempo considerável dependendo do tamanho do arquivo.")
    
    con = duckdb.connect(MAIN_DB_PATH)
    con.execute(f"""
        CREATE TABLE solutions AS
        SELECT ROW_NUMBER() OVER () AS solution_id, *
        FROM read_parquet('{SOLUTIONS_PARQUET_PATH}');
    """)
    con.close()
    print(f"✅ Banco de dados principal '{MAIN_DB_PATH}' criado com sucesso.")

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
    
    if os.path.exists(PERCENTILES_DB_PATH):
        os.remove(PERCENTILES_DB_PATH)
    percentiles_con = duckdb.connect(PERCENTILES_DB_PATH)
    percentiles_con.execute("""
        CREATE TABLE stat_percentiles (
            stat_name VARCHAR,
            stat_value INTEGER,
            frequency BIGINT,
            percentile DOUBLE
        );
    """)

    all_percentiles_data = {}

    for i, stat in enumerate(STAT_COLUMNS):
        print(f"  -> Processando estatística {i+1}/{len(STAT_COLUMNS)}: {stat}...")
        
        query = f"""
            WITH ValueCounts AS (
                SELECT "{stat}" AS stat_value, COUNT(*) AS frequency
                FROM solutions GROUP BY "{stat}"
            )
            SELECT '{stat}' AS stat_name, stat_value, frequency,
                   (PERCENT_RANK() OVER (ORDER BY stat_value)) * 100 AS percentile
            FROM ValueCounts;
        """
        
        # --- CORREÇÃO APLICADA AQUI ---
        # 1. Busca os resultados usando a conexão correta (main_con)
        df = main_con.execute(query).fetchdf()
        
        # 2. Insere o DataFrame resultante na tabela de percentis usando a outra conexão
        percentiles_con.execute("INSERT INTO stat_percentiles SELECT * FROM df")
        # --------------------------------

        # Armazena em um dicionário para usar na próxima etapa (lógica inalterada)
        all_percentiles_data[stat] = df.set_index('stat_value')['percentile'].to_dict()

    percentiles_con.close()
    main_con.close()
    
    print(f"✅ Frequências e percentis salvos em '{PERCENTILES_DB_PATH}'.")
    return all_percentiles_data

# =============================================================================
# FUNÇÃO 3: PRÉ-CALCULAR E ARMAZENAR TODOS OS SCORES
# =============================================================================
def precompute_all_scores(stat_percentiles, game_cards):
    """Gera e armazena scores para todas as soluções e todas as cartas."""
    print("\n🚀 Pré-calculando scores para todas as soluções. Este é o passo mais longo...")
    
    con = duckdb.connect(MAIN_DB_PATH)

    # --- NOVA LINHA: CONFIGURAÇÃO DE MEMÓRIA ---
    # Define um limite de 16GB de RAM. O DuckDB usará o disco para o que exceder.
    # Ajuste este valor conforme a RAM disponível na sua máquina (ex: '8GB', '16GB').
    con.execute("PRAGMA memory_limit='16GB';")
    # Opcional: defina um diretório para os arquivos temporários em um SSD rápido
    # con.execute("PRAGMA temp_directory='/path/to/fast/ssd/temp.tmp';")
    # -----------------------------------------

    try:
        con.execute("SELECT card_1_score FROM solution_scores LIMIT 1;")
        print("✅ Tabela 'solution_scores' já existe. Pulando o cálculo de scores.")
        con.close()
        return
    except duckdb.CatalogException:
        print("  -> Tabela 'solution_scores' não encontrada. Iniciando o cálculo.")
        pass

    select_clauses = ["solution_id"]
    score_sums = []

    for i, card in enumerate(game_cards):
        card_num = i + 1
        card_key = card.get('key')
        card_type = card.get('type')
        
        if not card_key or card_key not in stat_percentiles:
            print(f"  -> Aviso: Chave do cartão {card_num} ('{card_key}') inválida ou não encontrada. Pulando.")
            continue

        percentiles_map = stat_percentiles[card_key]
        
        # --- CORREÇÃO APLICADA AQUI ---
        # Adicionamos CAST(... AS DOUBLE) para forçar o tipo de dado e evitar overflow.
        case_sql = f"\n    CAST((CASE \"{card_key}\""
        for value, percentile in percentiles_map.items():
            score = percentile if card_type == 'max' else 100.0 - percentile
            case_sql += f"\n        WHEN {value} THEN {score}"
        case_sql += f"\n        ELSE 0.0\n    END) AS DOUBLE) AS card_{card_num}_score"
        # --------------------------------
        
        select_clauses.append(case_sql)
        score_sums.append(f"card_{card_num}_score")

    super_score_sql = " + ".join(score_sums)
    select_clauses.append(f"({super_score_sql}) AS super_score")
    
    full_select_query = ",\n".join(select_clauses)
    
    create_table_query = f"CREATE TABLE solution_scores AS SELECT {full_select_query} FROM solutions;"
    
    print("  -> Executando a consulta massiva de criação de scores. Seja paciente...")
    con.execute(create_table_query)
    
    print("  -> Criando índice em super_score para desempate rápido...")
    con.execute("CREATE INDEX idx_super_score ON solution_scores (super_score DESC);")
    
    con.close()
    print("✅ Tabela 'solution_scores' criada e indexada com sucesso.")


# =============================================================================
# FUNÇÃO 4: ENCONTRAR AS MELHORES SOLUÇÕES COMPLETAS E GERAR MAPAS
# =============================================================================
def find_best_solutions_and_generate_maps(game_cards):
    """
    Encontra as melhores soluções, armazena seus layouts completos, normaliza o
    score e gera as imagens dos mapas, ignorando cartas não pontuáveis.
    """
    print("\n🚀 Encontrando as melhores soluções para combinações de cartas e gerando mapas...")

    main_con = duckdb.connect(MAIN_DB_PATH, read_only=True)
    
    if os.path.exists(BEST_SOLUTIONS_DB_PATH):
        os.remove(BEST_SOLUTIONS_DB_PATH)
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
    
    # --- CORREÇÃO APLICADA AQUI ---
    # Cria uma lista apenas com os IDs das cartas que podem ser pontuadas (têm uma 'key')
    scorable_card_ids = [card['number'] for card in game_cards if card.get('key')]
    num_scorable_cards = len(scorable_card_ids)
    print(f"  -> Encontradas {num_scorable_cards} cartas pontuáveis de um total de {len(game_cards)}.")
    # --------------------------------
    
    # ... (criação de diretórios inalterada) ...
    maps_1_card_dir = os.path.join(MAPS_OUTPUT_DIR, '1_card')
    maps_2_cards_dir = os.path.join(MAPS_OUTPUT_DIR, '2_cards')
    maps_3_cards_dir = os.path.join(MAPS_OUTPUT_DIR, '3_cards')
    maps_all_cards_dir = os.path.join(MAPS_OUTPUT_DIR, 'all_cards')

    os.makedirs(maps_1_card_dir, exist_ok=True)
    os.makedirs(maps_2_cards_dir, exist_ok=True)
    os.makedirs(maps_3_cards_dir, exist_ok=True)
    os.makedirs(maps_all_cards_dir, exist_ok=True)


    # --- 1. Melhores soluções para cartas individuais ---
    print("  -> Processando 1 carta por vez...")
    best_con.execute(f"CREATE TABLE best_single_card (card_id UTINYINT, best_solution_id BIGINT, best_score DOUBLE, {layout_definitions_str});")
    # CORREÇÃO: Itera apenas sobre os IDs das cartas pontuáveis
    for card_id in scorable_card_ids:
        query = f"""
            WITH bsi AS (
                SELECT solution_id, card_{card_id}_score AS score
                FROM solution_scores ORDER BY score DESC, super_score DESC LIMIT 1
            )
            SELECT {card_id}, s.solution_id, bsi.score, {layout_columns_str}
            FROM solutions s JOIN bsi ON s.solution_id = bsi.solution_id;
        """
        result_df = main_con.execute(query).fetchdf()
        if not result_df.empty:
            best_con.execute("INSERT INTO best_single_card SELECT * FROM result_df")
            solution_row = result_df.iloc[0]
            card_dir = os.path.join(maps_1_card_dir, str(card_id))
            os.makedirs(card_dir, exist_ok=True)
            image_path = os.path.join(card_dir, f"{card_id}.png")
            generate_tiling_image(solution_row, image_path)
            print(f"    - Mapa gerado para carta {card_id}: {image_path}")

    # --- 2. Melhores soluções para pares de cartas ---
    print("  -> Processando 2 cartas por vez...")
    best_con.execute(f"CREATE TABLE best_card_pairs (card_id_1 UTINYINT, card_id_2 UTINYINT, best_solution_id BIGINT, best_score DOUBLE, {layout_definitions_str});")
    # CORREÇÃO: Gera combinações apenas com os IDs das cartas pontuáveis
    for c1, c2 in itertools.combinations(scorable_card_ids, 2):
        query = f"""
            WITH bsi AS (
                SELECT solution_id, (card_{c1}_score + card_{c2}_score) / 2.0 AS score
                FROM solution_scores ORDER BY score DESC, super_score DESC LIMIT 1
            )
            SELECT {c1}, {c2}, s.solution_id, bsi.score, {layout_columns_str}
            FROM solutions s JOIN bsi ON s.solution_id = bsi.solution_id;
        """
        result_df = main_con.execute(query).fetchdf()
        if not result_df.empty:
            best_con.execute("INSERT INTO best_card_pairs SELECT * FROM result_df")
            solution_row = result_df.iloc[0]
            card1_dir = os.path.join(maps_2_cards_dir, str(c1))
            card2_dir = os.path.join(card1_dir, str(c2))
            os.makedirs(card2_dir, exist_ok=True)
            image_path = os.path.join(card2_dir, f"{c1}_{c2}.png")
            generate_tiling_image(solution_row, image_path)
            print(f"    - Mapa gerado para cartas {c1}_{c2}: {image_path}")

    # --- 3. Melhores soluções para trios de cartas ---
    print("  -> Processando 3 cartas por vez...")
    best_con.execute(f"CREATE TABLE best_card_trios (card_id_1 UTINYINT, card_id_2 UTINYINT, card_id_3 UTINYINT, best_solution_id BIGINT, best_score DOUBLE, {layout_definitions_str});")
    # CORREÇÃO: Gera combinações apenas com os IDs das cartas pontuáveis
    for c1, c2, c3 in itertools.combinations(scorable_card_ids, 3):
        query = f"""
            WITH bsi AS (
                SELECT solution_id, (card_{c1}_score + card_{c2}_score + card_{c3}_score) / 3.0 AS score
                FROM solution_scores ORDER BY score DESC, super_score DESC LIMIT 1
            )
            SELECT {c1}, {c2}, {c3}, s.solution_id, bsi.score, {layout_columns_str}
            FROM solutions s JOIN bsi ON s.solution_id = bsi.solution_id;
        """
        result_df = main_con.execute(query).fetchdf()
        if not result_df.empty:
            best_con.execute("INSERT INTO best_card_trios SELECT * FROM result_df")
            solution_row = result_df.iloc[0]
            card1_dir = os.path.join(maps_3_cards_dir, str(c1))
            card2_dir = os.path.join(card1_dir, str(c2))
            card3_dir = os.path.join(card2_dir, str(c3))
            os.makedirs(card3_dir, exist_ok=True)
            image_path = os.path.join(card3_dir, f"{c1}_{c2}_{c3}.png")
            generate_tiling_image(solution_row, image_path)
            print(f"    - Mapa gerado para cartas {c1}_{c2}_{c3}: {image_path}")
    
    # --- 4. Melhor solução geral (todas as 26 cartas) ---
    print(f"  -> Processando todas as {num_scorable_cards} cartas pontuáveis...")
    best_con.execute(f"CREATE TABLE best_overall_solution (best_solution_id BIGINT, best_score DOUBLE, {layout_definitions_str});")
    # CORREÇÃO: Usa o super_score, que já soma apenas as cartas pontuáveis
    query = f"""
        WITH bsi AS (
            SELECT solution_id, super_score / {num_scorable_cards}.0 AS score
            FROM solution_scores ORDER BY score DESC LIMIT 1
        )
        SELECT s.solution_id, bsi.score, {layout_columns_str}
        FROM solutions s JOIN bsi ON s.solution_id = bsi.solution_id;
    """
    result_df = main_con.execute(query).fetchdf()
    if not result_df.empty:
        best_con.execute("INSERT INTO best_overall_solution SELECT * FROM result_df")
        solution_row = result_df.iloc[0]
        image_path = os.path.join(maps_all_cards_dir, f"all_{num_scorable_cards}_cards.png")
        generate_tiling_image(solution_row, image_path)
        print(f"    - Mapa gerado para todas as cartas: {image_path}")

    main_con.close()
    best_con.close()
    print(f"✅ Melhores soluções (com layouts e scores normalizados) salvas em '{BEST_SOLUTIONS_DB_PATH}'.")

# =============================================================================
# EXECUTOR PRINCIPAL
# =============================================================================
def main():
    """Orquestra todo o processo de pós-processamento."""
    print("=" * 50)
    print("INICIANDO SCRIPT DE PÓS-PROCESSAMENTO DE SOLUÇÕES")
    print("=" * 50)

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

    # Passo 1: Garantir que o banco de dados principal exista
    create_db_from_parquet()

    # Passo 2: Calcular percentis (necessário para o cálculo de scores)
    stat_percentiles_data = calculate_percentiles()
    
    # Passo 3: Pré-calcular todos os scores
    precompute_all_scores(stat_percentiles_data, game_cards)

    # Passo 4: Encontrar e salvar as melhores soluções E gerar os mapas
    # Renomeada para refletir a nova funcionalidade
    find_best_solutions_and_generate_maps(game_cards)

    print("\n" + "=" * 50)
    print("✅ Processo concluído com sucesso!")
    print(f"  -> DB principal de trabalho: '{MAIN_DB_PATH}'")
    print(f"  -> Percentis estão em:      '{PERCENTILES_DB_PATH}'")
    print(f"  -> Melhores soluções (DB):  '{BEST_SOLUTIONS_DB_PATH}'")
    print(f"  -> Mapas gerados em:        '{MAPS_OUTPUT_DIR}'")
    print("=" * 50)


if __name__ == "__main__":
    main()