import numpy as np
import duckdb
import json
import itertools
import os
import time

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
SOURCE_SOLUTIONS_DIR = 'generated_solutions'
CARDS_JSON_PATH = 'game/cards/cards.json'
OUTPUT_PARETO_FILE = 'docs/data/pareto_front.json'

# =============================================================================
# FUNÇÕES DE APOIO MATEMÁTICO
# =============================================================================
def get_pareto_indices(scores_matrix, is_max_array):
    pts = scores_matrix.copy()
    for col_idx, is_max in enumerate(is_max_array):
        if not is_max:
            pts[:, col_idx] = -pts[:, col_idx]

    is_efficient = np.ones(pts.shape[0], dtype=bool)
    for i in range(pts.shape[0]):
        if is_efficient[i]:
            dominated = np.all(pts <= pts[i], axis=1) & np.any(pts < pts[i], axis=1)
            is_efficient[dominated] = False
            
    return np.where(is_efficient)[0]

def find_latest_solution_file(directory, base_name="tiling_solutions", extension="parquet"):
    import re
    if not os.path.isdir(directory): return None
    files = [f for f in os.listdir(directory) if f.endswith(f'.{extension}')]
    # Filtra para ter certeza de pegar o arquivo original, sem ID
    files = [f for f in files if 'with_id' not in f]
    
    if files:
        return os.path.join(directory, sorted(files)[-1])
    return None

# =============================================================================
# MOTOR PRINCIPAL (CORRIGIDO CONTRA OOM)
# =============================================================================
def main():
    print("🚀 Iniciando Motor de Fronteira de Pareto...")
    start_time = time.time()
    
    parquet_file = find_latest_solution_file(SOURCE_SOLUTIONS_DIR)
    if not parquet_file:
        print(f"❌ ERRO: Arquivo parquet válido não encontrado em {SOURCE_SOLUTIONS_DIR}.")
        return
        
    print(f"📂 Lendo dados base de: {parquet_file}")
    
    with open(CARDS_JSON_PATH, 'r', encoding='utf-8') as f:
        cards_data = json.load(f)
        
    valid_cards = [c for c in cards_data if c.get('key') and c.get('type')]
    all_keys = [c['key'] for c in valid_cards]
    
    # Prepara uma pasta temporária para o DuckDB usar como "RAM Extra" (Spilling)
    os.makedirs('temp_duckdb', exist_ok=True)
    
    con = duckdb.connect()
    # Aumentamos o limite para 24GB e ativamos o despejo no disco
    con.execute("PRAGMA memory_limit='24GB';") 
    con.execute("PRAGMA temp_directory='temp_duckdb';")
    
    # =========================================================================
    # A MÁGICA DA COMPRESSÃO COM ANY_VALUE
    # Reduz drasticamente o consumo de RAM removendo o ROW_NUMBER()
    # =========================================================================
    print("🗜️ Comprimindo layouts e extraindo matrizes físicas para a RAM...")
    
    all_columns_sql = ", ".join([f'"{k}"' for k in all_keys])
    
    # Constrói o Struct do Tabuleiro
    board_struct_parts = []
    for r in range(3):
        for c in range(3):
            board_struct_parts.extend([
                f"'p{r}{c}': piece_{r}{c}",
                f"'s{r}{c}': side_{r}{c}",
                f"'o{r}{c}': orient_{r}{c}"
            ])
    board_struct_sql = "{" + ", ".join(board_struct_parts) + "}"
    
    # Usamos ANY_VALUE. É absurdamente mais leve para a RAM pois não exige ordenação.
    con.execute(f"""
        CREATE TABLE unique_states AS 
        SELECT 
            {all_columns_sql},
            ANY_VALUE({board_struct_sql}) as board_data
        FROM read_parquet('{parquet_file}')
        GROUP BY {all_columns_sql}
    """)
    
    unique_count = con.execute("SELECT COUNT(*) FROM unique_states").fetchone()[0]
    print(f"📉 Compressão concluída! Extraídos {unique_count} tabuleiros únicos.\n")

# =========================================================================
    # LOOP DE PROCESSAMENTO (Rodando 100% na RAM)
    # =========================================================================
    pareto_library = {}
    all_combos = []
    total_boards_saved = 0  # <--- NOVO: Contador global de tabuleiros
    
    for r in [1, 2, 3]:
        all_combos.extend(list(itertools.combinations(valid_cards, r)))
        
    total_combos = len(all_combos)
    print(f"⚙️ Processando {total_combos} fronteiras de Pareto...")

    for idx, combo in enumerate(all_combos):
        combo_key = "_".join(str(c['number']) for c in combo)
        keys_to_select = [c['key'] for c in combo]
        is_max_array = [True if c['type'] == 'max' else False for c in combo]
        
        columns_sql = ", ".join([f'"{k}"' for k in keys_to_select])
        
        query = f"""
            SELECT ANY_VALUE(board_data) as board_struct, {columns_sql}
            FROM unique_states
            GROUP BY {columns_sql}
        """
        
        try:
            df_unique = con.execute(query).fetchdf()
        except duckdb.Error as e:
            continue
            
        if df_unique.empty:
            continue
            
        scores_matrix = df_unique[keys_to_select].astype(float).values
        pareto_mask = get_pareto_indices(scores_matrix, is_max_array)
        
        pareto_solutions = []

        # Mantém a matriz como uma lista plana de 9 itens lógicos
        for pareto_idx in pareto_mask:
            row = df_unique.iloc[pareto_idx]
            
            b_data = row['board_struct']
            board_matrix = []
            for r in range(3):
                for c in range(3):
                    board_matrix.append([
                        b_data[f'p{r}{c}'], 
                        b_data[f's{r}{c}'], 
                        b_data[f'o{r}{c}']
                    ])
            
            pareto_solutions.append({
                "scores": [int(row[k]) for k in keys_to_select], # Convertendo o score pra int
                "board": board_matrix
            })
            
        pareto_library[combo_key] = pareto_solutions
        total_boards_saved += len(pareto_solutions) # <--- NOVO: Soma os tabuleiros encontrados
        
        if (idx + 1) % 500 == 0 or (idx + 1) == total_combos:
            elapsed = time.time() - start_time
            print(f"✅ [{idx + 1}/{total_combos}] Combinações processadas... (Tempo decorrido: {elapsed/60:.2f} min)")

    print(f"\n💾 Formatando e salvando biblioteca de Pareto em {OUTPUT_PARETO_FILE}...")
    
    # O separators=(',', ':') arranca ABSOLUTAMENTE TODOS os espaços em branco do JSON
    with open(OUTPUT_PARETO_FILE, 'w') as f:
        json.dump(pareto_library, f, separators=(',', ':'))
        
    total_time = time.time() - start_time
    print(f"🎉 Processo concluído com sucesso em {total_time/60:.2f} minutos!")
    print(f"🧩 TOTAL DE TABULEIROS ÚNICOS (Estratégias Supremas) SALVOS: {total_boards_saved:,}") # <--- NOVO: Log de fechamento
    
    # Limpeza
    con.close()
    try:
        os.rmdir('temp_duckdb')
    except:
        pass

if __name__ == "__main__":
    main()