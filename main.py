# main.py
import json
import os
import duckdb
import multiprocessing
import glob
import numpy as np
import pandas as pd

from solver import find_valid_tilings_generator, find_candidate_tiles
from analysis import UnionFind
from utils import SolutionWriter, get_next_filename
from constants import NUM_NODES, TILE_NODES

# --- Constants for the main script ---
CHUNK_SIZE = 100_000
TEMP_DIR = "temp_solutions" # A dedicated folder for temporary files

def generate_tile_connections(game_tiles):
    # Cria um array NumPy 4D com formato (peça, lado, orientação, conexões)
    # Ex: (9 peças, 2 lados, 4 orientações, 4 pontos de conexão)
    tile_conns_array = np.zeros((9, 2, 4, 4), dtype=np.int8)

    for piece in range(9):
        for side in range(2):
            # 1. Calcula as conexões base para a orientação 0
            base_connections = np.zeros(4, dtype=np.int8)
            # A verificação de segurança para "roads" é mantida
            if side < len(game_tiles[piece]) and "roads" in game_tiles[piece][side]:
                for road in game_tiles[piece][side]["roads"]:
                    base_connections[road['connection'][0]] = 1
                    base_connections[road['connection'][1]] = 1
            
            # 2. Usa np.roll para gerar eficientemente todas as orientações
            for orientation in range(4):
                # np.roll "gira" os elementos do array, simulando a rotação
                tile_conns_array[piece, side, orientation] = np.roll(base_connections, shift=orientation)
    return tile_conns_array

def generate_required_connections_candidates(tile_connections):
    """
    Cria um dicionário que mapeia uma tupla de conexões requeridas
    para uma lista de peças/lados/orientações candidatas.
    """

    def connects(required_connections, tile_connections_to_check):
        # Itera sobre os 4 lados (0: Oeste, 1: Norte, 2: Leste, 3: Sul)
        for i in range(4):
            # Se uma conexão é exigida (não é -1) e não bate com a da peça, não é válida.
            if required_connections[i] != -1 and required_connections[i] != tile_connections_to_check[i]:
                return False
        return True

    # Use um dicionário para o mapeamento. É a estrutura ideal para isso.
    connections_candidates = {}

    # Itera sobre todas as combinações de conexões possíveis (-1, 0, 1)
    for i in range(-1, 2):  # Conexão Oeste
        for j in range(-1, 2):  # Conexão Norte
            for k in range(-1, 2):  # Conexão Leste
                for l in range(-1, 2):  # Conexão Sul

                    required_key = (i, j, k, l)
                    candidates_for_key = []

                    # Agora, encontre todas as peças que satisfazem essa exigência
                    for piece in range(9):
                        for side in range(2):
                            for orientation in range(4):
                                # Pega as conexões da peça atual
                                current_tile_conns = tile_connections[piece, side, orientation]
                                
                                # Se a peça for compatível, adicione à lista de candidatos
                                if connects(required_key, current_tile_conns):
                                    candidates_for_key.append((piece, side, orientation))

                    # Armazena a lista de candidatos no dicionário
                    connections_candidates[required_key] = candidates_for_key

    return connections_candidates

def solve_for_task(task_config):

    worker_id = task_config['id']
    tiling = task_config['tiling']
    available_pieces = task_config['available_pieces']
    uf_structure = task_config['uf_structure']
    game_tiles = task_config['game_tiles']
    tile_connections = task_config['tile_connections']
    connections_candidates = task_config['connections_candidates']

    temp_file_path = os.path.join(TEMP_DIR, f"solutions_{worker_id}.parquet")
    
    initial_domains = {}
    for r in range(3):
        for c in range(3):
            if tiling[r, c, 0] == -1: # Verifica se a célula está vazia
                pos = r * 3 + c
                initial_domains[(r, c)] = find_candidate_tiles(tiling, pos, available_pieces, tile_connections, connections_candidates)

    with SolutionWriter(temp_file_path, CHUNK_SIZE, silent=True, worker_id=worker_id) as writer:
        solution_generator = find_valid_tilings_generator(tiling, available_pieces, game_tiles, tile_connections, connections_candidates, uf_structure, initial_domains)
        writer.process_solutions(solution_generator, game_tiles)
    
    return writer.total_solutions_found

def merge_parquet_files(temp_dir, final_output_path):
    """
    Finds all temporary parquet files, merges them into a single file using DuckDB,
    and cleans up the temporary files. This method is memory-efficient.
    """
    print("\nMesclando resultados de todos os workers usando DuckDB...")
    
    temp_files_pattern = os.path.join(temp_dir, "*.parquet")

    query = f"""
    COPY (SELECT * FROM read_parquet('{temp_files_pattern}'))
    TO '{final_output_path}'
    WITH (FORMAT PARQUET);
    """
    
    try:
        temp_files_list = glob.glob(temp_files_pattern)
        if not temp_files_list:
            print("Nenhum arquivo temporário encontrado para mesclar.")
            return

        # --- MODIFICAÇÃO AQUI ---
        # 1. Conecta a um banco de dados em memória
        con = duckdb.connect()
        
        # 2. Define um limite de RAM. Ex: '16GB'. Ajuste conforme sua RAM disponível.
        #    Use um valor seguro, como 50-70% da sua RAM total.
        con.execute("PRAGMA memory_limit='16GB';")
        
        # 3. Executa a consulta de mesclagem usando a conexão configurada
        print("  -> Iniciando a mesclagem com limite de memória. Isso pode levar algum tempo...")
        con.execute(query)
        
        # 4. Fecha a conexão
        con.close()
        # --- FIM DA MODIFICAÇÃO ---
        
        for f in temp_files_list:
            os.remove(f)
        os.rmdir(temp_dir)
        print(f"✅ Arquivos mesclados em '{final_output_path}' e arquivos temporários limpos.")
    except Exception as e:
        print(f"❌ Ocorreu um erro durante a mesclagem com o DuckDB: {e}")

def main():
    # 1. Define o output directory e get the unique, indexed file path
    OUTPUT_DIR = "generated_solutions"
    CHUNK_SIZE = 100_000
    file_path = get_next_filename(OUTPUT_DIR, base_name="tiling_solutions")
    
    # Load data and perform pre-computation
    with open('game/tiles/tiles.json', 'r', encoding='utf-8') as file:
        game_tiles = json.load(file)
    tile_connections = generate_tile_connections(game_tiles)
    connections_candidates = generate_required_connections_candidates(tile_connections)

    # Configurations for placing the first piece (Piece 6 generates the least branches)
    search_configs = [
        {
            "name": "Piece 6 at top-left corner",
            "start_pos": (0, 0),
            "candidates": [(6, side, orient) for side in range(2) for orient in range(4)]
        },
        {
            "name": "Piece 6 at top-center edge",
            "start_pos": (0, 1),
            "candidates": [(6, side, orient) for side in range(2) for orient in range(4)]
        },
        {
            "name": "Piece 6 at board center",
            "start_pos": (1, 1),
            "candidates": [(6, 0, 0), (6, 1, 0)]
        }
    ]

    # --- 2. PREPARE TASKS ---
    print("Preparing tasks (1 initial piece)...")
    tasks = []
    task_id_counter = 0

    # Generate tasks with a single initial piece placed
    for config in search_configs:
        for candidate in config['candidates']:
            # Initialize the tiling as a NumPy array
            (piece, side, orientation) = candidate
            tiling = np.full((3, 3, 3), -1, dtype=np.int8)
            tiling[config['start_pos'][0], config['start_pos'][1]] = candidate
            
            # CORREÇÃO: Inicializa com todas as 9 peças (0-8) e remove a que foi colocada.
            available_pieces = set(range(9))
            available_pieces.remove(piece)
            
            # Cria a estrutura UnionFind para a tarefa
            uf = UnionFind(NUM_NODES)
            piece_pos = config['start_pos'][0] * 3 + config['start_pos'][1]
            for road in game_tiles[piece][side]["roads"]:
                l_conn1, l_conn2 = road['connection']
                g_id1 = TILE_NODES[piece_pos][(l_conn1 + orientation) % 4]
                g_id2 = TILE_NODES[piece_pos][(l_conn2 + orientation) % 4]
                uf.union(g_id1, g_id2)

            # Empacota a tarefa
            tasks.append({
                'id': task_id_counter,
                'tiling': tiling,
                'available_pieces': available_pieces,
                'uf_structure': uf,
                'game_tiles': game_tiles,
                'tile_connections': tile_connections,
                'connections_candidates': connections_candidates,
            })
            task_id_counter += 1

    # --- 3. RUN IN PARALLEL ---
    cpu_count = os.cpu_count()
    print(f"Distributing {len(tasks)} tasks across {cpu_count} CPU cores...")
    os.makedirs(TEMP_DIR, exist_ok=True)
    
    with multiprocessing.Pool(cpu_count) as pool:
        results = pool.map(solve_for_task, tasks)

    # --- 4. MERGE AND FINALIZE ---
    total_solutions = sum(results)
    final_parquet_path = get_next_filename("generated_solutions", "tiling_solutions")
    merge_parquet_files(TEMP_DIR, final_parquet_path)

    print("\n-------------------------------------------")
    print(f"✅ All tasks complete. Found a total of {total_solutions:,} solutions.")
    print("-------------------------------------------")


if __name__ == "__main__":
    main()
