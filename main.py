# main.py
import json
import os
import time
import multiprocessing
import numpy as np

from solver import find_valid_tilings_generator, update_position_domain
from analysis import UnionFind
from utils import SolutionWriter, get_next_filename, merge_parquet_files
from constants import NUM_NODES, TILE_NODES, NORTH, EAST, SOUTH, WEST

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
        # Itera sobre os 4 lados (0: Norte, 1: Leste, 2: Sul, 3: Oeste)
        for i in range(4):
            # Se uma conexão é exigida (não é -1) e não bate com a da peça, não é válida.
            if required_connections[i] != -1 and required_connections[i] != tile_connections_to_check[i]:
                return False
        return True

    # Use um dicionário para o mapeamento. É a estrutura ideal para isso.
    connections_candidates = {}

    # Itera sobre todas as combinações de conexões possíveis (-1, 0, 1)
    for i in range(-1, 2):  # Conexão Norte
        for j in range(-1, 2):  # Conexão Leste
            for k in range(-1, 2):  # Conexão Sul
                for l in range(-1, 2): # Conexão Oeste

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

    task_start_time = time.perf_counter()

    worker_id = task_config['id']
    tiling = task_config['tiling']
    domains = task_config['domains']
    global_nodes = task_config['global_nodes']
    available_pieces = task_config['available_pieces']
    uf_structure = task_config['uf_structure']
    game_tiles = task_config['game_tiles']
    tile_connections = task_config['tile_connections']
    connections_candidates = task_config['connections_candidates']

    temp_file_path = os.path.join(TEMP_DIR, f"solutions_{worker_id}.parquet")
    
    with SolutionWriter(temp_file_path, CHUNK_SIZE, silent=True, worker_id=worker_id) as writer:
        solution_generator = find_valid_tilings_generator(tiling, global_nodes, available_pieces, game_tiles, tile_connections, connections_candidates, uf_structure, domains)
        writer.process_solutions(solution_generator, game_tiles)
    
    task_end_time = time.perf_counter()
    task_duration = task_end_time - task_start_time
    
    # Opcional: Imprime na tela assim que o núcleo terminar
    print(f"✅ Tarefa {worker_id} finalizada em {task_duration:.2f} segundos.")
    
    # Retorna um "relatório" em vez de só um número
    return {
        'worker_id': worker_id,
        'solutions_found': writer.total_solutions_found,
        'duration': task_duration
    }

def main():

    total_start_time = time.perf_counter()

    # Load data and perform pre-computation
    with open('game/tiles/tiles.json', 'r', encoding='utf-8') as file:
        game_tiles = json.load(file)

    tile_connections = generate_tile_connections(game_tiles)
    connections_candidates = generate_required_connections_candidates(tile_connections)

    # Configurations for placing the first piece (Piece 6 generates the least branches)
    search_configs = [
            {
                "name": f"Piece {piece} at board center",
                "start_pos": 4, 
                "candidates": [(piece, 0, 0), (piece, 1, 0)] 
            }
            for piece in range(9)
        ]

    # --- 2. PREPARE TASKS ---
    print("Preparing tasks (1 initial piece)...")
    tasks = []
    task_id_counter = 0

    # Generate tasks with a single initial piece placed
    for config in search_configs:
        for candidate in config['candidates']:

            (piece, side, orientation) = candidate

            start_position = config['start_pos']

            tiling = [None] * 9
            tiling[start_position] = candidate

            # Nasce a lista de 24 nós (todos -1)
            global_nodes = [-1] * 24

            # Preenche os 4 nós da primeira peça jogada
            candidate_connections = tile_connections[piece][side][orientation]
            global_nodes[TILE_NODES[start_position][NORTH]] = candidate_connections[NORTH]
            global_nodes[TILE_NODES[start_position][SOUTH]] = candidate_connections[SOUTH]
            global_nodes[TILE_NODES[start_position][EAST]] = candidate_connections[EAST]
            global_nodes[TILE_NODES[start_position][WEST]] = candidate_connections[WEST]

            available_pieces = set(range(9))
            available_pieces.remove(piece)

            # Atualização dos domínios
            domains = [None] * 9
            for position in range(9):
                if position != start_position:
                    domains[position] = update_position_domain(global_nodes, position, available_pieces, connections_candidates)
                    
            # Cria a estrutura UnionFind para a tarefa
            uf = UnionFind(NUM_NODES)
            for road in game_tiles[piece][side]["roads"]:
                l_conn1, l_conn2 = road['connection']
                g_id1 = TILE_NODES[start_position][(l_conn1 + orientation) % 4]
                g_id2 = TILE_NODES[start_position][(l_conn2 + orientation) % 4]
                uf.union(g_id1, g_id2)

            # Empacota a tarefa
            tasks.append({
                'id': task_id_counter,
                'tiling': tiling,
                'domains': domains,
                'global_nodes': global_nodes,
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
    # Como 'results' agora é uma lista de dicionários, precisamos somar assim:
    total_solutions = sum(r['solutions_found'] for r in results)
    
    final_parquet_path = get_next_filename("generated_solutions", "tiling_solutions")
    merge_parquet_files(TEMP_DIR, final_parquet_path)

    # PARA O CRONÔMETRO GLOBAL
    total_end_time = time.perf_counter()
    total_duration = total_end_time - total_start_time

    # --- IMPRIME O RELATÓRIO FINAL ---
    print("\n===========================================")
    print(" 📊 RELATÓRIO DE DESEMPENHO DOS NÚCLEOS")
    print("===========================================")
    # Ordena os resultados do mais demorado para o mais rápido
    results_sorted = sorted(results, key=lambda x: x['duration'], reverse=True)
    for r in results_sorted:
        print(f"Tarefa {r['worker_id']:02d} | Tempo: {r['duration']:7.2f}s | Soluções: {r['solutions_found']:,}")

    print("\n-------------------------------------------")
    print(f"✅ Execução finalizada!")
    print(f"🧩 Total de soluções encontradas: {total_solutions:,}")
    print(f"⏱️ Tempo Total Absoluto: {total_duration:.2f} segundos ({(total_duration/60):.2f} minutos)")
    print("-------------------------------------------")

if __name__ == "__main__":
    main()
