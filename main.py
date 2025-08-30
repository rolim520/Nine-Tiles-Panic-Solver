# main.py
import json
import os
import duckdb
import multiprocessing
import glob
import pandas as pd

from solver import UnionFind, generate_tile_connections, find_valid_tilings_generator, find_candidate_tiles
from utils import SolutionWriter, get_next_filename
from constants import NUM_NODES, TILE_NODES

# --- Constants for the main script ---
CHUNK_SIZE = 100_000
TEMP_DIR = "temp_solutions" # A dedicated folder for temporary files

def solve_for_task(task_config):
    # --- MODIFIED: 'start_index' has been removed ---
    worker_id = task_config['id']
    tiling = task_config['tiling']
    available_pieces = task_config['available_pieces']
    uf_structure = task_config['uf_structure']
    game_tiles = task_config['game_tiles']
    tile_connections = task_config['tile_connections']

    temp_file_path = os.path.join(TEMP_DIR, f"solutions_{worker_id}.parquet")
    
    initial_domains = {}
    for r in range(3):
        for c in range(3):
            if not tiling[r][c]:
                pos = r * 3 + c
                initial_domains[(r, c)] = find_candidate_tiles(tiling, pos, available_pieces, tile_connections)

    with SolutionWriter(temp_file_path, CHUNK_SIZE, silent=True, worker_id=worker_id) as writer:
        # The new generator is called here, which doesn't use a start_index
        solution_generator = find_valid_tilings_generator(
            tiling, available_pieces, game_tiles, tile_connections, uf_structure, initial_domains
        )
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
    # 1. Define the output directory and get the unique, indexed file path
    OUTPUT_DIR = "generated_solutions"
    CHUNK_SIZE = 100_000
    file_path = get_next_filename(OUTPUT_DIR, base_name="tiling_solutions")
    
    # Load data and perform pre-computation
    with open('game/tiles/tiles.json', 'r', encoding='utf-8') as file:
        game_tiles = json.load(file)
    tile_connections = generate_tile_connections(game_tiles)

    # Configurations for placing the VERY FIRST piece (Piece 0)
    search_configs = [
        {
            "name": "Piece 0 at top-left corner",
            "start_pos": (0, 0),
            "candidates": [(0, side, orient) for side in range(2) for orient in range(4)]
        },
        {
            "name": "Piece 0 at top-center edge",
            "start_pos": (0, 1),
            "candidates": [(0, side, orient) for side in range(2) for orient in range(4)]
        },
        {
            "name": "Piece 0 at board center",
            "start_pos": (1, 1),
            "candidates": [(0, 0, 0), (0, 1, 0)]
        }
    ]

    # --- 2. PREPARE TASKS (LOGIC MODIFIED FOR HIGHER GRANULARITY) ---
    print("Preparing tasks with increased granularity (2 initial pieces)...")
    tasks = []
    task_id_counter = 0

    # Outer loop: Place the first piece (Piece 0)
    for config in search_configs:
        for first_candidate in config['candidates']:
            # Create an intermediate state with the first piece placed
            tiling1 = [[() for _ in range(3)] for _ in range(3)]
            tiling1[config['start_pos'][0]][config['start_pos'][1]] = first_candidate
            
            available_pieces1 = set(range(1, 9)) # Pieces 1-8 are available
            
            uf1 = UnionFind(NUM_NODES)
            (piece1, side1, orientation1) = first_candidate
            pos1 = config['start_pos'][0] * 3 + config['start_pos'][1]
            for road in game_tiles[piece1][side1]["roads"]:
                l_conn1, l_conn2 = road['connection']
                g_id1 = TILE_NODES[pos1][(l_conn1 + orientation1) % 4]
                g_id2 = TILE_NODES[pos1][(l_conn2 + orientation1) % 4]
                uf1.union(g_id1, g_id2)

            # --- LÓGICA DE GRANULARIDADE AUMENTADA ---
            # Inner loop: Iterate through empty spots to place a second piece
            for r2 in range(3):
                for c2 in range(3):
                    if not tiling1[r2][c2]: # If the spot is empty
                        pos2 = r2 * 3 + c2
                        
                        # Find all valid pieces/orientations for this second spot
                        second_piece_candidates = find_candidate_tiles(
                            tiling1, pos2, available_pieces1, tile_connections
                        )
                        
                        # Create a task for each valid second piece placement
                        for second_candidate in second_piece_candidates:
                            # Create the final task state based on the two placed pieces
                            tiling2 = [row[:] for row in tiling1]
                            tiling2[r2][c2] = second_candidate

                            (piece2, side2, orientation2) = second_candidate
                            available_pieces2 = available_pieces1.copy()
                            available_pieces2.remove(piece2)
                            
                            uf2 = uf1.copy() # IMPORTANT: Copy the UF structure
                            for road in game_tiles[piece2][side2]["roads"]:
                                l_conn1, l_conn2 = road['connection']
                                g_id1 = TILE_NODES[pos2][(l_conn1 + orientation2) % 4]
                                g_id2 = TILE_NODES[pos2][(l_conn2 + orientation2) % 4]
                                uf2.union(g_id1, g_id2)

                            # Package everything the worker needs for a 2-piece start
                            tasks.append({
                                'id': task_id_counter,
                                'tiling': tiling2,
                                'available_pieces': available_pieces2,
                                'uf_structure': uf2,
                                'game_tiles': game_tiles,
                                'tile_connections': tile_connections
                            })
                            task_id_counter += 1
            # --- FIM DA LÓGICA DE GRANULARIDADE ---

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