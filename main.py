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
    Encontra todos os arquivos parquet temporários, os mescla em um único arquivo usando o DuckDB e limpa os arquivos temporários.
    Este método é extremamente eficiente em termos de memória.
    """
    print("\nMesclando resultados de todos os workers usando DuckDB...")
    
    # Usa um padrão glob para que o DuckDB encontre todos os arquivos no diretório
    temp_files_pattern = os.path.join(temp_dir, "*.parquet")

    # O comando SQL para copiar todos os arquivos correspondentes para um novo arquivo Parquet.
    # O DuckDB gerencia toda a memória nos bastidores.
    query = f"""
    COPY (SELECT * FROM read_parquet('{temp_files_pattern}'))
    TO '{final_output_path}'
    WITH (FORMAT PARQUET);
    """
    
    try:
        # Verifica se há arquivos para mesclar
        temp_files_list = glob.glob(temp_files_pattern)
        if not temp_files_list:
            print("Nenhum arquivo temporário encontrado para mesclar.")
            return

        duckdb.execute(query)
        
        # Limpa os arquivos temporários
        for f in temp_files_list:
            os.remove(f)
        os.rmdir(temp_dir)
        print(f"✅ Arquivos mesclados em '{final_output_path}' e arquivos temporários limpos.")
    except Exception as e:
        print(f"❌ Ocorreu um erro durante a mesclagem com o DuckDB: {e}")

def create_duckdb_from_parquet(parquet_path, db_path):
    """
    Creates a DuckDB database by importing a Parquet file.
    """
    print(f"\n--- Creating DuckDB database at '{db_path}' ---")
    
    if os.path.exists(db_path):
        print(f"Database file '{db_path}' already exists. Skipping creation.")
        return

    try:
        con = duckdb.connect(db_path)

        print(f"Importing data from '{parquet_path}'...")
        con.execute(f"CREATE TABLE solutions AS SELECT * FROM read_parquet('{parquet_path}');")
        print("Import complete.")

        # Verification step
        parquet_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
        db_rows = con.execute("SELECT COUNT(*) FROM solutions").fetchone()[0]

        if parquet_rows != db_rows:
            print(f"❌ Verification failed! Row counts do not match ({parquet_rows} vs {db_rows}).")
            con.close()
            return

        print(f"✅ Verification successful. {db_rows:,} rows imported.")
        con.close()

    except Exception as e:
        print(f"❌ An error occurred during DuckDB creation: {e}")

def main():
    # 1. Define the output directory and get the unique, indexed file path
    OUTPUT_DIR = "generated_solutions"
    CHUNK_SIZE = 100_000
    file_path = get_next_filename(OUTPUT_DIR, base_name="tiling_solutions")
    
    # Load data and perform pre-computation
    with open('game/tiles/tiles.json', 'r', encoding='utf-8') as file:
        game_tiles = json.load(file)
    tile_connections = generate_tile_connections(game_tiles)

    # --- MODIFIED: 'start_index' has been removed from the configurations ---
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

    # --- 2. PREPARE TASKS ---
    print("Preparing tasks for parallel execution...")
    tasks = []
    task_id_counter = 0
    for config in search_configs:
        for candidate in config['candidates']:
            tiling = [[() for _ in range(3)] for _ in range(3)]
            tiling[config['start_pos'][0]][config['start_pos'][1]] = candidate
            available_pieces = set(range(1, 9))
            
            uf = UnionFind(NUM_NODES)
            (piece, side, orientation) = candidate
            piece_pos = config['start_pos'][0] * 3 + config['start_pos'][1]
            for road in game_tiles[piece][side]["roads"]:
                l_conn1, l_conn2 = road['connection']
                g_id1 = TILE_NODES[piece_pos][(l_conn1 + orientation) % 4]
                g_id2 = TILE_NODES[piece_pos][(l_conn2 + orientation) % 4]
                uf.union(g_id1, g_id2)

            # Package everything a worker needs into a dictionary
            tasks.append({
                'id': task_id_counter,
                'tiling': tiling,
                'available_pieces': available_pieces,
                'uf_structure': uf,
                'game_tiles': game_tiles,
                'tile_connections': tile_connections
            })
            task_id_counter += 1

    # --- 3. RUN IN PARALLEL ---
    cpu_count = os.cpu_count()
    print(f"Distributing {len(tasks)} tasks across {cpu_count} CPU cores...")
    os.makedirs(TEMP_DIR, exist_ok=True)
    
    # Create a pool of workers and map the tasks to them
    with multiprocessing.Pool(cpu_count) as pool:
        results = pool.map(solve_for_task, tasks)

    # --- 4. MERGE AND FINALIZE ---
    total_solutions = sum(results)
    final_parquet_path = get_next_filename("generated_solutions", "tiling_solutions")
    merge_parquet_files(TEMP_DIR, final_parquet_path)

    # --- 5. NEW: AUTOMATICALLY CREATE THE DUCKDB DATABASE ---
    # This step runs only if the merge was successful and the Parquet file was created.
    if os.path.exists(final_parquet_path):
        # Create a matching database name, e.g., 'tiling_solutions_1.duckdb'
        db_path = os.path.splitext(final_parquet_path)[0] + '.duckdb'
        create_duckdb_from_parquet(final_parquet_path, db_path)

    print("\n-------------------------------------------")
    print(f"✅ All tasks complete. Found a total of {total_solutions:,} solutions.")
    print("-------------------------------------------")

if __name__ == "__main__":
    main()