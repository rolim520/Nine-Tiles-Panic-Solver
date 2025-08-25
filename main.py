# main.py
import json
import os
import multiprocessing
import glob
import pandas as pd

from solver import UnionFind, generate_tile_connections, find_valid_tilings_generator
from utils import SolutionWriter, get_next_filename
from constants import NUM_NODES, TILE_NODES

# --- Constants for the main script ---
CHUNK_SIZE = 100_000
TEMP_DIR = "temp_solutions" # A dedicated folder for temporary files

def solve_for_task(task_config):
    """
    This is the worker function that each process will run.
    It solves for a single starting configuration and writes to a unique temp file.
    """
    # 1. Unpack all the necessary data for the solver
    worker_id = task_config['id']
    start_index = task_config['start_index']
    tiling = task_config['tiling']
    available_pieces = task_config['available_pieces']
    uf_structure = task_config['uf_structure']
    game_tiles = task_config['game_tiles']
    tile_connections = task_config['tile_connections']

    temp_file_path = os.path.join(TEMP_DIR, f"solutions_{worker_id}.parquet")

    # Pass the worker_id to the writer
    with SolutionWriter(temp_file_path, CHUNK_SIZE, silent=True, worker_id=worker_id) as writer:
        solution_generator = find_valid_tilings_generator(
            tiling, start_index, available_pieces, game_tiles, tile_connections, uf_structure
        )
        writer.process_solutions(solution_generator)
    
    # Return the number of solutions found by this worker
    return writer.total_solutions_found

def merge_parquet_files(temp_dir, final_output_path):
    """
    Finds all temporary parquet files, merges them into one, and cleans up.
    """
    print("\nMerging results from all workers...")
    temp_files = glob.glob(os.path.join(temp_dir, "*.parquet"))
    
    if not temp_files:
        print("No temporary files found to merge.")
        return

    # Read all temp files into a list of pandas DataFrames
    df_list = [pd.read_parquet(f) for f in temp_files]
    
    # Concatenate all DataFrames and write to the final output file
    final_df = pd.concat(df_list, ignore_index=True)
    final_df.to_parquet(final_output_path)
    
    # Clean up temporary files
    for f in temp_files:
        os.remove(f)
    os.rmdir(temp_dir)
    print(f"✅ Merged {len(df_list)} files into '{final_output_path}' and cleaned up.")

def main():
    # 1. Define the output directory and get the unique, indexed file path
    OUTPUT_DIR = "generated_solutions"
    CHUNK_SIZE = 100_000
    file_path = get_next_filename(OUTPUT_DIR, base_name="tiling_solutions")
    
    # Load data and perform pre-computation
    with open('tiles/tiles.json', 'r', encoding='utf-8') as file:
        game_tiles = json.load(file)
    tile_connections = generate_tile_connections(game_tiles)

    search_configs = [
        {
            "name": "Piece 0 at top-left corner",
            "start_pos": (0, 0),
            "start_index": 1,
            "candidates": [(0, side, orient) for side in range(2) for orient in range(4)]
        },
        {
            "name": "Piece 0 at top-center edge",
            "start_pos": (0, 1),
            "start_index": 0,
            "candidates": [(0, side, orient) for side in range(2) for orient in range(4)]
        },
        {
            "name": "Piece 0 at board center",
            "start_pos": (1, 1),
            "start_index": 0,
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
                'start_index': config['start_index'],
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
    final_path = get_next_filename("generated_solutions", "tiling_solutions")
    merge_parquet_files(TEMP_DIR, final_path)

    print("\n-------------------------------------------")
    print(f"✅ All tasks complete. Found a total of {total_solutions:,} solutions.")
    print("-------------------------------------------")

if __name__ == "__main__":
    # This guard is CRUCIAL for multiprocessing to work correctly
    main()