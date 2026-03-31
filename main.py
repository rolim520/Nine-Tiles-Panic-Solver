import json
import os
import time
import multiprocessing
import numpy as np

from solver import find_valid_boards_generator, update_position_domain
from analysis import UnionFind
from utils import SolutionWriter, get_next_filename, merge_parquet_files
from constants import NUM_NODES, TILE_NODES, NORTH, EAST, SOUTH, WEST

# --- Constants for the main script ---
CHUNK_SIZE = 100_000
TEMP_DIR = "temp_solutions"

def generate_tile_connections(game_tiles):
    # Creates a 4D NumPy array with format (piece, side, orientation, connections)
    # E.g.: (9 pieces, 2 sides, 4 orientations, 4 connection points)
    tile_conns_array = np.zeros((9, 2, 4, 4), dtype=np.int8)

    for piece in range(9):
        for side in range(2):
            # 1. Calculates the base connections for orientation 0
            base_connections = np.zeros(4, dtype=np.int8)
            if side < len(game_tiles[piece]) and "roads" in game_tiles[piece][side]:
                for road in game_tiles[piece][side]["roads"]:
                    base_connections[road['connection'][0]] = 1
                    base_connections[road['connection'][1]] = 1
            
            # 2. Uses np.roll to efficiently generate all orientations
            for orientation in range(4):
                tile_conns_array[piece, side, orientation] = np.roll(base_connections, shift=orientation)
    return tile_conns_array

def generate_required_connections_candidates(tile_connections):
    """
    Creates a dictionary mapping a required connections tuple
    to a list of candidate pieces/sides/orientations.
    """

    def connects(required_connections, tile_connections_to_check):
        # Iterates over the 4 sides (0: North, 1: East, 2: South, 3: West)
        for i in range(4):
            if required_connections[i] != -1 and required_connections[i] != tile_connections_to_check[i]:
                return False
        return True

    connections_candidates = {}

    # Iterates over all possible connection combinations (-1, 0, 1)
    for i in range(-1, 2):  # North connection
        for j in range(-1, 2):  # East connection
            for k in range(-1, 2):  # South connection
                for l in range(-1, 2): # West connection

                    required_key = (i, j, k, l)
                    candidates_for_key = []

                    # Finds all pieces that satisfy this requirement
                    for piece in range(9):
                        for side in range(2):
                            for orientation in range(4):
                                current_tile_conns = tile_connections[piece, side, orientation]
                                
                                if connects(required_key, current_tile_conns):
                                    candidates_for_key.append((piece, side, orientation))

                    connections_candidates[required_key] = candidates_for_key

    return connections_candidates

def solve_for_task(task_config):

    task_start_time = time.time()
    process_id = os.getpid() 

    worker_id = task_config['id']
    board_state = task_config['board_state']
    domains = task_config['domains']
    node_states = task_config['node_states']
    available_pieces = task_config['available_pieces']
    uf_structure = task_config['uf_structure']
    game_tiles = task_config['game_tiles']
    tile_connections = task_config['tile_connections']
    connections_candidates = task_config['connections_candidates']

    temp_file_path = os.path.join(TEMP_DIR, f"solutions_{worker_id}.parquet")
    
    with SolutionWriter(temp_file_path, CHUNK_SIZE, silent=True, worker_id=worker_id) as writer:
        solution_generator = find_valid_boards_generator(board_state, node_states, available_pieces, game_tiles, tile_connections, connections_candidates, uf_structure, domains)
        writer.process_solutions(solution_generator, game_tiles)
    
    task_end_time = time.time()
    task_duration = task_end_time - task_start_time
    
    return {
        'worker_id': worker_id,
        'pid': process_id,
        'start_time': task_start_time,
        'end_time': task_end_time,
        'duration': task_duration,
        'solutions_found': writer.total_solutions_found
    }

def main():

    global_start_time = time.time()

    # Load data and perform pre-computation
    with open('game/tiles/tiles.json', 'r', encoding='utf-8') as file:
        game_tiles = json.load(file)

    tile_connections = generate_tile_connections(game_tiles)
    connections_candidates = generate_required_connections_candidates(tile_connections)

    # Configurations for placing the first piece
    search_configs = [
            {
                "name": f"Piece {piece} at board center",
                "start_pos": 4, 
                "candidates": [(piece, 0, 0), (piece, 1, 0)] 
            }
            for piece in range(9)
        ]

    print("Preparing tasks (1 initial piece)...")
    tasks = []
    task_id_counter = 0

    # Generate tasks with a single initial piece placed
    for config in search_configs:
        for candidate in config['candidates']:

            (piece, side, orientation) = candidate

            start_position = config['start_pos']

            board_state = [None] * 9
            board_state[start_position] = candidate

            node_states = [-1] * 24

            candidate_connections = tile_connections[piece][side][orientation]
            node_states[TILE_NODES[start_position][NORTH]] = candidate_connections[NORTH]
            node_states[TILE_NODES[start_position][SOUTH]] = candidate_connections[SOUTH]
            node_states[TILE_NODES[start_position][EAST]] = candidate_connections[EAST]
            node_states[TILE_NODES[start_position][WEST]] = candidate_connections[WEST]

            available_pieces = set(range(9))
            available_pieces.remove(piece)

            domains = [None] * 9
            for position in range(9):
                if position != start_position:
                    domains[position] = update_position_domain(node_states, position, available_pieces, connections_candidates)
                    
            uf = UnionFind(NUM_NODES)
            for road in game_tiles[piece][side]["roads"]:
                l_conn1, l_conn2 = road['connection']
                g_id1 = TILE_NODES[start_position][(l_conn1 + orientation) % 4]
                g_id2 = TILE_NODES[start_position][(l_conn2 + orientation) % 4]
                uf.union(g_id1, g_id2)

            tasks.append({
                'id': task_id_counter,
                'board_state': board_state,
                'domains': domains,
                'node_states': node_states,
                'available_pieces': available_pieces,
                'uf_structure': uf,
                'game_tiles': game_tiles,
                'tile_connections': tile_connections,
                'connections_candidates': connections_candidates,
            })
            task_id_counter += 1

    cpu_count = os.cpu_count()
    print(f"Distributing {len(tasks)} tasks across {cpu_count} CPU cores...")
    os.makedirs(TEMP_DIR, exist_ok=True)
    
    with multiprocessing.Pool(cpu_count) as pool:
        results = pool.map(solve_for_task, tasks)

    total_solutions = sum(r['solutions_found'] for r in results)
    
    final_parquet_path = get_next_filename("generated_solutions", "tiling_solutions")
    merge_parquet_files(TEMP_DIR, final_parquet_path)

    total_duration = time.time() - global_start_time

    print("\n==========================================================================")
    print(" 📊 CORE PERFORMANCE AND SCHEDULING REPORT")
    print("==========================================================================")
    
    # Sorts results by start time (helps visualize chronological order)
    results_sorted = sorted(results, key=lambda x: x['start_time'])
    
    print(f"{'TASK':<8} | {'PID (CORE)':<10} | {'START (s)':<12} | {'DURATION (s)':<12} | {'SOLUTIONS'}")
    print("-" * 74)
    
    chart_data = []

    for r in results_sorted:
        relative_start = r['start_time'] - global_start_time
        
        print(f"Task {r['worker_id']:02d} | PID {r['pid']:<6} | Started at {relative_start:5.2f}s | Duration: {r['duration']:7.2f}s | {r['solutions_found']:,}")
        
        chart_data.append({
            "Task": r['worker_id'],
            "Core_PID": r['pid'],
            "Start": relative_start,
            "Duration": r['duration']
        })

    with open("gantt_chart_data.json", "w") as f:
        json.dump(chart_data, f, indent=4)

    print("\n-------------------------------------------")
    print(f"✅ Execution finished!")
    print(f"🧩 Total solutions found: {total_solutions:,}")
    print(f"⏱️ Absolute Total Time: {total_duration:.2f} seconds ({(total_duration/60):.2f} minutes)")
    print("-------------------------------------------")

if __name__ == "__main__":
    main()