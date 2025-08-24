import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

def solution_to_flat_dict(solution):
    """Converts a 3x3 grid solution into a flat dictionary for a DataFrame."""
    flat_data = {}
    for r, row_data in enumerate(solution):
        for c, tile_data in enumerate(row_data):
            # tile_data is a tuple (piece, side, orientation)
            flat_data[f'piece_{r}{c}'] = tile_data[0]
            flat_data[f'side_{r}{c}'] = tile_data[1]
            flat_data[f'orient_{r}{c}'] = tile_data[2]
    return flat_data

with open('Tiles/tiles.json', 'r', encoding='utf-8') as file:
    # guarda as tiles do jogo na lista game_tiles
    game_tiles = json.load(file)

def connects(required_connections, tile_connections):
    for i in range(4):
        if required_connections[i] != -1 and required_connections[i] != tile_connections[i]:
            return False
    return True

def generate_tile_connections():
    tile_connections = dict()
    connections = [0, 0, 0, 0]
    for piece in range(9):
        for side in range(2):
            for orientation in range(4):
                connections = [0, 0, 0, 0]
                for road in game_tiles[piece][side]["roads"]:
                    connections[(road['connection'][0]+orientation)%4] = 1
                    connections[(road['connection'][1]+orientation)%4] = 1
                tile_connections[(piece, side, orientation)] = connections
    return tile_connections

def find_candidate_tiles(tiling, position, available_pieces):
    row, col = position // 3, position % 3

    # Obtém as conexões que a nova tile deve ter
    required_connections = [-1, -1, -1, -1]
    if row-1 >= 0 and tiling[row-1][col] != ():
        if tile_connections[tiling[row-1][col]][3] == 1:
            required_connections[1] = 1
        else:
            required_connections[1] = 0
    if row+1 <= 2 and tiling[row+1][col] != ():
        if tile_connections[tiling[row+1][col]][1] == 1:
            required_connections[3] = 1
        else:
            required_connections[3] = 0
    if col-1 >= 0 and tiling[row][col-1] != ():
        if tile_connections[tiling[row][col-1]][2] == 1:
            required_connections[0] = 1
        else:
            required_connections[0] = 0
    if col+1 <= 2 and tiling[row][col+1] != ():
        if tile_connections[tiling[row][col+1]][0] == 1:
            required_connections[2] = 1
        else:
            required_connections[2] = 0
    
    # Encontra as candidatas com as conexões encontradas
    candidates = []
    for piece in available_pieces:
        for side in range(2):
            for orientation in range(4):
                if connects(required_connections, tile_connections[(piece, side, orientation)]):
                    candidates.append((piece, side, orientation))
    return candidates

def find_valid_tilings_generator(tiling, position, available_pieces):
    # Base case: A solution is found, so yield a copy of it.
    if position == 9:
        yield [row[:] for row in tiling]
        return # Stop this path

    # Convert linear position to grid coordinates
    row, col = position // 3, position % 3
    candidates = find_candidate_tiles(tiling, position, available_pieces)

    # Explore each candidate
    for candidate in candidates:
        tiling[row][col] = candidate
        available_pieces.remove(candidate[0])
        
        # Yield the solutions found from the deeper recursive calls
        yield from find_valid_tilings_generator(tiling, position + 1, available_pieces)
        
        # Backtrack
        available_pieces.add(candidate[0])
        tiling[row][col] = ()


# --- Main Execution ---

# 1. Setup global variables and constants
tile_connections = generate_tile_connections()
initial_candidates = [(0, i, j) for i in range(2) for j in range(4)]
CHUNK_SIZE = 100_000
FILE_PATH = 'tiling_solutions.parquet'

# 2. Prepare for file writing
solutions_chunk = []
total_solutions_found = 0
writer = None

# Remove old file if it exists to ensure a fresh start
if os.path.exists(FILE_PATH):
    os.remove(FILE_PATH)

print(f"Starting solution generation. Results will be saved to '{FILE_PATH}'")

# 3. Main loop to generate and save solutions
try:
    for candidate in initial_candidates:
        tiling = [[() for _ in range(3)] for _ in range(3)]
        tiling[0][0] = candidate
        available_pieces = set(range(1, 9))

        # The generator produces solutions one by one, keeping memory low
        solution_generator = find_valid_tilings_generator(tiling, 1, available_pieces)

        for solution in solution_generator:
            solutions_chunk.append(solution_to_flat_dict(solution))
            total_solutions_found += 1

            # When the chunk is full, write it to the Parquet file
            if len(solutions_chunk) >= CHUNK_SIZE:
                table = pa.Table.from_pylist(solutions_chunk)
                if writer is None:
                    # Create the Parquet file writer with the first chunk
                    writer = pq.ParquetWriter(FILE_PATH, table.schema)
                writer.write_table(table)
                solutions_chunk = [] # Reset the chunk
                print(f" ... Wrote {total_solutions_found} solutions so far")

finally:
    # 4. Write any remaining solutions in the final chunk
    if solutions_chunk:
        table = pa.Table.from_pylist(solutions_chunk)
        if writer is None:
            writer = pq.ParquetWriter(FILE_PATH, table.schema)
        writer.write_table(table)

    # 5. Close the file writer
    if writer:
        writer.close()

print("\n-------------------------------------------")
print(f"✅ Finished! Found and saved a total of {total_solutions_found} solutions.")
print("-------------------------------------------")