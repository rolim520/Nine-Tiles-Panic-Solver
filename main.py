# main.py
import json
from solver import generate_tile_connections, find_valid_tilings_generator
from utils import SolutionWriter, get_next_filename

def main():
    # 1. Define the output directory and get the unique, indexed file path
    OUTPUT_DIR = "generated_solutions"
    CHUNK_SIZE = 100_000
    file_path = get_next_filename(OUTPUT_DIR, base_name="tiling_solutions")
    
    # Load data and perform pre-computation
    with open('tiles/tiles.json', 'r', encoding='utf-8') as file:
        game_tiles = json.load(file)
    tile_connections = generate_tile_connections(game_tiles)

    # 2. Define search configurations
    search_configs = [
        # ... (identical search_configs list)
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

    # 3. Run the solver and write the results to the newly determined path
    print(f"Starting solution generation. Results will be saved to '{file_path}'")
    with SolutionWriter(file_path, CHUNK_SIZE) as writer:
        for config in search_configs:
            print(f"\n--- Starting search: {config['name']} ---")
            for candidate in config['candidates']:
                tiling = [[() for _ in range(3)] for _ in range(3)]
                tiling[config['start_pos'][0]][config['start_pos'][1]] = candidate
                available_pieces = set(range(1, 9))

                # Pass the necessary data to the generator
                solution_generator = find_valid_tilings_generator(
                tiling, config['start_index'], available_pieces, game_tiles, tile_connections
            )
            writer.process_solutions(solution_generator)


if __name__ == "__main__":
    main()