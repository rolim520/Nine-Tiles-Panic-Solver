import pandas as pd
import pyarrow.parquet as pq
import pygame
import os
import sys

# --- Configuration ---
IMAGE_DIR = os.path.join('tiles', 'piece_images')
PARQUET_FILE = 'tiling_solutions.parquet'
WINDOW_SIZE = 800
GRID_DIM = 3
GRID_LINE_WIDTH = 5
GRID_LINE_COLOR = (0, 0, 0) # Black

def preload_images(tile_size):
    """
    Loads and scales all 18 base tile images into a dictionary (cache) once.
    This avoids slow disk access during the main loop.
    """
    image_cache = {}
    print("Pre-loading and scaling images...")
    for side in range(2):
        for piece in range(9):
            image_filename = f"{side}_{piece}.png"
            image_path = os.path.join(IMAGE_DIR, image_filename)
            try:
                # Load the original image
                tile_image = pygame.image.load(image_path)
                # Scale it to the final display size and store in the cache
                scaled_tile = pygame.transform.scale(tile_image, (tile_size, tile_size))
                image_cache[(side, piece)] = scaled_tile
            except pygame.error:
                print(f"⚠️ Warning: Could not load image '{image_path}'. It will be missing.")
                image_cache[(side, piece)] = None
    print("✅ Image cache created.")
    return image_cache

def draw_solution(screen, pq_file, solution_index, image_cache):
    """
    Draws a solution using the pre-loaded image cache for maximum speed.
    """
    num_solutions = pq_file.metadata.num_rows
    solution_index = max(0, min(solution_index, num_solutions - 1))

    solution_row = None
    rows_processed = 0
    for i in range(pq_file.num_row_groups):
        row_group_meta = pq_file.metadata.row_group(i)
        if rows_processed <= solution_index < rows_processed + row_group_meta.num_rows:
            df_chunk = pq_file.read_row_group(i).to_pandas()
            index_in_chunk = solution_index - rows_processed
            solution_row = df_chunk.iloc[index_in_chunk]
            break
        rows_processed += row_group_meta.num_rows

    if solution_row is None:
        return

    pygame.display.set_caption(f"Solution #{solution_index} of {num_solutions - 1}")
    screen.fill((20, 20, 20))
    tile_size = WINDOW_SIZE // GRID_DIM

    for r in range(GRID_DIM):
        for c in range(GRID_DIM):
            piece, side, orientation = solution_row[f'piece_{r}{c}'], solution_row[f'side_{r}{c}'], solution_row[f'orient_{r}{c}']
            scaled_tile = image_cache.get((side, piece))

            if scaled_tile:
                rotation_angle = -(orientation * 90)
                rotated_tile = pygame.transform.rotate(scaled_tile, rotation_angle)
                screen.blit(rotated_tile, (c * tile_size, r * tile_size))
            else:
                placeholder = pygame.Rect(c * tile_size, r * tile_size, tile_size, tile_size)
                pygame.draw.rect(screen, (255, 0, 0), placeholder, 2)

    # --- Draw Internal Grid Lines ---
    for i in range(1, GRID_DIM):
        pygame.draw.line(screen, GRID_LINE_COLOR, (i * tile_size, 0), (i * tile_size, WINDOW_SIZE), GRID_LINE_WIDTH)
        pygame.draw.line(screen, GRID_LINE_COLOR, (0, i * tile_size), (WINDOW_SIZE, i * tile_size), GRID_LINE_WIDTH)
    
    # --- NEW: Draw External Border ---
    pygame.draw.rect(screen, GRID_LINE_COLOR, (0, 0, WINDOW_SIZE, WINDOW_SIZE), GRID_LINE_WIDTH)

    pygame.display.flip()

def main(initial_index):
    try:
        pq_file = pq.ParquetFile(PARQUET_FILE)
        num_solutions = pq_file.metadata.num_rows
        print(f"✅ Loaded '{PARQUET_FILE}' with {num_solutions:,} solutions.")
    except Exception as e:
        print(f"❌ Error: Could not open '{PARQUET_FILE}'. Details: {e}")
        return

    pygame.init()
    screen = pygame.display.set_mode((WINDOW_SIZE, WINDOW_SIZE))
    clock = pygame.time.Clock()
    
    tile_size = WINDOW_SIZE // GRID_DIM
    image_cache = preload_images(tile_size)

    current_index = initial_index
    draw_solution(screen, pq_file, current_index, image_cache)

    scroll_delay = 250
    scroll_interval = 1 
    scroll_timer = 0
    
    running = True
    while running:
        clock.tick(60)

        for event in pygame.event.get():
            if event.type == pygame.QUIT or (event.type == pygame.KEYDOWN and event.key == pygame.K_ESCAPE):
                running = False
            if event.type == pygame.KEYDOWN:
                direction = 0
                if event.key == pygame.K_RIGHT: direction = 1
                if event.key == pygame.K_LEFT:  direction = -1
                if direction != 0:
                    current_index = (current_index + direction + num_solutions) % num_solutions
                    draw_solution(screen, pq_file, current_index, image_cache)
                    scroll_timer = pygame.time.get_ticks() + scroll_delay
        
        keys = pygame.key.get_pressed()
        now = pygame.time.get_ticks()
        
        direction = 0
        if keys[pygame.K_RIGHT]: direction = 1
        if keys[pygame.K_LEFT]:  direction = -1

        if direction != 0 and now > scroll_timer:
            current_index = (current_index + direction + num_solutions) % num_solutions
            draw_solution(screen, pq_file, current_index, image_cache)
            scroll_timer = now + scroll_interval

    pygame.quit()
    print("Visualizer closed.")

if __name__ == "__main__":
    start_index = 0
    if len(sys.argv) > 1:
        try:
            start_index = int(sys.argv[1])
        except ValueError:
            print("Usage: python visualize.py <start_index>")
            sys.exit(1)
    main(start_index)