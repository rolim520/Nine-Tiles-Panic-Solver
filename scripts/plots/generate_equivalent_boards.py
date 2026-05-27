import os
import duckdb
import pandas as pd
from PIL import Image

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
PARQUET_PATH = 'generated_solutions/*.parquet' 
TILE_IMAGES_DIR = 'docs/assets/tile_images'
OUTPUT_DIR = 'images/tabuleiros_equivalentes'

TARGET_ROADS = 3
TARGET_HOUSES = 2
TARGET_DOGS = 4
NUM_TABULEIROS_GERADOS = 50 

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================
def load_tile_images(images_dir):
    print("Carregando imagens das peças...")
    images_cache = {}
    for face in [0, 1]:
        for piece in range(9):
            filename = f"{face}_{piece}.webp" 
            filepath = os.path.join(images_dir, filename)
            if os.path.exists(filepath):
                images_cache[filename] = Image.open(filepath).convert("RGBA")
            else:
                print(f"⚠️ Aviso: Imagem não encontrada: {filepath}")
    return images_cache

def generate_board_image(row_data, images_cache, output_path):
    sample_img = next(iter(images_cache.values()))
    tile_w, tile_h = sample_img.size
    
    board_img = Image.new('RGBA', (tile_w * 3, tile_h * 3), (255, 255, 255, 255))
    
    for r in range(3):
        for c in range(3):
            piece = int(row_data[f'piece_{r}{c}'])
            side = int(row_data[f'side_{r}{c}'])
            orient = int(row_data[f'orient_{r}{c}'])
            
            img_key = f"{side}_{piece}.webp"
            tile_img = images_cache.get(img_key)
            
            if tile_img:
                rotated_tile = tile_img.rotate(-90 * orient, expand=False)
                x_offset = c * tile_w
                y_offset = r * tile_h
                board_img.paste(rotated_tile, (x_offset, y_offset), rotated_tile)
                
    if output_path.lower().endswith('.pdf'):
        board_img.convert('RGB').save(output_path, "PDF")
    else:
        board_img.save(output_path) 

# =============================================================================
# MOTOR PRINCIPAL
# =============================================================================
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print(f"🔍 Buscando tabuleiros com: Roads={TARGET_ROADS}, Houses={TARGET_HOUSES}, Dogs={TARGET_DOGS}...")
    
    layout_cols = ", ".join([f"piece_{r}{c}, side_{r}{c}, orient_{r}{c}" for r in range(3) for c in range(3)])
    
    query = f"""
        SELECT {layout_cols}
        FROM read_parquet('{PARQUET_PATH}')
        WHERE total_roads = {TARGET_ROADS}
          AND max_hamburgers_in_front_of_alien = {TARGET_HOUSES}
          AND largest_dog_group = {TARGET_DOGS}
        ORDER BY random()
        LIMIT {NUM_TABULEIROS_GERADOS}
    """
    
    try:
        con = duckdb.connect()
        df_boards = con.execute(query).fetchdf()
        con.close()
    except Exception as e:
        print(f"❌ Erro ao ler os dados: {e}")
        return

    num_found = len(df_boards)
    if num_found == 0:
        print("❌ Nenhum tabuleiro encontrado com essas métricas exatas.")
        return
        
    print(f"✅ Sorteados {num_found} tabuleiros distintos! Gerando imagens...")
    
    images_cache = load_tile_images(TILE_IMAGES_DIR)
    
    for index, row in df_boards.iterrows():
        out_filename = os.path.join(OUTPUT_DIR, f"equivalente_{index + 1:02d}.pdf")
        generate_board_image(row, images_cache, out_filename)
    
    print(f"\n🎉 Concluído! Verifique a pasta '{OUTPUT_DIR}' e escolha seus favoritos!")

if __name__ == "__main__":
    main()