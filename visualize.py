import duckdb
import pygame
import os
import sys
import re

# --- Configuração ---
IMAGE_DIR = os.path.join('game', 'tiles', 'tile_images')
SOLUTION_DIR = 'generated_solutions'
# Janela mais larga para acomodar o painel de estatísticas
WINDOW_WIDTH = 1200
WINDOW_HEIGHT = 800
GRID_DIM = 3

# --- Configuração do Painel de Estatísticas ---
STATS_PANEL_WIDTH = 400
STATS_BG_COLOR = (30, 30, 30)
STATS_FONT_COLOR = (220, 220, 220)
STATS_FONT_SIZE = 22
STATS_HEADER_FONT_SIZE = 28

# --- Configuração da Grade de Visualização ---
GRID_SIZE = WINDOW_HEIGHT # A grade será um quadrado de 800x800
GRID_LINE_WIDTH = 5
GRID_LINE_COLOR = (0, 0, 0) # Preto

def find_latest_solution_file(directory, base_name="tiling_solutions", extension="duckdb"):
    """
    Encontra o arquivo de solução com o maior índice em um diretório.
    Agora procura por arquivos .duckdb por padrão.
    """
    if not os.path.isdir(directory):
        return None, f"Diretório de soluções '{directory}' não encontrado."

    pattern = re.compile(rf"{base_name}_(\d+)\.{extension}")
    
    highest_index = -1
    latest_file_path = None
    
    for filename in os.listdir(directory):
        match = pattern.match(filename)
        if match:
            index = int(match.group(1))
            if index > highest_index:
                highest_index = index
                latest_file_path = os.path.join(directory, filename)
    
    if latest_file_path:
        return latest_file_path, None
    else:
        return None, f"Nenhum arquivo de solução (ex: 'tiling_solutions_1.duckdb') encontrado em '{directory}'."

def preload_images(tile_size):
    """
    Carrega e escala todas as 18 imagens de peças em um dicionário (cache).
    """
    image_cache = {}
    print("Pré-carregando e escalando imagens...")
    for side in range(2):
        for piece in range(9):
            image_filename = f"{side}_{piece}.png"
            image_path = os.path.join(IMAGE_DIR, image_filename)
            try:
                tile_image = pygame.image.load(image_path)
                scaled_tile = pygame.transform.scale(tile_image, (tile_size, tile_size))
                image_cache[(side, piece)] = scaled_tile
            except pygame.error:
                print(f"⚠️ Aviso: Não foi possível carregar a imagem '{image_path}'.")
                image_cache[(side, piece)] = None
    print("✅ Cache de imagens criado.")
    return image_cache

def draw_stats(screen, solution_data, font, header_font):
    """
    Desenha o painel lateral com todas as estatísticas da solução.
    """
    # Cria uma superfície para o painel
    stats_panel = pygame.Surface((STATS_PANEL_WIDTH, WINDOW_HEIGHT))
    stats_panel.fill(STATS_BG_COLOR)
    
    # Desenha o cabeçalho
    header_text = header_font.render("Estatísticas da Solução", True, (255, 255, 255))
    stats_panel.blit(header_text, (20, 20))

    # Filtra para obter apenas as colunas de estatísticas (não as de layout)
    # CORREÇÃO AQUI: 'orientation_{r}{c}' mudou para 'orient_{r}{c}'
    excluded_cols = [f'piece_{r}{c}' for r in range(3) for c in range(3)] + \
                    [f'side_{r}{c}' for r in range(3) for c in range(3)] + \
                    [f'orient_{r}{c}' for r in range(3) for c in range(3)]
    
    y_offset = 70
    for col_name, value in solution_data.items():
        if col_name not in excluded_cols:
            # Formata o texto
            stat_text = f"{col_name}: {value}"
            text_surface = font.render(stat_text, True, STATS_FONT_COLOR)
            stats_panel.blit(text_surface, (20, y_offset))
            y_offset += 30 # Espaçamento entre as linhas

    # Desenha o painel na tela principal
    screen.blit(stats_panel, (GRID_SIZE, 0))

def draw_solution(screen, db_connection, solution_index, image_cache, fonts):
    """
    Busca uma solução do banco de dados e a desenha, incluindo suas estatísticas.
    """
    num_solutions = db_connection.execute('SELECT COUNT(*) FROM solutions').fetchone()[0]
    solution_index = max(0, min(solution_index, num_solutions - 1))

    query = f"SELECT * FROM solutions LIMIT 1 OFFSET {solution_index}"
    solution_df = db_connection.execute(query).fetchdf()
    
    if solution_df.empty:
        return

    solution_row = solution_df.iloc[0]

    pygame.display.set_caption(f"Solução #{solution_index} de {num_solutions - 1}")
    screen.fill((20, 20, 20)) 
    tile_size = GRID_SIZE // GRID_DIM

    for r in range(GRID_DIM):
        for c in range(GRID_DIM):
            piece = solution_row[f'piece_{r}{c}']
            side = solution_row[f'side_{r}{c}']
            # CORREÇÃO AQUI: 'orientation_{r}{c}' mudou para 'orient_{r}{c}'
            orientation = solution_row[f'orient_{r}{c}']
            scaled_tile = image_cache.get((side, piece))

            if scaled_tile:
                rotation_angle = -(orientation * 90)
                rotated_tile = pygame.transform.rotate(scaled_tile, rotation_angle)
                screen.blit(rotated_tile, (c * tile_size, r * tile_size))

    for i in range(1, GRID_DIM):
        pygame.draw.line(screen, GRID_LINE_COLOR, (i * tile_size, 0), (i * tile_size, GRID_SIZE), GRID_LINE_WIDTH)
        pygame.draw.line(screen, GRID_LINE_COLOR, (0, i * tile_size), (GRID_SIZE, i * tile_size), GRID_LINE_WIDTH)
    pygame.draw.rect(screen, GRID_LINE_COLOR, (0, 0, GRID_SIZE, GRID_SIZE), GRID_LINE_WIDTH)

    draw_stats(screen, solution_row, fonts['stats'], fonts['header'])

    pygame.display.flip()

def main(initial_index):
    db_file_path, error = find_latest_solution_file(SOLUTION_DIR)
    
    if error:
        print(f"❌ Erro: {error}")
        return

    try:
        db_con = duckdb.connect(database=db_file_path, read_only=True)
        num_solutions = db_con.execute('SELECT COUNT(*) FROM solutions').fetchone()[0]
        print(f"✅ Conectado a '{db_file_path}' com {num_solutions:,} soluções.")
    except Exception as e:
        print(f"❌ Erro: Não foi possível abrir o banco de dados '{db_file_path}'. Detalhes: {e}")
        return

    pygame.init()
    screen = pygame.display.set_mode((WINDOW_WIDTH, WINDOW_HEIGHT))
    clock = pygame.time.Clock()
    
    try:
        stats_font = pygame.font.Font(None, STATS_FONT_SIZE)
        header_font = pygame.font.Font(None, STATS_HEADER_FONT_SIZE)
        fonts = {'stats': stats_font, 'header': header_font}
    except Exception as e:
        print(f"Erro ao carregar fontes: {e}. Usando fonte padrão.")
        stats_font = pygame.font.SysFont(None, STATS_FONT_SIZE)
        header_font = pygame.font.SysFont(None, STATS_HEADER_FONT_SIZE)
        fonts = {'stats': stats_font, 'header': header_font}

    tile_size = GRID_SIZE // GRID_DIM
    image_cache = preload_images(tile_size)

    current_index = initial_index
    draw_solution(screen, db_con, current_index, image_cache, fonts)

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
                    draw_solution(screen, db_con, current_index, image_cache, fonts)
                    scroll_timer = pygame.time.get_ticks() + scroll_delay
        
        keys = pygame.key.get_pressed()
        now = pygame.time.get_ticks()
        
        direction = 0
        if keys[pygame.K_RIGHT]: direction = 1
        if keys[pygame.K_LEFT]:  direction = -1

        if direction != 0 and now > scroll_timer:
            current_index = (current_index + direction + num_solutions) % num_solutions
            draw_solution(screen, db_con, current_index, image_cache, fonts)
            scroll_timer = now + scroll_interval

    db_con.close()
    pygame.quit()
    print("Visualizador fechado.")

if __name__ == "__main__":
    start_index = 0
    if len(sys.argv) > 1:
        try:
            start_index = int(sys.argv[1])
        except ValueError:
            print("Uso: python visualize.py <índice_inicial>")
            sys.exit(1)
    main(start_index)