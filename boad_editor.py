import pygame
import os
import sys

# --- Configurações (MODIFICADAS) ---
# Janela mais larga para acomodar a paleta maior
WINDOW_WIDTH = 1170 
WINDOW_HEIGHT = 650
GRID_SIZE = 600
GRID_ORIGIN = (25, 25)
GRID_DIM = 3
TILE_SIZE = GRID_SIZE // GRID_DIM

# Paleta com peças maiores
PALETTE_ORIGIN = (GRID_SIZE + 50, 25)
PALETTE_TILE_SIZE = 150 # Tamanho aumentado para 150px
PALETTE_COLS = 3 # Mantém o layout 3x3

IMAGE_DIR = os.path.join('game', 'tiles', 'tile_images')

# Cores (inalterado)
COLOR_BACKGROUND = (40, 40, 40)
COLOR_GRID_LINES = (80, 80, 80)
COLOR_GRID_BG = (60, 60, 60)
COLOR_PALETTE_BG = (50, 50, 50)
COLOR_TEXT = (220, 220, 220)
COLOR_BUTTON_ACTIVE = (100, 150, 255)
COLOR_POPUP_BG = (20, 20, 20)
COLOR_POPUP_BORDER = (200, 200, 200)

# Timers e Estética (inalterado)
HOVER_DURATION = 200
CLICK_THRESHOLD = 200
GRID_CORNER_RADIUS = 20
PALETTE_CORNER_RADIUS = 15 # Raio um pouco maior para peças maiores

# =============================================================================
# NOVA FUNÇÃO AUXILIAR
# =============================================================================
def apply_rounded_corners(surface, radius):
    """Aplica uma máscara de cantos arredondados a uma superfície do Pygame."""
    mask = pygame.Surface(surface.get_size(), pygame.SRCALPHA)
    pygame.draw.rect(mask, (255, 255, 255, 255), mask.get_rect(), border_radius=radius)
    
    rounded_surface = surface.copy()
    rounded_surface.blit(mask, (0, 0), special_flags=pygame.BLEND_RGBA_MIN)
    return rounded_surface

class InteractiveBoard:
    def __init__(self):
        pygame.init()
        self.screen = pygame.display.set_mode((WINDOW_WIDTH, WINDOW_HEIGHT))
        pygame.display.set_caption("Editor de Tabuleiro - Nine Tiles Panic")
        self.clock = pygame.time.Clock()
        self.font = pygame.font.Font(None, 24)
        self.font_bold = pygame.font.Font(None, 26)

        self.tile_images = self.load_tile_images()
        if not self.tile_images:
            print("ERRO: Não foi possível carregar as imagens.")
            sys.exit()
            
        self.grid_state = [[None for _ in range(GRID_DIM)] for _ in range(GRID_DIM)]
        self.pieces_on_grid = set()
        
        self.active_palette_side = 0
        
        palette_content_width = (PALETTE_TILE_SIZE * PALETTE_COLS) + (10 * (PALETTE_COLS - 1))
        button_width = palette_content_width // 2
        button_height = 40

        self.side_a_button_rect = pygame.Rect(PALETTE_ORIGIN[0], PALETTE_ORIGIN[1], button_width, button_height)
        self.side_b_button_rect = pygame.Rect(PALETTE_ORIGIN[0] + button_width, PALETTE_ORIGIN[1], button_width, button_height)

        # --- MUDANÇA AQUI: Define a área da paleta como um atributo da classe ---
        self.palette_bg_rect = pygame.Rect(PALETTE_ORIGIN[0] - 15, PALETTE_ORIGIN[1] - 15, 
                                     (PALETTE_TILE_SIZE + 10) * PALETTE_COLS + 20, WINDOW_HEIGHT - 20)
        # --------------------------------------------------------------------

        self.palette_tiles = self.setup_palette()

        self.drag_state = {'is_dragging': False, 'data': None, 'image': None, 'offset': (0, 0), 'source_cell': None}
        self.hover_state = {'cell': None, 'start_time': 0, 'show_popup': False}
        self.click_state = {'cell': None, 'start_time': 0}

    # ... (As funções load_tile_images, setup_palette, get_grid_cell_from_mouse, update_pieces_on_grid, handle_events, update e draw permanecem inalteradas) ...
    def load_tile_images(self):
        cache = {}
        try:
            for side in range(2):
                for piece in range(9):
                    path = os.path.join(IMAGE_DIR, f"{side}_{piece}.png")
                    cache[(piece, side)] = pygame.image.load(path).convert_alpha()
        except pygame.error as e: return None
        return cache
    
    def setup_palette(self):
        palette = []
        start_y = self.side_a_button_rect.bottom + 20
        x, y = PALETTE_ORIGIN[0], start_y
        for piece in range(9):
            rect = pygame.Rect(x, y, PALETTE_TILE_SIZE, PALETTE_TILE_SIZE)
            palette.append({'rect': rect, 'piece': piece})
            x += PALETTE_TILE_SIZE + 10
            if (piece + 1) % PALETTE_COLS == 0:
                x = PALETTE_ORIGIN[0]
                y += PALETTE_TILE_SIZE + 10
        return palette

    def get_grid_cell_from_mouse(self, mouse_pos):
        if GRID_ORIGIN[0] < mouse_pos[0] < GRID_ORIGIN[0] + GRID_SIZE and \
           GRID_ORIGIN[1] < mouse_pos[1] < GRID_ORIGIN[1] + GRID_SIZE:
            col = (mouse_pos[0] - GRID_ORIGIN[0]) // TILE_SIZE
            row = (mouse_pos[1] - GRID_ORIGIN[1]) // TILE_SIZE
            return row, col
        return None

    def update_pieces_on_grid(self):
        self.pieces_on_grid = {data[0] for row in self.grid_state for data in row if data is not None}

    def handle_events(self):
        """Processa todos os eventos de input, com lógica de troca e retorno para a paleta."""
        for event in pygame.event.get():
            if event.type == pygame.QUIT or (event.type == pygame.KEYDOWN and event.key == pygame.K_ESCAPE):
                return False

            elif event.type == pygame.MOUSEBUTTONDOWN:
                cell = self.get_grid_cell_from_mouse(event.pos)
                if event.button == 1: # Click Esquerdo
                    # Checa botões da paleta para trocar de lado
                    if self.side_a_button_rect.collidepoint(event.pos): self.active_palette_side = 0
                    elif self.side_b_button_rect.collidepoint(event.pos): self.active_palette_side = 1
                    
                    # Iniciar arrastar da paleta
                    for tile in self.palette_tiles:
                        piece_num = tile['piece']
                        if piece_num not in self.pieces_on_grid and tile['rect'].collidepoint(event.pos):
                            tile_data = (piece_num, self.active_palette_side, 0)
                            self.drag_state.update({'is_dragging': True, 'data': tile_data, 'source_cell': 'palette'})
                            
                            original_img = self.tile_images[(piece_num, self.active_palette_side)]
                            scaled_img = pygame.transform.scale(original_img, (TILE_SIZE, TILE_SIZE))
                            self.drag_state['image'] = apply_rounded_corners(scaled_img, GRID_CORNER_RADIUS)

                            self.drag_state['offset'] = (event.pos[0] - tile['rect'].x, event.pos[1] - tile['rect'].y)
                            break
                    
                    # Iniciar arrastar ou click na grade
                    if not self.drag_state['is_dragging'] and cell and self.grid_state[cell[0]][cell[1]]:
                        self.click_state = {'cell': cell, 'start_time': pygame.time.get_ticks()}
                        piece, side, orient = self.grid_state[cell[0]][cell[1]]
                        self.drag_state.update({'is_dragging': True, 'data': (piece, side, orient), 'source_cell': cell})
                        
                        original_img = self.tile_images[(piece, side)]
                        scaled = pygame.transform.scale(original_img, (TILE_SIZE, TILE_SIZE))
                        rotated = pygame.transform.rotate(scaled, -90 * orient)
                        self.drag_state['image'] = apply_rounded_corners(rotated, GRID_CORNER_RADIUS)

                        cell_rect = pygame.Rect(GRID_ORIGIN[0] + cell[1] * TILE_SIZE, GRID_ORIGIN[1] + cell[0] * TILE_SIZE, TILE_SIZE, TILE_SIZE)
                        self.drag_state['offset'] = (event.pos[0] - cell_rect.x, event.pos[1] - cell_rect.y)
                        self.grid_state[cell[0]][cell[1]] = None # Pega a peça da grade

                elif event.button == 3 and cell and self.grid_state[cell[0]][cell[1]]: # Click Direito para virar
                    piece, side, orient = self.grid_state[cell[0]][cell[1]]
                    self.grid_state[cell[0]][cell[1]] = (piece, 1 - side, orient)

            elif event.type == pygame.MOUSEBUTTONUP and event.button == 1:
                if self.drag_state['is_dragging']:
                    target_cell = self.get_grid_cell_from_mouse(event.pos)
                    source_cell = self.drag_state['source_cell']
                    dragged_data = self.drag_state['data']

                    # Caso 1: A peça foi arrastada DE DENTRO da grade
                    if source_cell != 'palette':
                        # Se soltou em uma célula válida...
                        if target_cell:
                            target_data = self.grid_state[target_cell[0]][target_cell[1]]
                            self.grid_state[target_cell[0]][target_cell[1]] = dragged_data
                            # Se o destino estava ocupado, é uma TROCA.
                            if target_data:
                                self.grid_state[source_cell[0]][source_cell[1]] = target_data
                        # Se soltou fora da grade, verifica se foi na paleta para REMOVER
                        else:
                            if self.palette_bg_rect.collidepoint(event.pos):
                                # Soltou na paleta, a peça é removida (não a colocamos de volta)
                                pass 
                            else:
                                # Soltou em qualquer outro lugar, cancela e retorna a peça à origem
                                self.grid_state[source_cell[0]][source_cell[1]] = dragged_data
                    
                    # Caso 2: A peça foi arrastada DA PALETA
                    else:
                        if target_cell:
                            self.grid_state[target_cell[0]][target_cell[1]] = dragged_data
                    
                    self.drag_state['is_dragging'] = False
                    self.update_pieces_on_grid()

                # Lógica de click curto para rotação
                if self.click_state['cell']:
                    duration = pygame.time.get_ticks() - self.click_state['start_time']
                    if not self.drag_state['is_dragging'] and duration < CLICK_THRESHOLD:
                         r, c = self.click_state['cell']
                         if self.grid_state[r][c]:
                            piece, side, orient = self.grid_state[r][c]
                            self.grid_state[r][c] = (piece, side, (orient + 1) % 4)
                    self.click_state['cell'] = None
            
            elif event.type == pygame.MOUSEMOTION:
                cell = self.get_grid_cell_from_mouse(event.pos)
                if not self.drag_state['is_dragging'] and cell and self.grid_state[cell[0]][cell[1]]:
                    if self.hover_state['cell'] != cell:
                        self.hover_state.update({'cell': cell, 'start_time': pygame.time.get_ticks(), 'show_popup': False})
                else:
                    self.hover_state.update({'cell': None, 'show_popup': False})
        
        return True

    def update(self):
        if self.hover_state['cell'] and not self.hover_state['show_popup']:
            if pygame.time.get_ticks() - self.hover_state['start_time'] > HOVER_DURATION:
                self.hover_state['show_popup'] = True
    
    def draw(self):
        self.screen.fill(COLOR_BACKGROUND)
        self.draw_palette()
        self.draw_grid()
        self.draw_hover_popup()
        self.draw_dragged_tile()
        pygame.display.flip()

    def draw_grid(self):
        grid_surface = pygame.Surface((GRID_SIZE, GRID_SIZE), pygame.SRCALPHA)
        grid_surface.fill(COLOR_GRID_BG)
        for r in range(GRID_DIM):
            for c in range(GRID_DIM):
                tile_data = self.grid_state[r][c]
                if tile_data:
                    piece, side, orient = tile_data
                    img = self.tile_images.get((piece, side))
                    if img:
                        scaled_img = pygame.transform.scale(img, (TILE_SIZE, TILE_SIZE))
                        rotated_img = pygame.transform.rotate(scaled_img, -90 * orient)
                        rect = rotated_img.get_rect(center=(c * TILE_SIZE + TILE_SIZE // 2, r * TILE_SIZE + TILE_SIZE // 2))
                        grid_surface.blit(rotated_img, rect.topleft)
        mask = pygame.Surface((GRID_SIZE, GRID_SIZE), pygame.SRCALPHA)
        pygame.draw.rect(mask, (255, 255, 255, 255), (0, 0, GRID_SIZE, GRID_SIZE), border_radius=GRID_CORNER_RADIUS)
        grid_surface.blit(mask, (0, 0), special_flags=pygame.BLEND_RGBA_MULT)
        for i in range(1, GRID_DIM):
            pygame.draw.line(grid_surface, COLOR_GRID_LINES, (0, i * TILE_SIZE), (GRID_SIZE, i * TILE_SIZE), 3)
            pygame.draw.line(grid_surface, COLOR_GRID_LINES, (i * TILE_SIZE, 0), (i * TILE_SIZE, GRID_SIZE), 3)
        pygame.draw.rect(grid_surface, COLOR_GRID_LINES, (0, 0, GRID_SIZE, GRID_SIZE), 3, border_radius=GRID_CORNER_RADIUS)
        self.screen.blit(grid_surface, GRID_ORIGIN)

    def draw_palette(self):
        """Função atualizada para desenhar peças da paleta com cantos arredondados."""
        palette_bg_rect = pygame.Rect(PALETTE_ORIGIN[0] - 15, PALETTE_ORIGIN[1] - 15, 
                                     (PALETTE_TILE_SIZE + 10) * PALETTE_COLS + 20, WINDOW_HEIGHT - 20)
        pygame.draw.rect(self.screen, COLOR_PALETTE_BG, palette_bg_rect, border_radius=GRID_CORNER_RADIUS)
        
        pygame.draw.rect(self.screen, COLOR_BUTTON_ACTIVE if self.active_palette_side == 0 else COLOR_GRID_BG, self.side_a_button_rect, border_radius=5)
        pygame.draw.rect(self.screen, COLOR_BUTTON_ACTIVE if self.active_palette_side == 1 else COLOR_GRID_BG, self.side_b_button_rect, border_radius=5)
        
        label_a = self.font_bold.render("Lado A", True, COLOR_TEXT)
        self.screen.blit(label_a, label_a.get_rect(center=self.side_a_button_rect.center))
        label_b = self.font_bold.render("Lado B", True, COLOR_TEXT)
        self.screen.blit(label_b, label_b.get_rect(center=self.side_b_button_rect.center))

        for tile in self.palette_tiles:
            piece_num = tile['piece']
            if piece_num not in self.pieces_on_grid:
                img = self.tile_images.get((piece_num, self.active_palette_side))
                if img:
                    scaled_img = pygame.transform.scale(img, (PALETTE_TILE_SIZE, PALETTE_TILE_SIZE))
                    
                    # --- MUDANÇA AQUI: Aplica cantos arredondados ---
                    rounded_img = apply_rounded_corners(scaled_img, PALETTE_CORNER_RADIUS)
                    self.screen.blit(rounded_img, tile['rect'].topleft)
                    # -----------------------------------------------

    def draw_dragged_tile(self):
        if self.drag_state['is_dragging']:
            mouse_pos = pygame.mouse.get_pos()
            top_left_pos = (mouse_pos[0] - self.drag_state['offset'][0], 
                            mouse_pos[1] - self.drag_state['offset'][1])
            self.screen.blit(self.drag_state['image'], top_left_pos)

    def draw_hover_popup(self):
        if self.hover_state['show_popup'] and self.hover_state['cell']:
            r, c = self.hover_state['cell']
            tile_data = self.grid_state[r][c]
            if tile_data:
                piece, side, orient = tile_data
                popup_img_original = self.tile_images.get((piece, 1 - side))
                if popup_img_original:
                    scaled_img = pygame.transform.scale(popup_img_original, (TILE_SIZE, TILE_SIZE))
                    rotated_popup = pygame.transform.rotate(scaled_img, -90 * orient)
                    mouse_pos = pygame.mouse.get_pos()
                    popup_rect = rotated_popup.get_rect(topleft=(mouse_pos[0] + 20, mouse_pos[1]))
                    bg_rect = popup_rect.inflate(10, 10)
                    pygame.draw.rect(self.screen, COLOR_POPUP_BG, bg_rect, border_radius=5)
                    pygame.draw.rect(self.screen, COLOR_POPUP_BORDER, bg_rect, 2, 5)
                    self.screen.blit(rotated_popup, popup_rect)

    def run(self):
        running = True
        while running:
            running = self.handle_events()
            self.update()
            self.draw()
            self.clock.tick(60)
        pygame.quit()

if __name__ == "__main__":
    editor = InteractiveBoard()
    editor.run()