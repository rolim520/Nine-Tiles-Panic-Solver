import pygame
import os
import sys
import json
import textwrap

# --- Configurações (MODIFICADAS) ---
# Janela e Layout
WINDOW_WIDTH = 1400
WINDOW_HEIGHT = 1000
REGION_SPACING = 20 # Distância entre os elementos principais
BG_PADDING = 15 # Preenchimento para os painéis de fundo

# Paleta (Apenas dimensões, a posição agora é dinâmica)
PALETTE_TILE_SIZE = 120
PALETTE_PADDING = 30
PALETTE_WIDTH = 9 * PALETTE_TILE_SIZE + 8 * PALETTE_PADDING
PALETTE_HEIGHT = 2 * PALETTE_TILE_SIZE + 1 * PALETTE_PADDING

GRID_DIM = 3

# Caminhos
TILES_DIR = os.path.join('game', 'tiles', 'tile_images')
CARDS_DIR = os.path.join('game', 'cards', 'card_images')
CARDS_JSON_PATH = os.path.join('game', 'cards', 'cards.json')

# Cores
COLOR_BACKGROUND = (40, 40, 40)
COLOR_GRID_LINES = (80, 80, 80)
COLOR_GRID_BG = (60, 60, 60)
COLOR_PALETTE_BG = (50, 50, 50)
COLOR_TEXT = (220, 220, 220)
COLOR_POPUP_BG = (30, 30, 30)
COLOR_POPUP_BORDER = (200, 200, 200)
COLOR_ARROW = (150, 150, 150)
COLOR_ARROW_HOVER = (220, 220, 220)

# Timers e Estética
HOVER_DURATION = 300 
CLICK_THRESHOLD = 200
GRID_CORNER_RADIUS = 20
PALETTE_CORNER_RADIUS = 15
CARD_CORNER_RADIUS = 10 

# =============================================================================
# FUNÇÃO AUXILIAR
# =============================================================================
def apply_rounded_corners(surface, radius):
    """Aplica uma máscara de cantos arredondados a uma superfície do Pygame."""
    rounded_surface = pygame.Surface(surface.get_size(), pygame.SRCALPHA)
    pygame.draw.rect(rounded_surface, (255, 255, 255, 255), rounded_surface.get_rect(), border_radius=radius)
    
    rounded_surface.blit(surface, (0, 0), special_flags=pygame.BLEND_RGBA_MIN)
    return rounded_surface

class InteractiveBoard:
    def __init__(self):
        pygame.init()
        self.screen = pygame.display.set_mode((WINDOW_WIDTH, WINDOW_HEIGHT))
        pygame.display.set_caption("Editor de Tabuleiro - Nine Tiles Panic")
        self.clock = pygame.time.Clock()
        
        self.font = pygame.font.Font(None, 24)
        self.font_card_title = pygame.font.Font(None, 28)
        self.font_card_desc = pygame.font.Font(None, 22)
        
        self._setup_dynamic_layout()

        self.tile_images = self.load_tile_images()
        if not self.tile_images:
            print("ERRO: Não foi possível carregar as imagens das peças.")
            sys.exit()
            
        self.card_images = self.load_card_images()
        self.card_data = self.load_card_data()
        if not self.card_images or not self.card_data:
            print("AVISO: Não foi possível carregar as cartas ou seus dados. A área de cartas será desativada.")

        self.grid_state = [[None for _ in range(GRID_DIM)] for _ in range(GRID_DIM)]
        self.pieces_on_grid = set()
        
        self.palette_tiles = self.setup_palette()
        self.card_slots = self.setup_card_slots()
        
        self._cache_images()
        
        self.drag_state = {'is_dragging': False, 'data': None, 'image': None, 'offset': (0, 0), 'source_cell': None}
        self.hover_state = {'cell': None, 'card_slot_index': None, 'start_time': 0, 'show_popup': False}
        self.click_state = {'cell': None, 'start_time': 0}

    def _setup_dynamic_layout(self):
        """Calcula o tamanho e a posição de todos os elementos da UI dinamicamente."""
        palette_bg_x = (WINDOW_WIDTH - (PALETTE_WIDTH + 2 * BG_PADDING)) // 2
        palette_bg_y = REGION_SPACING
        self.palette_bg_rect = pygame.Rect(
            palette_bg_x, palette_bg_y,
            PALETTE_WIDTH + 2 * BG_PADDING, PALETTE_HEIGHT + 2 * BG_PADDING
        )
        self.palette_origin = (palette_bg_x + BG_PADDING, palette_bg_y + BG_PADDING)

        grid_origin_y = self.palette_bg_rect.bottom + REGION_SPACING
        self.bottom_area_height = WINDOW_HEIGHT - grid_origin_y - REGION_SPACING
        
        self.grid_size = self.bottom_area_height
        self.grid_origin = (REGION_SPACING, grid_origin_y)
        self.tile_size = self.grid_size // GRID_DIM

        card_bg_origin_x = self.grid_origin[0] + self.grid_size + REGION_SPACING
        card_bg_origin_y = self.grid_origin[1]
        
        self.arrow_width = 30
        self.arrow_height = 40
        card_padding = 20

        card_area_inner_height = self.bottom_area_height - (2 * BG_PADDING)
        self.card_height = (card_area_inner_height - (2 * card_padding)) / 3
        self.card_width = int(self.card_height * 1.729)
        self.card_padding = card_padding

        card_img_origin_x = card_bg_origin_x + BG_PADDING + self.arrow_width + 5
        
        card_area_total_width = self.card_width + 2 * (self.arrow_width + 5)
        self.card_area_bg_rect = pygame.Rect(
            card_bg_origin_x,
            card_bg_origin_y,
            card_area_total_width + 2 * BG_PADDING,
            self.bottom_area_height
        )
        
        self.card_slots_positions = []
        for i in range(3):
            y_pos = (card_bg_origin_y + BG_PADDING) + i * (self.card_height + self.card_padding)
            card_rect = pygame.Rect(card_img_origin_x, y_pos, self.card_width, self.card_height)
            arrow_y = y_pos + (self.card_height - self.arrow_height) // 2
            left_arrow_rect = pygame.Rect(card_img_origin_x - self.arrow_width - 5, arrow_y, self.arrow_width, self.arrow_height)
            right_arrow_rect = pygame.Rect(card_img_origin_x + self.card_width + 5, arrow_y, self.arrow_width, self.arrow_height)
            self.card_slots_positions.append({
                'card_rect': card_rect,
                'left_arrow_rect': left_arrow_rect,
                'right_arrow_rect': right_arrow_rect
            })


    def _cache_images(self):
        """Pré-renderiza imagens para a paleta e as cartas para melhorar o desempenho."""
        self.cached_palette_images = {}
        for (piece, side), img in self.tile_images.items():
            scaled_img = pygame.transform.scale(img, (PALETTE_TILE_SIZE, PALETTE_TILE_SIZE))
            rounded_img = apply_rounded_corners(scaled_img, PALETTE_CORNER_RADIUS)
            self.cached_palette_images[(piece, side)] = rounded_img

        self.cached_card_images = []
        if self.card_images and self.card_width > 0 and self.card_height > 0:
            for card_image in self.card_images:
                scaled_card = pygame.transform.smoothscale(card_image, (int(self.card_width), int(self.card_height)))
                rounded_card = apply_rounded_corners(scaled_card, CARD_CORNER_RADIUS)
                self.cached_card_images.append(rounded_card)

    def load_tile_images(self):
        cache = {}
        try:
            for side in range(2):
                for piece in range(9):
                    path = os.path.join(TILES_DIR, f"{side}_{piece}.png")
                    cache[(piece, side)] = pygame.image.load(path).convert_alpha()
        except pygame.error:
            return None
        return cache

    def load_card_images(self):
        images = []
        if not os.path.isdir(CARDS_DIR): return []
        try:
            card_files = os.listdir(CARDS_DIR)
            sorted_files = sorted(
                [f for f in card_files if f.endswith('.jpg')],
                key=lambda fname: int(os.path.splitext(fname)[0])
            )
            for filename in sorted_files:
                path = os.path.join(CARDS_DIR, filename)
                images.append(pygame.image.load(path).convert())
        except (pygame.error, ValueError) as e:
            print(f"ERRO ao carregar imagens das cartas: {e}")
            return []
        return images

    def load_card_data(self):
        if not os.path.exists(CARDS_JSON_PATH):
            print(f"ERRO: Arquivo de dados das cartas não encontrado em '{CARDS_JSON_PATH}'")
            return None
        try:
            with open(CARDS_JSON_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return {card['number']: card for card in data}
        except (json.JSONDecodeError, IOError) as e:
            print(f"ERRO ao ler o arquivo JSON das cartas: {e}")
            return None

    def setup_palette(self):
        palette = []
        start_x, start_y = self.palette_origin
        for piece in range(9):
            x = start_x + piece * (PALETTE_TILE_SIZE + PALETTE_PADDING)
            rect_a = pygame.Rect(x, start_y, PALETTE_TILE_SIZE, PALETTE_TILE_SIZE)
            palette.append({'rect': rect_a, 'piece': piece, 'side': 0})
            y_b = start_y + PALETTE_TILE_SIZE + PALETTE_PADDING
            rect_b = pygame.Rect(x, y_b, PALETTE_TILE_SIZE, PALETTE_TILE_SIZE)
            palette.append({'rect': rect_b, 'piece': piece, 'side': 1})
        return palette

    def setup_card_slots(self):
        slots = []
        for i in range(3):
            pos_data = self.card_slots_positions[i]
            slots.append({
                'current_index': i, 
                'card_rect': pos_data['card_rect'],
                'left_arrow_rect': pos_data['left_arrow_rect'], 
                'right_arrow_rect': pos_data['right_arrow_rect']
            })
        return slots

    def get_grid_cell_from_mouse(self, mouse_pos):
        if self.grid_origin[0] <= mouse_pos[0] < self.grid_origin[0] + self.grid_size and \
           self.grid_origin[1] <= mouse_pos[1] < self.grid_origin[1] + self.grid_size:
            col = int((mouse_pos[0] - self.grid_origin[0]) // self.tile_size)
            row = int((mouse_pos[1] - self.grid_origin[1]) // self.tile_size)
            
            if 0 <= row < GRID_DIM and 0 <= col < GRID_DIM:
                return row, col
        return None

    def update_pieces_on_grid(self):
        self.pieces_on_grid = {data[0] for row in self.grid_state for data in row if data is not None}

    def handle_events(self):
        for event in pygame.event.get():
            if event.type == pygame.QUIT or (event.type == pygame.KEYDOWN and event.key == pygame.K_ESCAPE):
                return False
            
            elif event.type == pygame.MOUSEBUTTONDOWN:
                if event.button == 1:
                    if self.card_images:
                        for i, slot in enumerate(self.card_slots):
                            num_cards = len(self.card_images)
                            if slot['left_arrow_rect'].collidepoint(event.pos):
                                self.card_slots[i]['current_index'] = (slot['current_index'] - 1 + num_cards) % num_cards
                                return True
                            elif slot['right_arrow_rect'].collidepoint(event.pos):
                                self.card_slots[i]['current_index'] = (slot['current_index'] + 1) % num_cards
                                return True

                    cell = self.get_grid_cell_from_mouse(event.pos)
                    for tile in self.palette_tiles:
                        if tile['piece'] not in self.pieces_on_grid and tile['rect'].collidepoint(event.pos):
                            tile_data = (tile['piece'], tile['side'], 0)
                            self.drag_state.update({'is_dragging': True, 'data': tile_data, 'source_cell': 'palette'})
                            original_img = self.tile_images[(tile['piece'], tile['side'])]
                            scaled_img = pygame.transform.scale(original_img, (self.tile_size, self.tile_size))
                            self.drag_state['image'] = apply_rounded_corners(scaled_img, GRID_CORNER_RADIUS)
                            self.drag_state['offset'] = (event.pos[0] - tile['rect'].x, event.pos[1] - tile['rect'].y)
                            break
                                        
                    if not self.drag_state['is_dragging'] and cell and self.grid_state[cell[0]][cell[1]]:
                        self.click_state = {'cell': cell, 'start_time': pygame.time.get_ticks()}
                        piece, side, orient = self.grid_state[cell[0]][cell[1]]
                        self.drag_state.update({'is_dragging': True, 'data': (piece, side, orient), 'source_cell': cell})
                        original_img = self.tile_images[(piece, side)]
                        scaled = pygame.transform.scale(original_img, (self.tile_size, self.tile_size))
                        rotated = pygame.transform.rotate(scaled, -90 * orient)
                        self.drag_state['image'] = apply_rounded_corners(rotated, GRID_CORNER_RADIUS)
                        cell_rect = pygame.Rect(self.grid_origin[0] + cell[1] * self.tile_size, self.grid_origin[1] + cell[0] * self.tile_size, self.tile_size, self.tile_size)
                        self.drag_state['offset'] = (event.pos[0] - cell_rect.x, event.pos[1] - cell_rect.y)
                        self.grid_state[cell[0]][cell[1]] = None

                elif event.button == 3 and (cell := self.get_grid_cell_from_mouse(event.pos)) and self.grid_state[cell[0]][cell[1]]:
                    piece, side, orient = self.grid_state[cell[0]][cell[1]]
                    self.grid_state[cell[0]][cell[1]] = (piece, 1 - side, orient)

            elif event.type == pygame.MOUSEBUTTONUP and event.button == 1:
                if self.drag_state['is_dragging']:
                    target_cell = self.get_grid_cell_from_mouse(event.pos)
                    source_cell = self.drag_state['source_cell']
                    dragged_data = self.drag_state['data']
                    
                    if source_cell != 'palette':
                        if target_cell:
                            target_data = self.grid_state[target_cell[0]][target_cell[1]]
                            self.grid_state[target_cell[0]][target_cell[1]] = dragged_data
                            if target_data: self.grid_state[source_cell[0]][source_cell[1]] = target_data
                        elif self.palette_bg_rect.collidepoint(event.pos): pass 
                        else: self.grid_state[source_cell[0]][source_cell[1]] = dragged_data
                    elif target_cell: self.grid_state[target_cell[0]][target_cell[1]] = dragged_data
                                        
                    self.drag_state = {'is_dragging': False, 'data': None, 'image': None, 'offset': (0, 0), 'source_cell': None}
                    self.update_pieces_on_grid()

                if self.click_state['cell']:
                    duration = pygame.time.get_ticks() - self.click_state['start_time']
                    if not self.drag_state['is_dragging'] and duration < CLICK_THRESHOLD:
                        r, c = self.click_state['cell']
                        if self.grid_state[r][c]:
                            piece, side, orient = self.grid_state[r][c]
                            self.grid_state[r][c] = (piece, side, (orient + 1) % 4)
                    self.click_state['cell'] = None
                        
            elif event.type == pygame.MOUSEMOTION:
                mouse_pos = event.pos
                grid_cell = self.get_grid_cell_from_mouse(mouse_pos)
                
                if not self.drag_state['is_dragging'] and grid_cell and self.grid_state[grid_cell[0]][grid_cell[1]]:
                    if self.hover_state.get('cell') != grid_cell:
                        # --- CORREÇÃO: Armazena o retângulo da célula específica ---
                        r, c = grid_cell
                        cell_rect = pygame.Rect(
                            self.grid_origin[0] + c * self.tile_size,
                            self.grid_origin[1] + r * self.tile_size,
                            self.tile_size, self.tile_size
                        )
                        self.hover_state = {'cell': grid_cell, 'cell_rect': cell_rect, 'card_slot_index': None, 'start_time': pygame.time.get_ticks(), 'show_popup': False}
                
                elif self.card_images and not self.drag_state['is_dragging']:
                    hovered_card_slot = None
                    for i, slot in enumerate(self.card_slots):
                        if slot['card_rect'].collidepoint(mouse_pos):
                            hovered_card_slot = i
                            break
                    if self.hover_state.get('card_slot_index') != hovered_card_slot:
                         self.hover_state = {'cell': None, 'card_slot_index': hovered_card_slot, 'start_time': pygame.time.get_ticks(), 'show_popup': False}
                else:
                    self.hover_state = {'cell': None, 'card_slot_index': None, 'start_time': 0, 'show_popup': False}
        return True

    def update(self):
        if (self.hover_state.get('cell') is not None or self.hover_state.get('card_slot_index') is not None) and not self.hover_state['show_popup']:
            if pygame.time.get_ticks() - self.hover_state['start_time'] > HOVER_DURATION:
                self.hover_state['show_popup'] = True

    def draw(self):
        self.screen.fill(COLOR_BACKGROUND)
        self.draw_palette()
        self.draw_grid()
        self.draw_card_area()
        self.draw_hover_popup()
        self.draw_dragged_tile()
        pygame.display.flip()

    def draw_grid(self):
        grid_surface = pygame.Surface((self.grid_size, self.grid_size), pygame.SRCALPHA)
        grid_surface.fill(COLOR_GRID_BG)
        for r in range(GRID_DIM):
            for c in range(GRID_DIM):
                tile_data = self.grid_state[r][c]
                if tile_data:
                    img = self.tile_images.get((tile_data[0], tile_data[1]))
                    if img:
                        scaled_img = pygame.transform.scale(img, (self.tile_size, self.tile_size))
                        rotated_img = pygame.transform.rotate(scaled_img, -90 * tile_data[2])
                        rect = rotated_img.get_rect(center=(c * self.tile_size + self.tile_size // 2, r * self.tile_size + self.tile_size // 2))
                        grid_surface.blit(rotated_img, rect.topleft)

        mask = pygame.Surface((self.grid_size, self.grid_size), pygame.SRCALPHA)
        pygame.draw.rect(mask, (255, 255, 255, 255), (0, 0, self.grid_size, self.grid_size), border_radius=GRID_CORNER_RADIUS)
        grid_surface.blit(mask, (0, 0), special_flags=pygame.BLEND_RGBA_MULT)
        for i in range(1, GRID_DIM):
            pygame.draw.line(grid_surface, COLOR_GRID_LINES, (0, i * self.tile_size), (self.grid_size, i * self.tile_size), 3)
            pygame.draw.line(grid_surface, COLOR_GRID_LINES, (i * self.tile_size, 0), (i * self.tile_size, self.grid_size), 3)
        pygame.draw.rect(grid_surface, COLOR_GRID_LINES, (0, 0, self.grid_size, self.grid_size), 3, border_radius=GRID_CORNER_RADIUS)
        self.screen.blit(grid_surface, self.grid_origin)

    def draw_palette(self):
        pygame.draw.rect(self.screen, COLOR_PALETTE_BG, self.palette_bg_rect, border_radius=GRID_CORNER_RADIUS)
        for tile in self.palette_tiles:
            if tile['piece'] not in self.pieces_on_grid:
                cached_img = self.cached_palette_images.get((tile['piece'], tile['side']))
                if cached_img:
                    self.screen.blit(cached_img, tile['rect'].topleft)

    def draw_card_area(self):
        if not self.cached_card_images: return
        
        pygame.draw.rect(self.screen, COLOR_PALETTE_BG, self.card_area_bg_rect, border_radius=GRID_CORNER_RADIUS)
        
        mouse_pos = pygame.mouse.get_pos()
        for slot in self.card_slots:
            rounded_card = self.cached_card_images[slot['current_index']]
            self.screen.blit(rounded_card, slot['card_rect'].topleft)
            
            left_color = COLOR_ARROW_HOVER if slot['left_arrow_rect'].collidepoint(mouse_pos) else COLOR_ARROW
            pygame.draw.polygon(self.screen, left_color, [(slot['left_arrow_rect'].right, slot['left_arrow_rect'].top), (slot['left_arrow_rect'].right, slot['left_arrow_rect'].bottom), (slot['left_arrow_rect'].left, slot['left_arrow_rect'].centery)])
            right_color = COLOR_ARROW_HOVER if slot['right_arrow_rect'].collidepoint(mouse_pos) else COLOR_ARROW
            pygame.draw.polygon(self.screen, right_color, [(slot['right_arrow_rect'].left, slot['right_arrow_rect'].top), (slot['right_arrow_rect'].left, slot['right_arrow_rect'].bottom), (slot['right_arrow_rect'].right, slot['right_arrow_rect'].centery)])

    def draw_dragged_tile(self):
        if self.drag_state['is_dragging']:
            mouse_pos = pygame.mouse.get_pos()
            top_left_pos = (mouse_pos[0] - self.drag_state['offset'][0], mouse_pos[1] - self.drag_state['offset'][1])
            self.screen.blit(self.drag_state['image'], top_left_pos)

    def draw_hover_popup(self):
        if not self.hover_state['show_popup']:
            return
        
        mouse_pos = pygame.mouse.get_pos()
        
        # --- CORREÇÃO: Verifica a colisão com o retângulo da célula individual ---
        if self.hover_state.get('cell') and self.hover_state.get('cell_rect') and self.hover_state['cell_rect'].collidepoint(mouse_pos):
            r, c = self.hover_state['cell']
            tile_data = self.grid_state[r][c]
            if not tile_data: return
            piece, side, orient = tile_data
            popup_img_original = self.tile_images.get((piece, 1 - side))
            if popup_img_original:
                scaled_img = pygame.transform.scale(popup_img_original, (self.tile_size, self.tile_size))
                rotated_popup = pygame.transform.rotate(scaled_img, -90 * orient)
                popup_rect = rotated_popup.get_rect(topleft=(mouse_pos[0] + 20, mouse_pos[1]))
                bg_rect = popup_rect.inflate(10, 10)
                pygame.draw.rect(self.screen, COLOR_POPUP_BG, bg_rect, border_radius=5)
                pygame.draw.rect(self.screen, COLOR_POPUP_BORDER, bg_rect, 2, 5)
                self.screen.blit(rotated_popup, popup_rect)
        
        elif self.hover_state['card_slot_index'] is not None and self.card_data:
            slot_index = self.hover_state['card_slot_index']
            card_index = self.card_slots[slot_index]['current_index']
            card_number = card_index + 1 
            
            data = self.card_data.get(card_number)
            if not data: return

            title_surf = self.font_card_title.render(f"{data['number']}. {data['name']}", True, COLOR_TEXT)
            
            wrapped_lines = textwrap.wrap(data['description'], width=40)
            desc_surfs = [self.font_card_desc.render(line, True, COLOR_TEXT) for line in wrapped_lines]

            popup_width = max(title_surf.get_width(), max((s.get_width() for s in desc_surfs), default=0)) + 20
            popup_height = title_surf.get_height() + sum(s.get_height() for s in desc_surfs) + 20
            
            popup_rect = pygame.Rect(mouse_pos[0] - popup_width - 10, mouse_pos[1], popup_width, popup_height)

            pygame.draw.rect(self.screen, COLOR_POPUP_BG, popup_rect, border_radius=8)
            pygame.draw.rect(self.screen, COLOR_POPUP_BORDER, popup_rect, 2, 8)
            
            self.screen.blit(title_surf, (popup_rect.x + 10, popup_rect.y + 10))
            current_y = popup_rect.y + 10 + title_surf.get_height()
            for surf in desc_surfs:
                self.screen.blit(surf, (popup_rect.x + 10, current_y))
                current_y += surf.get_height()

    def run(self):
        running = True
        while running:
            running = self.handle_events()
            self.update()
            self.draw()
            self.clock.tick(60)
        pygame.quit()

if __name__ == "__main__":
    if not os.path.isdir(TILES_DIR):
        print(f"ERRO: Diretório de peças não encontrado em '{TILES_DIR}'")
    else:
        editor = InteractiveBoard()
        editor.run()

