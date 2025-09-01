# solver.py

import numpy as np
from numba import njit
from constants import TILE_NODES, NUM_NODES

class UnionFind:
    """A class for the Union-Find data structure with cycle detection."""
    def __init__(self, size):
        # Initialize with each node as its own parent.
        self.parent = list(range(size))

    def find(self, i):
        """Finds the root of element i with path compression."""
        if self.parent[i] == i:
            return i
        # Path compression for efficiency
        self.parent[i] = self.find(self.parent[i])
        return self.parent[i]

    def union(self, i, j):
        """
        Connects i and j. Returns True if a cycle is formed, False otherwise.
        """
        root_i = self.find(i)
        root_j = self.find(j)
        if root_i != root_j:
            self.parent[root_i] = root_j
            return False  # No cycle created
        return True  # A cycle was detected!

    def copy(self):
        """
        Returns a new UnionFind instance with a copy of the current parent state.
        This is crucial for non-destructive updates during recursion.
        """
        new_uf = UnionFind(len(self.parent))
        new_uf.parent = self.parent[:]  # Create a shallow copy
        return new_uf

@njit
def connects(required_connections, tile_connections):
    for i in range(4):
        if required_connections[i] != -1 and required_connections[i] != tile_connections[i]:
            return False
    return True

def generate_tile_connections(game_tiles):
    # Cria um array NumPy 4D com formato (peça, lado, orientação, conexões)
    # Ex: (9 peças, 2 lados, 4 orientações, 4 pontos de conexão)
    tile_conns_array = np.zeros((9, 2, 4, 4), dtype=np.int8)
    
    for piece in range(9):
        for side in range(2):
            for orientation in range(4):
                connections = [0, 0, 0, 0]
                # Verifica se a peça/lado existe no JSON antes de acessar
                if side < len(game_tiles[piece]) and "roads" in game_tiles[piece][side]:
                    for road in game_tiles[piece][side]["roads"]:
                        connections[(road['connection'][0] + orientation) % 4] = 1
                        connections[(road['connection'][1] + orientation) % 4] = 1
                tile_conns_array[piece, side, orientation] = connections
    return tile_conns_array

@njit
def find_candidate_tiles(tiling, position, available_pieces, tile_connections):
    row, col = position // 3, position % 3
    required_connections = [-1, -1, -1, -1]

    # A lógica para determinar as conexões necessárias é a mesma
    if row > 0 and tiling[row-1, col, 0] != -1:
        piece, side, orient = tiling[row-1, col]
        required_connections[1] = tile_connections[piece, side, orient, 3]

    if row < 2 and tiling[row+1, col, 0] != -1:
        piece, side, orient = tiling[row+1, col]
        required_connections[3] = tile_connections[piece, side, orient, 1]

    if col > 0 and tiling[row, col-1, 0] != -1:
        piece, side, orient = tiling[row, col-1]
        required_connections[0] = tile_connections[piece, side, orient, 2]

    if col < 2 and tiling[row, col+1, 0] != -1:
        piece, side, orient = tiling[row, col+1]
        required_connections[2] = tile_connections[piece, side, orient, 0]
    
    candidates = []
    for piece in available_pieces:
        for side in range(2):
            for orientation in range(4):
                # Acessa os dados usando a indexação do NumPy array
                tile_conns = tile_connections[piece, side, orientation]
                if connects(required_connections, tile_conns):
                    candidates.append((piece, side, orientation))
    return candidates


def find_valid_tilings_generator(tiling, available_pieces, game_tiles, tile_connections, uf_structure, candidate_domains):
    if not candidate_domains:
        yield tiling.tolist()
        return

    best_next_cell = min(candidate_domains, key=lambda cell: len(candidate_domains[cell]))
    
    r, c = best_next_cell
    position = r * 3 + c
    
    # Itera sobre os candidatos para a célula escolhida
    for candidate in candidate_domains[best_next_cell]:
        uf_copy = uf_structure.copy()
        cycle_found = False
        (piece, side, orientation) = candidate
        
        for road in game_tiles[piece][side]["roads"]:
            local_conn1, local_conn2 = road['connection']
            global_id1 = TILE_NODES[position][(local_conn1 + orientation) % 4]
            global_id2 = TILE_NODES[position][(local_conn2 + orientation) % 4]
            if uf_copy.union(global_id1, global_id2):
                cycle_found = True
                break
        
        if cycle_found:
            continue

        # Aplica a jogada
        tiling[r, c] = candidate
        available_pieces.remove(piece)

        # --- CORREÇÃO APLICADA AQUI: FORWARD CHECKING ---
        # 1. Cria uma cópia dos domínios restantes
        new_domains = candidate_domains.copy()
        del new_domains[best_next_cell]
        
        dead_end_found = False
        for (empty_r, empty_c) in new_domains.keys():
            # The erroneous 'if' condition has been removed.
            # We now correctly re-evaluate every remaining empty cell's domain.
            empty_pos = empty_r * 3 + empty_c
            
            updated_candidates = find_candidate_tiles(tiling, empty_pos, available_pieces, tile_connections)
            
            if not updated_candidates:
                dead_end_found = True
                break
            
            new_domains[(empty_r, empty_c)] = updated_candidates

        # 3. Se um beco sem saída foi encontrado, não continua a recursão
        if dead_end_found:
            # Desfaz a jogada antes de tentar o próximo candidato
            available_pieces.add(piece)
            tiling[r, c] = -1
            continue
        # --- FIM DA CORREÇÃO ---

        # Chama a recursão com o estado e domínios consistentes
        yield from find_valid_tilings_generator(tiling, available_pieces, game_tiles, tile_connections, uf_copy, new_domains)
        
        # Desfaz a jogada (Backtrack)
        available_pieces.add(piece)
        tiling[r, c] = -1