# solver.py

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

def connects(required_connections, tile_connections):
    for i in range(4):
        if required_connections[i] != -1 and required_connections[i] != tile_connections[i]:
            return False
    return True

def generate_tile_connections(game_tiles):
    tile_connections = dict()
    for piece in range(9):
        for side in range(2):
            for orientation in range(4):
                connections = [0, 0, 0, 0]
                for road in game_tiles[piece][side]["roads"]:
                    connections[(road['connection'][0]+orientation)%4] = 1
                    connections[(road['connection'][1]+orientation)%4] = 1
                tile_connections[(piece, side, orientation)] = connections
    return tile_connections

def find_candidate_tiles(tiling, position, available_pieces, tile_connections):
    row, col = position // 3, position % 3
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
    
    candidates = []
    for piece in available_pieces:
        for side in range(2):
            for orientation in range(4):
                if connects(required_connections, tile_connections[(piece, side, orientation)]):
                    candidates.append((piece, side, orientation))
    return candidates

def find_valid_tilings_generator(tiling, available_pieces, game_tiles, tile_connections, uf_structure, candidate_domains):
    # BASE CASE: If there are no more domains to fill, a solution is found.
    if not candidate_domains:
        yield [row[:] for row in tiling]
        return

    # --- HEURISTIC: Find the best cell from the pre-calculated domains ---
    # The cell with the smallest domain (list of candidates) is the most constrained.
    best_next_cell = min(candidate_domains, key=lambda cell: len(candidate_domains[cell]))
    
    # --- RECURSIVE STEP ---
    r, c = best_next_cell
    position = r * 3 + c
    
    # Iterate through the candidates for ONLY the best cell.
    for candidate in candidate_domains[best_next_cell]:
        # A) Perform the cycle check (same as before)
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

        # B) If no cycle, prepare for the recursive call
        tiling[r][c] = candidate
        available_pieces.remove(candidate[0])

        # C) --- FORWARD CHECKING ---
        # Create a copy of the domains and remove the cell we just filled.
        new_domains = candidate_domains.copy()
        del new_domains[best_next_cell]
        
        # Update the domains of all remaining empty cells (the neighbors of our choice)
        dead_end_found = False
        for empty_r, empty_c in new_domains:
            empty_pos = empty_r * 3 + empty_c
            # Recalculate candidates for the neighbor based on the newly updated tiling.
            # This is where we "cross off" possibilities like in Sudoku.
            updated_candidates = find_candidate_tiles(tiling, empty_pos, available_pieces, tile_connections)
            
            if not updated_candidates:
                dead_end_found = True
                break # This path is invalid, no need to check other neighbors.
            
            new_domains[(empty_r, empty_c)] = updated_candidates

        # If forward checking led to a dead end, prune this branch.
        if dead_end_found:
            # Backtrack from the temporary changes before continuing the loop
            available_pieces.add(candidate[0])
            tiling[r][c] = ()
            continue

        # D) Make the recursive call with the pruned domains
        yield from find_valid_tilings_generator(tiling, available_pieces, game_tiles, tile_connections, uf_copy, new_domains)
        
        # E) Backtrack
        available_pieces.add(candidate[0])
        tiling[r][c] = ()