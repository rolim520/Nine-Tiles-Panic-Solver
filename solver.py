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

def find_valid_tilings_generator(tiling, position, available_pieces, game_tiles, tile_connections, uf_structure):
    if position == 9:
        yield [row[:] for row in tiling]
        return

    row, col = position // 3, position % 3

    if tiling[row][col]:
        yield from find_valid_tilings_generator(tiling, position + 1, available_pieces, game_tiles, tile_connections, uf_structure)
    else:
        candidates = find_candidate_tiles(tiling, position, available_pieces, tile_connections)
        
        for candidate in candidates:
            # 1. Create a copy of the UnionFind structure to work with.
            uf_copy = uf_structure.copy()
            cycle_found = False

            # 2. Apply unions for the new candidate tile using the copy's methods.
            (piece, side, orientation) = candidate
            for road in game_tiles[piece][side]["roads"]:
                local_conn1, local_conn2 = road['connection']
                global_id1 = TILE_NODES[position][(local_conn1 + orientation) % 4]
                global_id2 = TILE_NODES[position][(local_conn2 + orientation) % 4]

                # The logic is now a clean method call.
                if uf_copy.union(global_id1, global_id2):
                    cycle_found = True
                    break
            
            # 3. If a cycle was found, prune this branch.
            if cycle_found:
                continue

            # 4. If valid, place the tile and recurse with the updated copy.
            tiling[row][col] = candidate
            available_pieces.remove(candidate[0])
            
            yield from find_valid_tilings_generator(tiling, position + 1, available_pieces, game_tiles, tile_connections, uf_copy)
            
            # Backtrack (no change needed here).
            available_pieces.add(candidate[0])
            tiling[row][col] = ()