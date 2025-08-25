# solver.py

from constants import TILE_NODES

def have_road_cycles(tiling, game_tiles):
    """
    Checks for road cycles in a given tiling using a Union-Find algorithm.
    """
    parent = list(range(24)) # NUM_NODES could be used here

    def find(i):
        if parent[i] == i:
            return i
        parent[i] = find(parent[i])
        return parent[i]

    def union(i, j):
        root_i = find(i)
        root_j = find(j)
        if root_i != root_j:
            parent[root_i] = root_j

    for r in range(3):
        for c in range(3):
            (piece, side, orientation) = tiling[r][c]
            piece_position = r * 3 + c
            
            for road in game_tiles[piece][side]["roads"]:
                local_conn1, local_conn2 = road['connection']
                
                # Use the imported TILE_NODES constant
                global_id1 = TILE_NODES[piece_position][(local_conn1 + orientation) % 4]
                global_id2 = TILE_NODES[piece_position][(local_conn2 + orientation) % 4]

                if find(global_id1) == find(global_id2):
                    return True  # Cycle detected
                
                union(global_id1, global_id2)

    return False

def connects(required_connections, tile_connections):
    # ... (identical code)
    for i in range(4):
        if required_connections[i] != -1 and required_connections[i] != tile_connections[i]:
            return False
    return True

def generate_tile_connections(game_tiles):
    # ... (identical code, but now takes game_tiles as an argument)
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
    # ... (identical code, but now takes tile_connections as an argument)
    row, col = position // 3, position % 3
    required_connections = [-1, -1, -1, -1]
    # ... (rest of the function)
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


def find_valid_tilings_generator(tiling, position, available_pieces, game_tiles, tile_connections):
    # ... (identical code, but now takes game_tiles and tile_connections as arguments)
    if position == 9:
        if not have_road_cycles(tiling, game_tiles):
            yield [row[:] for row in tiling]
        return

    row, col = position // 3, position % 3

    if tiling[row][col] != ():
        yield from find_valid_tilings_generator(tiling, position + 1, available_pieces, game_tiles, tile_connections)
    else:
        candidates = find_candidate_tiles(tiling, position, available_pieces, tile_connections)
        for candidate in candidates:
            tiling[row][col] = candidate
            available_pieces.remove(candidate[0])
            yield from find_valid_tilings_generator(tiling, position + 1, available_pieces, game_tiles, tile_connections)
            available_pieces.add(candidate[0])
            tiling[row][col] = ()