import json

with open('Tiles/tiles.json', 'r', encoding='utf-8') as file:
    # guarda as tiles do jogo na lista game_tiles
    game_tiles = json.load(file)

def connects(required_connections, tile_connections):
    for i in range(4):
        if required_connections[i] != -1 and required_connections[i] != tile_connections[i]:
            return False
    return True

def generate_tile_connections():
    tile_connections = dict()
    connections = [0, 0, 0, 0]
    for piece in range(9):
        for side in range(2):
            for orientation in range(4):
                connections = [0, 0, 0, 0]
                for road in game_tiles[piece][side]["roads"]:
                    connections[(road['connection'][0]+orientation)%4] = 1
                    connections[(road['connection'][1]+orientation)%4] = 1
                tile_connections[(piece, side, orientation)] = connections
    return tile_connections

def find_candidate_tiles(tiling, position, available_pieces):
    row, col = position // 3, position % 3

    # Obtém as conexões que a nova tile deve ter
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
    
    # Encontra as candidatas com as conexões encontradas
    candidates = []
    for piece in available_pieces:
        for side in range(2):
            for orientation in range(4):
                if connects(required_connections, tile_connections[(piece, side, orientation)]):
                    candidates.append((piece, side, orientation))
    return candidates

def find_valid_tilings(tiling, position, available_pieces, solutions_list):
    if position == 9:
        solutions_list.append([row[:] for row in tiling])
        print(tiling, len(solutions_list))
        return

    # Convert linear position to grid coordinates
    row, col = position // 3, position % 3

    # Find all valid candidates for the current empty spot
    candidates = find_candidate_tiles(tiling, position, available_pieces)

    # Explore each candidate
    for candidate in candidates:
        tiling[row][col] = candidate
        available_pieces.remove(candidate[0])
        find_valid_tilings(tiling, position + 1, available_pieces, solutions_list)
        available_pieces.add(candidate[0])
        tiling[row][col] = ()


# Caching para as conexões de cada peça
tile_connections = generate_tile_connections()

# Todas as posições da peça numero 0
initial_candidates = [(0,i,j) for i in range(2) for j in range(4)]

tiling_solutions = []
for candidate in initial_candidates:
    # Cria um tensor tridimensional para guardar para cada posição da grid 3x3 uma lista contendo [peça, lado, orientação]
    tiling = [[() for _ in range(3)] for _ in range(3)] 
    tiling[0][0] = candidate
    available_pieces = set([1,2,3,4,5,6,7,8]) # Zero já foi removido
    find_valid_tilings(tiling, 1, available_pieces, tiling_solutions)

print(len(tiling_solutions))