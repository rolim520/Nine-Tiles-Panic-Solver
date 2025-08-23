import json
from copy import deepcopy

with open('Tiles/tiles.json', 'r', encoding='utf-8') as file:
    # guarda as tiles do jogo na lista game_tiles
    game_tiles = json.load(file)

# def rotated_tile(road, orientation):
#     return [(road[0]+orientation)%4, (road[1]+orientation)%4]

def tile_connetions(tile):
    connections = set()
    for road in game_tiles[tile[0]][tile[1]]["roads"]:
        connections.add((road['connection'][0]+tile[2])%4)
        connections.add((road['connection'][1]+tile[2])%4)
    return connections

def find_candidate_tiles(tiling, position):
    coord = [position//3, position%3]

    # Obtém as tilings já utilizadas
    used_tiles = set()
    for i in range(3):
        for j in range(3):
            if tiling[i][j] != ():
                used_tiles.add(tiling[i][j][0])

    # Obtém as conexões que a nova tile deve ter
    required_connections = set()
    if coord[0]-1 >= 0 and tiling[coord[0]-1][coord[1]] != () and 3 in tile_connetions(tiling[coord[0]-1][coord[1]]):
        required_connections.add(1)
    if coord[0]+1 <= 2 and tiling[coord[0]+1][coord[1]] != () and 1 in tile_connetions(tiling[coord[0]+1][coord[1]]):
        required_connections.add(3)
    if coord[1]-1 >= 0 and tiling[coord[0]][coord[1]-1] != () and 2 in tile_connetions(tiling[coord[0]][coord[1]-1]):
        required_connections.add(0)
    if coord[1]+1 <= 2 and tiling[coord[0]][coord[1]+1] != () and 0 in tile_connetions(tiling[coord[0]][coord[1]+1]):
        required_connections.add(2)
    
    # Encontra as candidatas com as conexões encontradas
    candidates = []
    for piece in range(9):
        if piece not in used_tiles:
            for side in range(2):
                for orientation in range(4):
                    if required_connections.issubset(tile_connetions((piece, side, orientation))):
                        candidates.append((piece, side, orientation))
    return candidates

    

def find_valid_tilings(tiling, position):
    coord = [position//3, position%3]

    if position == 9:
        print(tiling)
        return [tiling]

    valid_tilings = []
    candidates = find_candidate_tiles(tiling, position)
    for candidate in candidates:
        new_tiling = deepcopy(tiling)
        new_tiling[coord[0]][coord[1]] = candidate
        valid_tilings += find_valid_tilings(new_tiling, position+1)
    return valid_tilings


# Cria um tensor tridimensional para guardar para cada posição da grid 3x3 uma lista contendo [peça, lado, orientação]
tiling = [[() for _ in range(3)] for _ in range(3)]

tiling_solutions = []

# Todas as posições da peça numero 0
initial_candidates = [(0,i,j) for i in range(2) for j in range(4)]


# for candidate in initial_candidates:
#     tiling = deepcopy(tiling)
#    tiling[0][0] = candidate
#    tiling_solutions += find_valid_tilings(tiling, 0)

# for candidate in initial_candidates:
#     tiling = deepcopy(tiling)
tiling[0][0] = initial_candidates[0]
tiling_solutions += find_valid_tilings(tiling, 1)

print(len(tiling_solutions))