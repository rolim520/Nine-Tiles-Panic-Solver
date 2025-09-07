# solver.py

from constants import TILE_NODES, WEST, NORTH, EAST, SOUTH

def find_candidate_tiles(tiling, position, available_pieces, tile_connections, connections_candidates):
    row, col = position // 3, position % 3
    required_connections = [-1, -1, -1, -1]

    if row > 0 and tiling[row-1, col, 0] != -1:
        piece, side, orient = tiling[row-1, col]
        required_connections[NORTH] = tile_connections[piece, side, orient, SOUTH]

    if row < 2 and tiling[row+1, col, 0] != -1:
        piece, side, orient = tiling[row+1, col]
        required_connections[SOUTH] = tile_connections[piece, side, orient, NORTH]

    if col > 0 and tiling[row, col-1, 0] != -1:
        piece, side, orient = tiling[row, col-1]
        required_connections[WEST] = tile_connections[piece, side, orient, EAST]

    if col < 2 and tiling[row, col+1, 0] != -1:
        piece, side, orient = tiling[row, col+1]
        required_connections[EAST] = tile_connections[piece, side, orient, WEST]

    potential_candidates = connections_candidates[tuple(required_connections)]
    final_candidates = [candidate for candidate in potential_candidates if candidate[0] in available_pieces]

    return final_candidates


def find_valid_tilings_generator(tiling, available_pieces, game_tiles, tile_connections, connections_candidates, uf_structure, candidate_domains):
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

        # 1. Cria uma cópia dos domínios restantes
        new_domains = candidate_domains.copy()
        del new_domains[best_next_cell]
        
        dead_end_found = False
        for (empty_r, empty_c) in new_domains.keys():
            # The erroneous 'if' condition has been removed.
            # We now correctly re-evaluate every remaining empty cell's domain.
            empty_pos = empty_r * 3 + empty_c

            updated_candidates = find_candidate_tiles(tiling, empty_pos, available_pieces, tile_connections, connections_candidates)

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

        # Chama a recursão com o estado e domínios consistentes
        yield from find_valid_tilings_generator(tiling, available_pieces, game_tiles, tile_connections, connections_candidates, uf_copy, new_domains)
        
        # Desfaz a jogada (Backtrack)
        available_pieces.add(piece)
        tiling[r, c] = -1