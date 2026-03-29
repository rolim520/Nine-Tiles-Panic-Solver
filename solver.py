# solver.py

from constants import TILE_NODES, NEIGHBOURS, WEST, NORTH, EAST, SOUTH

def update_position_domain(global_nodes, position, available_pieces, connections_candidates):
    
    required_connections = [-1, -1, -1, -1]
    required_connections[NORTH] = global_nodes[TILE_NODES[position][NORTH]]
    required_connections[EAST] = global_nodes[TILE_NODES[position][EAST]]
    required_connections[SOUTH] = global_nodes[TILE_NODES[position][SOUTH]]
    required_connections[WEST] = global_nodes[TILE_NODES[position][WEST]]

    candidates = connections_candidates[tuple(required_connections)]
    domain = [candidate for candidate in candidates if candidate[0] in available_pieces]

    return domain


def find_valid_tilings_generator(tiling, global_nodes, available_pieces, game_tiles, tile_connections, connections_candidates, uf_structure, domains):
    
    # 1. Encontra as posições disponiveis
    available_positions = [i for i in range(9) if domains[i] is not None]
    
    if not available_positions:
        # Não tem mais posições vazias, achamos uma solução! (Removemos o .tolist())
        yield tiling, uf_structure
        return

    # 2. MRV: Pega a posição com o menor domínio
    position = min(available_positions, key=lambda i: len(domains[i]))
    
    # Itera sobre os candidatos para a célula escolhida
    for candidate in domains[position]:
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

        # 3. Aplica a jogada
        tiling[position] = candidate
        available_pieces.remove(piece)

        # 4. Cópia rápida dos estados (Nativo em C, super rápido)
        new_domains = domains[:]
        new_domains[position] = None
        new_global_nodes = global_nodes[:]
        
        # 5. Atualiza o grafo global com a nova peça usando o padrão N-E-S-W
        piece_conns = tile_connections[piece][side][orientation]
        new_global_nodes[TILE_NODES[position][NORTH]] = piece_conns[NORTH]
        new_global_nodes[TILE_NODES[position][EAST]]  = piece_conns[EAST]
        new_global_nodes[TILE_NODES[position][SOUTH]] = piece_conns[SOUTH]
        new_global_nodes[TILE_NODES[position][WEST]]  = piece_conns[WEST]

        dead_end_found = False
        neighbors = NEIGHBOURS[position]
        
        # 6. Forward Checking Híbrido
        for pos in range(9):
            if new_domains[pos] is None:
                continue
                
            if pos in neighbors:
                # Vizinho ortogonal: recalcula usando a sua nova função de domínio
                updated_domain = update_position_domain(new_global_nodes, pos, available_pieces, connections_candidates)
            else:
                # Buraco distante: a topologia é a mesma, só tira a peça que gastei
                updated_domain = [cand for cand in new_domains[pos] if cand[0] != piece]

            if not updated_domain:
                dead_end_found = True
                break
            
            new_domains[pos] = updated_domain

        # Se um beco sem saída foi encontrado, poda o galho inteiro
        if dead_end_found:
            available_pieces.add(piece)
            tiling[position] = None
            continue

        # Desce o nível na árvore
        for solution, final_uf in find_valid_tilings_generator(tiling, new_global_nodes, available_pieces, game_tiles, tile_connections, connections_candidates, uf_copy, new_domains):
            yield solution, final_uf

        # Desfaz a jogada (Backtrack)
        available_pieces.add(piece)
        tiling[position] = None