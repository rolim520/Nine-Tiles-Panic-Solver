from constants import TILE_NODES, NEIGHBOURS, WEST, NORTH, EAST, SOUTH

def update_position_domain(node_states, position, available_pieces, connections_candidates):
    
    required_connections = [-1, -1, -1, -1]
    required_connections[NORTH] = node_states[TILE_NODES[position][NORTH]]
    required_connections[EAST] = node_states[TILE_NODES[position][EAST]]
    required_connections[SOUTH] = node_states[TILE_NODES[position][SOUTH]]
    required_connections[WEST] = node_states[TILE_NODES[position][WEST]]

    candidates = connections_candidates[tuple(required_connections)]
    domain = [candidate for candidate in candidates if candidate[0] in available_pieces]

    return domain


def find_valid_boards_generator(board_state, node_states, available_pieces, game_tiles, tile_connections, connections_candidates, uf_structure, domains):
    
    available_positions = [i for i in range(9) if domains[i] is not None]
    
    if not available_positions:
        yield board_state, uf_structure
        return

    # Gets the position with the smallest domain (MRV)
    position = min(available_positions, key=lambda i: len(domains[i]))
    
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

        board_state[position] = candidate
        available_pieces.remove(piece)

        new_domains = domains[:]
        new_domains[position] = None
        new_node_states = node_states[:]
        
        piece_conns = tile_connections[piece][side][orientation]
        new_node_states[TILE_NODES[position][NORTH]] = piece_conns[NORTH]
        new_node_states[TILE_NODES[position][EAST]]  = piece_conns[EAST]
        new_node_states[TILE_NODES[position][SOUTH]] = piece_conns[SOUTH]
        new_node_states[TILE_NODES[position][WEST]]  = piece_conns[WEST]

        dead_end_found = False
        neighbors = NEIGHBOURS[position]
        
        # Forward Checking
        for pos in range(9):
            if new_domains[pos] is None:
                continue
                
            if pos in neighbors:
                # Orthogonal neighbor: recalculates using your new domain function
                updated_domain = update_position_domain(new_node_states, pos, available_pieces, connections_candidates)
            else:
                # Distant hole: topology is the same, just remove the used piece
                updated_domain = [cand for cand in new_domains[pos] if cand[0] != piece]

            if not updated_domain:
                dead_end_found = True
                break
            
            new_domains[pos] = updated_domain

        # If a dead end is found, prunes the entire branch
        if dead_end_found:
            available_pieces.add(piece)
            board_state[position] = None
            continue

        # Goes down a level in the tree
        for solution, final_uf in find_valid_boards_generator(board_state, new_node_states, available_pieces, game_tiles, tile_connections, connections_candidates, uf_copy, new_domains):
            yield solution, final_uf

        # Undoes the move (Backtrack)
        available_pieces.add(piece)
        board_state[position] = None