# analysis.py

from collections import Counter, deque
from constants import TILE_NODES

STAT_KEYS = [
    "houses", "ufos", "girls", "boys", "dogs", "hamburgers",
    "aliens", "agents", "captured_aliens", "curves"
]

# =============================================================================
# SECTION 1: FUNÇÕES DE ESTATÍSTICAS INDIVIDUAIS
# =============================================================================

def _find_sets_in_sequence(road, sequence):
    """
    Calcula o numero de ocorrencias da sequencia na ordem normal e na ordem reversa
    na road. Permite strings vazias entre os elementos da sequência
    """
    if not sequence:
        return 0

    items = [item for item, _ in road]
    num_sets = 0
    used_indices = set()

    seq_idx = 0
    current_set_indices = []

    for i, item in enumerate(items):
        if item == "":
            continue
        if item == sequence[seq_idx]:
            current_set_indices.append(i)
            seq_idx += 1
        else:
            current_set_indices = []
            if item == sequence[0]:
                seq_idx = 1
                current_set_indices.append(i)
            else:
                seq_idx = 0
        if seq_idx == len(sequence):
            num_sets += 1
            used_indices.update(current_set_indices)
            seq_idx = 0
            current_set_indices = []
    
    seq_idx = 0
    reversed_sequence = sequence[::-1]
    for i, item in list(enumerate(items)):
        if item == "":
            continue
        if item == reversed_sequence[seq_idx] and i not in used_indices:
            seq_idx += 1
        else:
            if item == reversed_sequence[0] and i not in used_indices:
                seq_idx = 1
            else:
                seq_idx = 0
        if seq_idx == len(reversed_sequence):
            num_sets += 1
            seq_idx = 0
    
    return num_sets

def _calculate_captured_indices(agents, aliens):
    """
    Identifica os índices de aliens capturados.
    """
    captured_indices = set()
    sorted_agents = sorted(agents, key=lambda a: a['pos'])

    for agent in sorted_agents:
        agent_pos, agent_dir = agent['pos'], agent['dir']
        potential_targets = []
        if agent_dir == 1: # Olhando para frente
            potential_targets = [a for a in aliens if a['pos'] > agent_pos and a['pos'] not in captured_indices]
            if potential_targets:
                target = min(potential_targets, key=lambda a: a['pos'])
                captured_indices.add(target['pos'])

        elif agent_dir == 0: # Olhando para trás
            potential_targets = [a for a in aliens if a['pos'] < agent_pos and a['pos'] not in captured_indices]
            if potential_targets:
                target = max(potential_targets, key=lambda a: a['pos'])
                captured_indices.add(target['pos'])
    return captured_indices

def _calculate_max_aliens_running_towards_agent(aliens, agent_indices):
    if not agent_indices: return 0
    count_right = 0
    count_left = 0
    for alien in aliens:
        alien_pos, alien_dir = alien['pos'], alien['dir']
        if (alien_dir == 1 and any(a_idx > alien_pos for a_idx in agent_indices)):
            count_right += 1
        elif (alien_dir == 0 and any(a_idx < alien_pos for a_idx in agent_indices)):
            count_left += 1
    return max(count_right, count_left)

def _calculate_max_hamburgers_in_front_of_alien(road, aliens, captured_indices):
    max_hamburgers = 0
    uncaptured_aliens = [a for a in aliens if a['pos'] not in captured_indices]

    for alien in uncaptured_aliens:
        alien_pos, alien_dir = alien['pos'], alien['dir']
        current_hamburgers = 0

        if alien_dir == 1:  # Olhando para frente
            for i in range(alien_pos + 1, len(road)):
                item, item_dir = road[i]
                if item == "hamburger":
                    current_hamburgers += 1
                # A visão é bloqueada por outro alien não capturado olhando na mesma direção
                elif item == "alien" and item_dir == 1 and i not in captured_indices:
                    break
        elif alien_dir == 0: # Olhando para trás
            for i in range(alien_pos - 1, -1, -1):
                item, item_dir = road[i]
                if item == "hamburger":
                    current_hamburgers += 1
                # A visão é bloqueada por outro alien não capturado olhando na mesma direção
                elif item == "alien" and item_dir == 0 and i not in captured_indices:
                    break
        max_hamburgers = max(current_hamburgers, max_hamburgers)
        
    return max_hamburgers


def _calculate_max_aliens_between_agents(road, agents):

    max_aliens = 0
    for agent in agents:
        agent_pos, agent_dir = agent['pos'], agent['dir']
        current_aliens = 0
        if agent_dir == 1:
            for i in range(agent_pos + 1, len(road)):
                item, item_dir = road[i]
                if item == "alien":
                    current_aliens += 1
                elif item == "agent":
                    if item_dir == 1:
                        current_aliens = 0
                    break
        if agent_dir == 0:    
            for i in range(agent_pos - 1, -1, -1):
                item, item_dir = road[i]
                if item == "alien":
                    current_aliens += 1
                elif item == "agent":
                    if item_dir == 0:
                        current_aliens = 0
                    break
        max_aliens = max(current_aliens, max_aliens)
    return max_aliens


# =============================================================================
# SECTION 2: PROCESSAMENTO CENTRALIZADO E CONSTRUÇÃO DE ESTRADAS
# =============================================================================

def _process_road_for_stats(road):
    if not road: return {}

    all_items = {'alien': [], 'agent': [], 'hamburger': []}
    for i, (item, direction) in enumerate(road):
        if item in all_items:
            all_items[item].append({'pos': i, 'dir': direction})

    agent_indices = {agent['pos'] for agent in all_items['agent']}
    captured_indices = _calculate_captured_indices(all_items['agent'], all_items['alien'])

    return {
        'num_agents': len(all_items['agent']),
        'num_aliens': len(all_items['alien']),
        'aliens_caught': len(captured_indices),
        'max_aliens_running_towards_agent': _calculate_max_aliens_running_towards_agent(all_items['alien'], agent_indices),
        'max_hamburgers_in_front_of_alien': _calculate_max_hamburgers_in_front_of_alien(road, all_items['alien'], captured_indices),
        'max_aliens_between_two_agents': _calculate_max_aliens_between_agents(road, all_items['agent']),
        'food_chain_sets': _find_sets_in_sequence(road, ['agent', 'alien', 'hamburger']),
    }

def _build_all_roads(solution, game_tiles):
    adj, edge_map = {i: [] for i in range(24)}, {}
    for r in range(3):
        for c in range(3):
            (piece, side, orientation) = solution[r][c]
            position = r * 3 + c
            for road_info in game_tiles[piece][side].get("roads", []):
                c1, c2 = road_info['connection']
                g1 = TILE_NODES[position][(c1 + orientation) % 4]
                g2 = TILE_NODES[position][(c2 + orientation) % 4]
                adj[g1].append(g2); adj[g2].append(g1)
                d = road_info.get('direction', -1)
                target_node = -1
                if d != -1: target_node = TILE_NODES[position][(d + orientation) % 4]
                edge = tuple(sorted((g1, g2)))
                edge_map[edge] = {'item': road_info.get('item', ''), 'target_node': target_node}

    visited_nodes, all_roads = set(), []
    for i in range(24):
        if i not in visited_nodes and adj[i]:
            component_nodes, q = set(), deque([i]); visited_nodes.add(i)
            while q:
                u = q.popleft(); component_nodes.add(u)
                for v in adj[u]:
                    if v not in visited_nodes: visited_nodes.add(v); q.append(v)
            
            endpoints = [n for n in component_nodes if sum(1 for neighbor in adj[n] if neighbor in component_nodes) == 1]
            start_node = endpoints[0] if endpoints else min(component_nodes)
            
            path, prev, curr = [start_node], -1, start_node
            while len(path) < len(component_nodes):
                found = False
                for neighbor in adj[curr]:
                    if neighbor in component_nodes and neighbor != prev:
                        path.append(neighbor)
                        prev, curr = curr, neighbor
                        found = True
                        break
                if not found: break

            road_items = []
            for idx in range(len(path) - 1):
                u, v = path[idx], path[idx+1]
                edge = tuple(sorted((u, v)))
                if edge in edge_map:
                    data = edge_map[edge]
                    direction = -1
                    if data['target_node'] != -1: direction = 1 if data['target_node'] == v else 0
                    road_items.append((data['item'], direction))
            all_roads.append(road_items)
    return all_roads

def analyze_road_network(solution, game_tiles):
    all_roads = _build_all_roads(solution, game_tiles)
    
    agg_stats = {
        "total_roads": len(all_roads), "aliens_caught": 0, "max_aliens_running_towards_agent": 0,
        "max_hamburgers_in_front_of_alien": 0, "max_agents_on_one_road": 0, "max_aliens_on_one_road": 0,
        "max_aliens_between_two_agents": 0, "total_food_chain_sets": 0
    }
    
    road_lengths = []
    for road in all_roads:
        road_lengths.append(len(road))
        road_stats = _process_road_for_stats(road)
        if not road_stats: continue

        agg_stats["aliens_caught"] += road_stats.get('aliens_caught', 0)
        agg_stats["total_food_chain_sets"] += road_stats.get('food_chain_sets', 0)
        agg_stats["max_hamburgers_in_front_of_alien"] = max(agg_stats["max_hamburgers_in_front_of_alien"], road_stats.get('max_hamburgers_in_front_of_alien', 0))
        agg_stats["max_aliens_running_towards_agent"] = max(agg_stats["max_aliens_running_towards_agent"], road_stats.get('max_aliens_running_towards_agent', 0))
        agg_stats["max_agents_on_one_road"] = max(agg_stats["max_agents_on_one_road"], road_stats.get('num_agents', 0))
        agg_stats["max_aliens_on_one_road"] = max(agg_stats["max_aliens_on_one_road"], road_stats.get('num_aliens', 0))
        agg_stats["max_aliens_between_two_agents"] = max(agg_stats["max_aliens_between_two_agents"], road_stats.get('max_aliens_between_two_agents', 0))

    if road_lengths:
        agg_stats["longest_road_size"] = max(road_lengths) if road_lengths else 0
        agg_stats["max_roads_of_same_length"] = Counter(road_lengths).most_common(1)[0][1] if road_lengths else 0
    else:
        agg_stats.update({"longest_road_size": 0, "max_roads_of_same_length": 0})
        
    return agg_stats

# =============================================================================
# SECTION 3: FUNÇÕES DE ADJACÊNCIA
# =============================================================================

def find_largest_component_size(grid_properties, property_key):
    max_size, visited = 0, set()
    for r in range(3):
        for c in range(3):
            if grid_properties[r][c].get(property_key, 0) > 0 and (r, c) not in visited:
                current_size, q = 0, deque([(r, c)])
                visited.add((r, c))
                while q:
                    curr_r, curr_c = q.popleft()
                    current_size += 1
                    for dr, dc in [(0, 1), (0, -1), (1, 0), (-1, 0)]:
                        next_r, next_c = curr_r + dr, curr_c + dc
                        if 0 <= next_r < 3 and 0 <= next_c < 3 and (next_r, next_c) not in visited and \
                           grid_properties[next_r][next_c].get(property_key, 0) > 0:
                            visited.add((next_r, next_c))
                            q.append((next_r, next_c))
                max_size = max(max_size, current_size)
    return max_size

def calculate_adjacency_stats(solution, game_tiles):
    grid_properties = [[{} for _ in range(3)] for _ in range(3)]
    for r in range(3):
        for c in range(3):
            (piece, side, _) = solution[r][c]
            tile_data = game_tiles[piece][side]
            grid_properties[r][c] = {
                'dogs': tile_data.get('dogs', 0),
                'houses': tile_data.get('houses', 0),
                'citizens': tile_data.get('boys', 0) + tile_data.get('girls', 0),
                'is_safe': 1 if tile_data.get('aliens', 0) == 0 else 0
            }
    
    return {
        "largest_dog_group": find_largest_component_size(grid_properties, 'dogs'),
        "largest_house_group": find_largest_component_size(grid_properties, 'houses'),
        "largest_citizen_group": find_largest_component_size(grid_properties, 'citizens'),
        "largest_safe_zone_size": find_largest_component_size(grid_properties, 'is_safe') # ALTERADO
    }

# =============================================================================
# SECTION 4: FUNÇÃO PRINCIPAL AGREGADORA
# =============================================================================

def calculate_solution_stats(solution, game_tiles):
    stats = {f"total_{key}": 0 for key in STAT_KEYS}
    stats["total_tiles_without_roads"] = 0
    for r in range(3):
        for c in range(3):
            (piece, side, _) = solution[r][c]
            tile_data = game_tiles[piece][side]
            for key in STAT_KEYS:
                if key in tile_data: stats[f"total_{key}"] += tile_data[key]
            if not tile_data.get("roads"): stats["total_tiles_without_roads"] += 1
    
    road_stats = analyze_road_network(solution, game_tiles)
    stats['total_captured_aliens'] += road_stats.pop('aliens_caught', 0)
    stats.update(road_stats)

    stats["aliens_times_ufos"] = (stats["total_aliens"] - stats["total_captured_aliens"]) * stats["total_ufos"]
    stats["aliens_times_hamburgers"] = (stats["total_aliens"] - stats["total_captured_aliens"]) * stats["total_hamburgers"]
    stats["citizen_dog_pairs"] = min((stats["total_boys"]+stats["total_girls"]), stats["total_dogs"])

    adjacency_stats = calculate_adjacency_stats(solution, game_tiles)
    stats.update(adjacency_stats)
                    
    return stats