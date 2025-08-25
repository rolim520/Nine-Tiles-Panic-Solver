# analysis.py

from collections import Counter, deque
from constants import TILE_NODES

STAT_KEYS = [
    "houses", "ufos", "girls", "boys", "dogs", "hamburgers",
    "aliens", "agents", "captured_aliens", "curves"
]

def find_largest_component_size(grid_properties, property_key):
    """
    Finds the size of the largest connected group of tiles that share a property.
    Uses a Breadth-First Search (BFS) to find "islands" of connected tiles.
    """
    max_size = 0
    visited = set()

    for r in range(3):
        for c in range(3):
            if grid_properties[r][c][property_key] > 0 and (r, c) not in visited:
                
                current_size = 0
                # --- MODIFIED: Use a deque for an efficient queue ---
                q = deque([(r, c)]) 
                visited.add((r, c))

                while q:
                    # --- MODIFIED: Use popleft() which is faster than pop(0) ---
                    curr_r, curr_c = q.popleft() 
                    current_size += 1

                    for dr, dc in [(0, 1), (0, -1), (1, 0), (-1, 0)]:
                        next_r, next_c = curr_r + dr, curr_c + dc
                        
                        if 0 <= next_r < 3 and 0 <= next_c < 3 and \
                           (next_r, next_c) not in visited and \
                           grid_properties[next_r][next_c][property_key] > 0:
                            
                            visited.add((next_r, next_c))
                            q.append((next_r, next_c))
                
                max_size = max(max_size, current_size)

    return max_size

def calculate_adjacency_stats(solution, game_tiles):
    """
    Calculates statistics based on the largest connected group for each property.
    """
    # Step 1: Pre-process the grid to create a simple property map.
    # This part is the same as before.
    grid_properties = [[{} for _ in range(3)] for _ in range(3)]
    for r in range(3):
        for c in range(3):
            (piece, side, _) = solution[r][c]
            tile_data = game_tiles[piece][side]
            grid_properties[r][c] = {
                'dogs': tile_data.get('dogs', 0),
                'houses': tile_data.get('houses', 0),
                'aliens': tile_data.get('aliens', 0),
                'citizens': tile_data.get('boys', 0) + tile_data.get('girls', 0)
            }

    # Step 2: Call the component-finding function for each property
    stats = {
        "largest_dog_group": find_largest_component_size(grid_properties, 'dogs'),
        "largest_house_group": find_largest_component_size(grid_properties, 'houses'),
        "largest_alien_group": find_largest_component_size(grid_properties, 'aliens'),
        "largest_citizen_group": find_largest_component_size(grid_properties, 'citizens'),
    }

    return stats


def analyze_road_network(solution, game_tiles):
    # ... (this function is unchanged)
    adj = {i: [] for i in range(24)}
    for r in range(3):
        for c in range(3):
            (piece, side, orientation) = solution[r][c]
            position = r * 3 + c
            
            for road in game_tiles[piece][side]["roads"]:
                local_conn1, local_conn2 = road['connection']
                global_id1 = TILE_NODES[position][(local_conn1 + orientation) % 4]
                global_id2 = TILE_NODES[position][(local_conn2 + orientation) % 4]
                
                adj[global_id1].append(global_id2)
                adj[global_id2].append(global_id1)

    visited = set()
    road_lengths = []
    
    for i in range(24):
        if i not in visited and adj[i]:
            component_nodes = set()
            q = [i]
            visited.add(i)
            
            head = 0
            while head < len(q):
                u = q[head]
                head += 1
                component_nodes.add(u)
                for v in adj[u]:
                    if v not in visited:
                        visited.add(v)
                        q.append(v)
            
            edge_sum_in_component = sum(len(adj[node]) for node in component_nodes)
            road_length = edge_sum_in_component // 2
            road_lengths.append(road_length)
            
    if not road_lengths:
        return {
            "total_roads": 0,
            "longest_road_size": 0,
            "max_roads_of_same_length": 0
        }
    
    road_length_counts = Counter(road_lengths)
    
    return {
        "total_roads": len(road_lengths),
        "longest_road_size": max(road_lengths),
        "max_roads_of_same_length": max(road_length_counts.values())
    }


def calculate_solution_stats(solution, game_tiles):
    """
    Calculates all aggregate statistics for a complete 3x3 tiling solution.
    """
    # Part 1: Simple counting stats
    stats = {f"total_{key}": 0 for key in STAT_KEYS}
    stats["total_tiles_without_roads"] = 0

    for r in range(3):
        for c in range(3):
            (piece, side, _) = solution[r][c]
            tile_data = game_tiles[piece][side]
            
            for key in STAT_KEYS:
                if key in tile_data:
                    stats[f"total_{key}"] += tile_data[key]
            
            if not tile_data["roads"]:
                stats["total_tiles_without_roads"] += 1
    
    # Part 2: Road network analysis
    road_stats = analyze_road_network(solution, game_tiles)
    stats.update(road_stats)

    # --- Part 3: NEW - Adjacency analysis ---
    adjacency_stats = calculate_adjacency_stats(solution, game_tiles)
    stats.update(adjacency_stats)
                    
    return stats