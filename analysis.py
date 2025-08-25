# analysis.py

from collections import Counter
# We need the TILE_NODES map to build the graph
from constants import TILE_NODES

STAT_KEYS = [
    "houses", "ufos", "girls", "boys", "dogs", "hamburgers",
    "aliens", "agents", "captured_aliens", "curves"
]

def analyze_road_network(solution, game_tiles):
    """
    Analyzes the road network of a solution by building and traversing a graph.
    """
    # 1. Build an adjacency list to represent the graph of all 24 connection nodes.
    adj = {i: [] for i in range(24)}
    for r in range(3):
        for c in range(3):
            (piece, side, orientation) = solution[r][c]
            position = r * 3 + c
            
            for road in game_tiles[piece][side]["roads"]:
                local_conn1, local_conn2 = road['connection']
                global_id1 = TILE_NODES[position][(local_conn1 + orientation) % 4]
                global_id2 = TILE_NODES[position][(local_conn2 + orientation) % 4]
                
                # Add a connection (an edge) between the two nodes
                adj[global_id1].append(global_id2)
                adj[global_id2].append(global_id1)

    # 2. Traverse the graph to find all distinct roads and their lengths.
    visited = set()
    road_lengths = []
    
    for i in range(24): # Iterate through all possible nodes
        # If a node is part of a road and we haven't visited it yet,
        # it must be the start of a new, distinct road.
        if i not in visited and adj[i]:
            
            component_nodes = set() # Nodes in this specific road
            q = [i] # Queue for Breadth-First Search (BFS)
            visited.add(i)
            
            # Perform BFS to find all connected nodes in this road
            head = 0
            while head < len(q):
                u = q[head]
                head += 1
                component_nodes.add(u)
                for v in adj[u]:
                    if v not in visited:
                        visited.add(v)
                        q.append(v)
            
            # The length of a road is the number of edges (tile segments) in it.
            # In a graph component, this is the sum of degrees / 2.
            edge_sum_in_component = sum(len(adj[node]) for node in component_nodes)
            road_length = edge_sum_in_component // 2
            road_lengths.append(road_length)
            
    # 3. Calculate the final statistics from the list of found road lengths.
    if not road_lengths:
        return {
            "total_roads": 0,
            "longest_road_size": 0,
            "max_roads_of_same_length": 0 # New stat name
        }
    
    road_length_counts = Counter(road_lengths)
    
    return {
        "total_roads": len(road_lengths),
        "longest_road_size": max(road_lengths),
        # --- MODIFIED: Find the highest value from the counts instead of returning the dict ---
        "max_roads_of_same_length": max(road_length_counts.values())
    }


def calculate_solution_stats(solution, game_tiles):
    """
    Calculates all aggregate statistics for a complete 3x3 tiling solution.
    """
    # --- Part 1: Simple counting stats (existing logic) ---
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
    
    # --- Part 2: NEW - Road network analysis ---
    road_stats = analyze_road_network(solution, game_tiles)
    
    # Merge the two dictionaries of stats together
    stats.update(road_stats)
                    
    return stats