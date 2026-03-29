# constants.py

# A map of (piece_position, local_connection) to a global_node_id (0-23)
TILE_NODES = (
    # --- Top Row ---
    # (North, East, South, West)
    (0, 4, 7, 3),    # Position 0
    (1, 5, 8, 4),    # Position 1
    (2, 6, 9, 5),    # Position 2

    # --- Middle Row ---
    (7, 11, 14, 10), # Position 3
    (8, 12, 15, 11), # Position 4
    (9, 13, 16, 12), # Position 5

    # --- Bottom Row ---
    (14, 18, 21, 17),# Position 6
    (15, 19, 22, 18),# Position 7
    (16, 20, 23, 19) # Position 8
)

# Stores the neighbour positions for any given position in the 3x3 grid
NEIGHBOURS = (
    (1, 3),       # 0
    (0, 2, 4),    # 1
    (1, 5),       # 2
    (0, 4, 6),    # 3
    (1, 3, 5, 7), # 4
    (2, 4, 8),    # 5
    (3, 7),       # 6
    (4, 6, 8),    # 7
    (5, 7)        # 8
)

# Direction constants
NORTH, EAST, SOUTH, WEST = 0, 1, 2, 3

# Other potential constants
GRID_SIZE = 3
NUM_NODES = 24