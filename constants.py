# constants.py

# A map of (piece_position, local_connection) to a global_node_id (0-23)
# This defines the static topology of the 3x3 grid.
TILE_NODES = [
    # --- Top Row (positions 0, 1, 2) ---
    # [Left, Top, Right, Bottom] connections for this grid spot
    [3, 0, 4, 7],    # Position 0
    [4, 1, 5, 8],    # Position 1
    [5, 2, 6, 9],    # Position 2

    # --- Middle Row (positions 3, 4, 5) ---
    [10, 7, 11, 14], # Position 3
    [11, 8, 12, 15], # Position 4
    [12, 9, 13, 16], # Position 5

    # --- Bottom Row (positions 6, 7, 8) ---
    [17, 14, 18, 21],# Position 6
    [18, 15, 19, 22],# Position 7
    [19, 16, 20, 23] # Position 8
]

# Other potential constants
GRID_SIZE = 3
NUM_NODES = 24