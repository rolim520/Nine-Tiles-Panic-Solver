tile_nodes = [
    # --- Peças da Linha de Cima (0, 1, 2) ---
    # [Esq, Cima, Dir, Baixo]
    [3, 0, 4, 7],    # Peça 0
    [4, 1, 5, 8],    # Peça 1
    [5, 2, 6, 9],    # Peça 2

    # --- Peças da Linha do Meio (3, 4, 5) ---
    # [Esq, Cima, Dir, Baixo]
    [10, 7, 11, 14], # Peça 3
    [11, 8, 12, 15], # Peça 4
    [12, 9, 13, 16], # Peça 5

    # --- Peças da Linha de Baixo (6, 7, 8) ---
    # [Esq, Cima, Dir, Baixo]
    [17, 14, 18, 21],# Peça 6
    [18, 15, 19, 22],# Peça 7
    [19, 16, 20, 23] # Peça 8
]

def have_road_cycles(tiling, game_tiles):
    """
    Verifica se há ciclos em um tiling, otimizada para nós de 0 a 23.
    """
    # 1. Inicializa a estrutura Union-Find para 24 nós (índices 0-23)
    num_nos = 24
    parent = list(range(num_nos))  # Cria uma lista de 0 a 23

    def find(i):
        if parent[i] == i:
            return i
        parent[i] = find(parent[i])
        return parent[i]

    def union(i, j):
        root_i = find(i)
        root_j = find(j)
        if root_i != root_j:
            parent[root_i] = root_j

    # 2. Itera sobre as peças do tiling
    for i in range(3):
        for j in range(3):
            (piece, side, orientation) = tiling[i][j]
            # Calcula a POSIÇÃO da peça (0-8)
            piece_position = i * 3 + j
            for road in game_tiles[piece][side]["roads"]:
                local_conn1, local_conn2 = road['connection']
                
                # Usa a nova matriz tile_nodes
                global_id1 = tile_nodes[piece_position][(local_conn1 + orientation) % 4]
                global_id2 = tile_nodes[piece_position][(local_conn2 + orientation) % 4]

                # 3. Executa a verificação Union-Find
                root1 = find(global_id1)
                root2 = find(global_id2)

                if root1 == root2:
                    return True  # Ciclo detectado!
                
                union(global_id1, global_id2)

    # 4. Se o loop terminar, não há ciclos
    return False