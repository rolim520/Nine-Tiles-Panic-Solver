import numpy as np
import pandas as pd
import duckdb
import json
import os
import random
import itertools

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
SOURCE_SOLUTIONS_DIR = 'generated_solutions'
PERCENTILES_JSON_PATH = 'docs/data/percentiles.json'
CARDS_JSON_PATH = 'game/cards/cards.json'
OUTPUT_JSON_PATH = 'results/montecarlo_results.json'

N_SIMULATIONS = 100000 
SAMPLE_UNIVERSE_SIZE = 1000000
TOLERANCE = 1e-5

# =============================================================================
# FUNÇÕES DE APOIO
# =============================================================================
def apply_percentiles(df, percentiles_dict, all_keys):
    df_perc = pd.DataFrame(index=df.index)
    for key in all_keys:
        mapping = {float(k): float(v) for k, v in percentiles_dict.get(key, {}).items()}
        df_perc[key] = df[key].astype(float).map(mapping).fillna(0.0)
    return df_perc

def calculate_round_points(player_scores, tie_breakers):
    num_players = len(player_scores)
    total_points = [0] * num_players 
    
    for obj_idx in range(3):
        scores = [player[obj_idx] for player in player_scores]
        ranked = sorted(zip(scores, tie_breakers, range(num_players)), 
                        key=lambda x: (-x[0], x[1]))
        for rank, (_, _, player_idx) in enumerate(ranked):
            total_points[player_idx] += (num_players - rank)
    return total_points

def determine_outcome(target_idx, all_points):
    target_points = all_points[target_idx]
    max_points = max(all_points) 
    
    if target_points < max_points: return 'losses'
    if all_points.count(max_points) == 1: return 'wins'
    return 'ties'

# =============================================================================
# LOOP PRINCIPAL
# =============================================================================
def main():
    print("🚀 Iniciando Preparação da Simulação...")
    
    with open(PERCENTILES_JSON_PATH, 'r') as f:
        percentiles_dict = json.load(f)
    with open(CARDS_JSON_PATH, 'r') as f:
        cards_data = json.load(f)
        
    ALL_KEYS = list(percentiles_dict.keys())
    valid_cards = [card for card in cards_data if card.get('key') and card.get('type')]
    
    con = duckdb.connect()
    parquet_files = sorted([f for f in os.listdir(SOURCE_SOLUTIONS_DIR) if f.endswith('.parquet')])
    parquet_path = os.path.join(SOURCE_SOLUTIONS_DIR, parquet_files[-1])
    print(f"📂 Lendo dados base de: {parquet_path}")
    
    columns_to_select = ", ".join(ALL_KEYS)
    query = f"""
        SELECT {columns_to_select}
        FROM read_parquet('{parquet_path}') 
        USING SAMPLE {SAMPLE_UNIVERSE_SIZE}
    """
    df_universe = con.execute(query).fetchdf()
    con.close()
    
    df_perc = apply_percentiles(df_universe, percentiles_dict, ALL_KEYS)
    
    card_matrix = np.zeros((len(df_perc), len(valid_cards)))
    for i, card in enumerate(valid_cards):
        raw_scores = df_perc[card['key']].values
        if card['type'] == 'min':
            card_matrix[:, i] = 100.0 - raw_scores
        else:
            card_matrix[:, i] = raw_scores
            
    all_indices_pool = list(range(len(card_matrix)))
    
    metrics = ['soma', 'produto', 'minimo']
    
    # =========================================================================
    # FASE 0: O SUPER CACHE 
    # =========================================================================
    print("🧠 Construindo a Biblioteca Mestra (Busca Direta em Tuplas Únicas)...")
    master_library = {}
    all_possible_combos = list(itertools.combinations(range(len(valid_cards)), 3))
    
    for combo in all_possible_combos:
        c = card_matrix[:, combo]
        
        unique_c, unique_indices = np.unique(c, axis=0, return_index=True)
        
        soma_arr = np.sum(unique_c, axis=1)
        prod_arr = np.prod(unique_c, axis=1)
        mini_arr = np.min(unique_c, axis=1)
        
        master_library[combo] = {}
        for m_name, m_array in zip(metrics, [soma_arr, prod_arr, mini_arr]):
            max_val = m_array.max()
            local_bests = np.where(m_array >= max_val - TOLERANCE)[0]
            global_bests = unique_indices[local_bests]
            master_library[combo][m_name] = {'bests_array': global_bests}

    # =========================================================================
    # PRÉ-CÁLCULO DOS CENÁRIOS
    # =========================================================================
    print(f"⚙️ Sorteando variáveis para {N_SIMULATIONS} rodadas...")
    
    rounds_cards = [tuple(sorted(random.sample(range(len(valid_cards)), 3))) for _ in range(N_SIMULATIONS)]
    
    # Oponentes Aleatórios
    rounds_opponents_random = [random.sample(all_indices_pool, 5) for _ in range(N_SIMULATIONS)]
    
    # --- NOVO CACHE: Oponentes Gulosos ---
    print("🤖 Construindo catálogo de Agentes Gulosos...")
    best_gulosos_per_card = {}
    
    for card_idx in range(len(valid_cards)):
        max_val = card_matrix[:, card_idx].max()
        bests = np.where(card_matrix[:, card_idx] >= max_val - TOLERANCE)[0]
        best_gulosos_per_card[card_idx] = bests

    rounds_opponents_greedy = []
    for i in range(N_SIMULATIONS):
        greedy_players_for_round = []
        for _ in range(5): 
            target_card = random.choice(rounds_cards[i]) 
            greedy_players_for_round.append(random.choice(best_gulosos_per_card[target_card]))
            
        rounds_opponents_greedy.append(greedy_players_for_round)
    # ---------------------------------------
    
    best_boards = {m: np.zeros(N_SIMULATIONS, dtype=int) for m in metrics}
    
    for sim_idx, card_idx_trio in enumerate(rounds_cards):
        combo_cache = master_library[card_idx_trio]
        for m_name in metrics:
            metric_data = combo_cache[m_name]
            best_boards[m_name][sim_idx] = random.choice(metric_data['bests_array'])

    # =========================================================================
    # INICIANDO OS CAMPEONATOS
    # =========================================================================
    
    results = {
        "metadata": {"simulations": N_SIMULATIONS},
        "individual_tests": {
            agent: {
                "vs_random": {f"players_{n}": {"finish_first": {"wins":0, "ties":0, "losses":0}, "finish_last": {"wins":0, "ties":0, "losses":0}} for n in [2, 3, 4, 5]},
                "vs_greedy": {f"players_{n}": {"finish_first": {"wins":0, "ties":0, "losses":0}, "finish_last": {"wins":0, "ties":0, "losses":0}} for n in [2, 3, 4, 5]}
            } for agent in metrics
        },
        "group_test": {m: {"wins":0, "ties":0, "losses":0} for m in metrics + ['guloso', 'random']}
    }

    print("🎲 Iniciando Torneios Isolados...")
    for agent in metrics:
        for opp_type, opp_pool in [("vs_random", rounds_opponents_random), ("vs_greedy", rounds_opponents_greedy)]:
            for n_players in [2, 3, 4, 5]:
                for tie_advantage in ["finish_first", "finish_last"]:
                    
                    base_other_orders = list(range(2, n_players + 1)) if tie_advantage == "finish_first" else list(range(1, n_players))
                    
                    for sim_idx in range(N_SIMULATIONS):
                        cards_idx = rounds_cards[sim_idx]
                        p1_idx = best_boards[agent][sim_idx]
                        p_others_idxs = opp_pool[sim_idx][:n_players - 1]
                        
                        round_scores = [card_matrix[p1_idx, cards_idx]] + [card_matrix[i, cards_idx] for i in p_others_idxs]
                        
                        other_orders = base_other_orders.copy()
                        random.shuffle(other_orders)
                        tie_breakers = [1] + other_orders if tie_advantage == "finish_first" else [n_players] + other_orders
                            
                        points = calculate_round_points(round_scores, tie_breakers)
                        outcome = determine_outcome(0, points)
                        results["individual_tests"][agent][opp_type][f"players_{n_players}"][tie_advantage][outcome] += 1

    print("⚔️ Simulando Battle Royale (Mesa de 5: Soma x Produto x Minimo x Guloso x Random)...")
    for sim_idx in range(N_SIMULATIONS):
        cards_idx = rounds_cards[sim_idx]
        
        round_scores = [
            card_matrix[best_boards['soma'][sim_idx], cards_idx],
            card_matrix[best_boards['produto'][sim_idx], cards_idx],
            card_matrix[best_boards['minimo'][sim_idx], cards_idx],
            card_matrix[rounds_opponents_greedy[sim_idx][0], cards_idx], # 1 Guloso
            card_matrix[rounds_opponents_random[sim_idx][0], cards_idx]  # 1 Aleatório
        ]
        
        tie_breakers = list(range(1, 6)) 
        random.shuffle(tie_breakers)
        
        points = calculate_round_points(round_scores, tie_breakers)
        
        agents = ['soma', 'produto', 'minimo', 'guloso', 'random']
        for i, agent in enumerate(agents):
            outcome = determine_outcome(i, points)
            results["group_test"][agent][outcome] += 1

    with open(OUTPUT_JSON_PATH, 'w') as f:
        json.dump(results, f, indent=4)
        
    print(f"\n🎉 Simulação concluída! Resultados salvos em '{OUTPUT_JSON_PATH}'")

if __name__ == "__main__":
    main()