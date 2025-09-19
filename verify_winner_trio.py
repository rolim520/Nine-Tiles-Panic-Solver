# analyze_all_combinations_mem_safe.py
import sys
import json
import os
import re
import duckdb
import csv
from itertools import combinations, combinations_with_replacement

# Funções auxiliares (a maioria inalterada)
def find_latest_solution_file(directory, base_name="tiling_solutions", extension="parquet"):
    if not os.path.isdir(directory): return None, f"Error: Directory '{directory}' not found."
    pattern = re.compile(rf"{base_name}_(\d+)\.{extension}")
    highest_index = -1; latest_file_path = None
    for filename in os.listdir(directory):
        match = pattern.match(filename)
        if match:
            index = int(match.group(1))
            if index > highest_index:
                highest_index = index
                latest_file_path = os.path.join(directory, filename)
    if latest_file_path: return latest_file_path, None
    else: return None, f"Error: No solution file matching '{base_name}_*.{extension}' found in '{directory}'."

def find_undefeated_trios_sql(db_con, parquet_file, stat_keys, stat_types):
    """
    OTIMIZADO e SEGURO DE MEMÓRIA: Encontra a Fronteira de Pareto usando SQL diretamente no arquivo.
    """
    dominance_conditions = []
    strict_dominance_conditions = []
    for i in range(3):
        key = f'"{stat_keys[i]}"'
        op = '>=' if stat_types[i] == 'max' else '<='
        dominance_conditions.append(f"o.{key} {op} c.{key}")
        op_strict = '>' if stat_types[i] == 'max' else '<'
        strict_dominance_conditions.append(f"o.{key} {op_strict} c.{key}")

    where_clause = " AND ".join(dominance_conditions)
    strict_where_clause = " OR ".join(strict_dominance_conditions)

    columns_str = ', '.join([f'"{key}"' for key in stat_keys])
    
    query = f"""
    WITH distinct_trios AS (
        SELECT DISTINCT {columns_str}
        FROM read_parquet('{parquet_file}')
    )
    SELECT {columns_str}
    FROM distinct_trios c
    WHERE NOT EXISTS (
        SELECT 1
        FROM distinct_trios o
        WHERE ({where_clause}) AND ({strict_where_clause})
    );
    """
    return db_con.execute(query).fetchall()

def calculate_round_winner(match_trios, stat_types, challenger_trio):
    # ... (código inalterado) ...
    num_players = len(match_trios)
    player_scores = {i: 0 for i in range(num_players)}
    for i in range(3):
        objective_values = []
        for p_idx, trio in enumerate(match_trios):
            tie_breaker = 0 if trio == challenger_trio else 1
            sort_value = -trio[i] if stat_types[i] == 'max' else trio[i]
            objective_values.append(((sort_value, tie_breaker), p_idx))
        objective_values.sort(key=lambda x: x[0])
        points = num_players
        for _, p_idx in objective_values: player_scores[p_idx] += points; points -= 1
    if not player_scores: return []
    max_score = max(player_scores.values())
    winner_indices = [p_idx for p_idx, score in player_scores.items() if score == max_score]
    return [match_trios[p_idx] for p_idx in winner_indices]

def find_unbeatable_trio(pareto_frontier, stat_types, num_players, verbose=False):
    # ... (código inalterado) ...
    if not pareto_frontier: return None
    num_opponents = num_players - 1
    if num_opponents < 0: return None
    if num_opponents == 0: return pareto_frontier[0]
    champion = pareto_frontier[0]
    try:
        opponent_combos = combinations_with_replacement(pareto_frontier, num_opponents)
        for opponent_group in opponent_combos:
            match_trios = [champion] + list(opponent_group)
            winners = calculate_round_winner(match_trios, stat_types, champion)
            if not winners: return None
            champion = winners[0]
    except (TypeError, ValueError):
        if verbose: print("  -> Muitas combinações, pulando.")
        return None
    potential_unbeatable = champion
    try:
        opponent_combos = combinations_with_replacement(pareto_frontier, num_opponents)
        for opponent_group in opponent_combos:
            match_trios = [potential_unbeatable] + list(opponent_group)
            winners = calculate_round_winner(match_trios, stat_types, potential_unbeatable)
            if len(winners) != 1 or winners[0] != potential_unbeatable:
                return None
    except (TypeError, ValueError):
        if verbose: print("  -> Muitas combinações, pulando.")
        return None
    return potential_unbeatable

def run_analysis_for_combo(card_combo, num_players, db_con, parquet_file):
    card_names = [c['name'] for c in card_combo]
    stat_keys = [c['key'] for c in card_combo]
    stat_types = [c['type'] for c in card_combo]
    print(f"Analisando: {', '.join(card_names)}")

    pareto_frontier = find_undefeated_trios_sql(db_con, parquet_file, stat_keys, stat_types)
    
    if not pareto_frontier:
        print("  -> Fronteira de Pareto vazia.")
        return False, "Fronteira Vazia"
    
    print(f"  -> Fronteira de Pareto com {len(pareto_frontier)} trios.")
    unbeatable_trio = find_unbeatable_trio(pareto_frontier, stat_types, num_players)
    
    if unbeatable_trio:
        print(f"  -> 🏆 ENCONTRADO: {unbeatable_trio}")
        return True, unbeatable_trio
    else:
        print("  -> Nenhum trio invicto.")
        return False, "N/A"

def main():
    if len(sys.argv) != 2:
        print("Usage: python your_script_name.py <num_players>")
        sys.exit(1)
    
    try:
        num_players = int(sys.argv[1])
        if num_players < 2:
             print("Error: O número de jogadores deve ser no mínimo 2."); sys.exit(1)
    except ValueError:
        print("Error: O número de jogadores deve ser um inteiro."); sys.exit(1)

    try:
        with open('docs/data/cards.json', 'r') as f: all_cards = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Erro ao carregar 'docs/data/cards.json': {e}"); sys.exit(1)

    # MODIFICAÇÃO: Conecta ao DB mas NÃO carrega a tabela inteira
    parquet_file, error = find_latest_solution_file('generated_solutions')
    if error: print(error); sys.exit(1)
    
    db_con = duckdb.connect(database=':memory:')
    db_con.execute("PRAGMA memory_limit='16GB';") # O limite ainda é útil para consultas individuais
    db_con.execute(f"PRAGMA threads={os.cpu_count()};")
    
    valid_cards = [card for card in all_cards if card.get("number") != 4 and card.get("key")]
    output_filename = f"unbeatable_analysis_results_{num_players}p.csv"
    card_combinations = list(combinations(valid_cards, 3))
    total_combos = len(card_combinations)
    print(f"Iniciando análise para {total_combos} combinações de cartas para {num_players} jogadores.")
    print(f"Arquivo Parquet a ser analisado: {parquet_file}")
    print(f"Resultados serão salvos em: {output_filename}")

    with open(output_filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Card 1 Num', 'Card 1 Name', 'Card 2 Num', 'Card 2 Name', 
                         'Card 3 Num', 'Card 3 Name', 'Unbeatable Exists?', 'Unbeatable Trio'])

        for i, card_combo in enumerate(card_combinations):
            print(f"\n--- Processando Combinação {i+1}/{total_combos} ---")
            
            # Passa o caminho do arquivo em cada chamada
            exists, trio = run_analysis_for_combo(card_combo, num_players, db_con, parquet_file)
            
            row = [card_combo[0]['number'], card_combo[0]['name'],
                   card_combo[1]['number'], card_combo[1]['name'],
                   card_combo[2]['number'], card_combo[2]['name'],
                   exists, str(trio)]
            writer.writerow(row)
            
    db_con.close()
    print(f"\nAnálise completa! Resultados salvos em '{output_filename}'.")

if __name__ == "__main__":
    main()