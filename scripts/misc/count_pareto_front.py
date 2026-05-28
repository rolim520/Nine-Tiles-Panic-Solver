import json
import os

def contar_tabuleiros_pareto(filepath):
    if not os.path.exists(filepath):
        print(f"❌ Arquivo '{filepath}' não encontrado.")
        return

    print("📂 Lendo fronteira de Pareto...")
    with open(filepath, 'r', encoding='utf-8') as f:
        pareto_lib = json.load(f)

    totais = {1: 0, 2: 0, 3: 0}

    for combo_key, lista_tabuleiros in pareto_lib.items():
        num_cartas = len(combo_key.split('_'))
        if num_cartas in totais:
            totais[num_cartas] += len(lista_tabuleiros)

    print("\n📊 TOTAL DE TABULEIROS POR NÚMERO DE CARTAS:")
    print("-" * 38)
    for size in sorted(totais.keys()):
        print(f"  {size} carta(s): {totais[size]:>10,}")
    print("-" * 38)
    print(f"  {'TOTAL GERAL':<15} {sum(totais.values()):>10,}")

if __name__ == "__main__":
    contar_tabuleiros_pareto('docs/data/pareto_front.json')
