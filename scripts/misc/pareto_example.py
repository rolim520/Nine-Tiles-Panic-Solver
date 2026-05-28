import json
import os

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
PARETO_FILE = 'docs/data/pareto_front.json'
COMBO_KEY = "3_10_16"  # Cartas: 3 (Dogs), 10 (Roads), 16 (Hamburgers)

def main():
    if not os.path.exists(PARETO_FILE):
        print(f"❌ Arquivo não encontrado: {PARETO_FILE}")
        return
        
    with open(PARETO_FILE, 'r') as f:
        pareto_data = json.load(f)
        
    if COMBO_KEY not in pareto_data:
        print(f"❌ Chave {COMBO_KEY} não encontrada no arquivo.")
        return
        
    solutions = pareto_data[COMBO_KEY]
    tuples_list =[]
    
    for sol in solutions:
        s = sol["scores"]
        
        f_x = (s[1], s[2], s[0])
        tuples_list.append(f_x)
        
    tuples_list = sorted(list(set(tuples_list)), reverse=True)
    
    print(f"✅ Encontradas {len(tuples_list)} tuplas únicas na Fronteira de Pareto!")
    print("F(x) = (Estradas, Hambúrgueres, Cachorros)")
    print("-" * 50)
    
    for t in tuples_list:
        print(f"        {t[0]} & {t[1]} & {t[2]} \\\\")

if __name__ == "__main__":
    main()