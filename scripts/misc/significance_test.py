import json
import numpy as np
from scipy.stats import chi2_contingency

with open('results/montecarlo_results.json', 'r') as f:
    results = json.load(f)

n_simulations = results["metadata"]["simulations"]

vit_produto = results["group_test"]["produto"]["wins"]
vit_soma = results["group_test"]["soma"]["wins"]

nao_vit_produto = n_simulations - vit_produto
nao_vit_soma = n_simulations - vit_soma

# Monta a Tabela de Contingência (Matriz 2x2)
# Linhas: Ganhou / Não Ganhou
# Colunas: Produto / Soma
tabela = np.array([
    [vit_produto, vit_soma],
    [nao_vit_produto, nao_vit_soma]
])

# Executa o teste estatístico
chi2, p_valor, graus_liberdade, esperados = chi2_contingency(tabela)

print(f"Vitórias Produto: {vit_produto}")
print(f"Vitórias Soma:    {vit_soma}")
print(f"Diferença:        {vit_produto - vit_soma} vitórias")
print("-" * 30)
print(f"P-Valor calculado: {p_valor:.5f}")

if p_valor < 0.05:
    print("LAUDO: SIGNIFICÂNCIA ESTATÍSTICA ALCANÇADA!")
    print("Você pode afirmar na defesa que o Produto Ponderado é superior.")
else:
    print("LAUDO: EMPATE TÉCNICO.")
    print("A diferença é estatisticamente irrelevante. Escreva no TCC que houve empate técnico.")