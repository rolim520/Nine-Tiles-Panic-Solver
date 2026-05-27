import json
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.patches as mpatches
import os

os.makedirs('images', exist_ok=True)

plt.rcParams.update({
    'font.size': 12,
    'axes.titlesize': 13, 
    'axes.labelsize': 12,
    'grid.alpha': 0.5,
    'figure.autolayout': True
})

COLOR_WIN = '#4a90e2'   
COLOR_TIE = '#e74c3c'   
COLOR_LOSS = '#e5e7e9'  

print("📊 Lendo dados da simulação...")
with open('results/montecarlo_results.json', 'r') as f:
    data = json.load(f)

# =========================================================================
# Produto Ponderado vs Oponentes Isolados
# =========================================================================
print("🎨 Gerando gráfico 1: Produto vs Oponentes Isolados...")
fig1, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5.5), sharey=True)

players = [2, 3, 4, 5]
p_keys = ['players_2', 'players_3', 'players_4', 'players_5']
x1 = np.arange(len(players))
width1 = 0.35 

oponentes = ['vs_random', 'vs_greedy']
eixos = [ax1, ax2]
titulos = ['Produto Ponderado vs Oponentes Aleatórios', 'Produto Ponderado vs Oponentes Gulosos']

for idx_op, op in enumerate(oponentes):
    ax = eixos[idx_op]
    
    for i, p in enumerate(p_keys):
        df = data['individual_tests']['produto'][op][p]['finish_first']
        tot_f = df['wins'] + df['ties'] + df['losses']
        wf, tf, lf = (df['wins']/tot_f)*100, (df['ties']/tot_f)*100, (df['losses']/tot_f)*100
        
        dl = data['individual_tests']['produto'][op][p]['finish_last']
        tot_l = dl['wins'] + dl['ties'] + dl['losses']
        wl, tl, ll = (dl['wins']/tot_l)*100, (dl['ties']/tot_l)*100, (dl['losses']/tot_l)*100

        pos_first = x1[i] - width1/2 - 0.02
        pos_last = x1[i] + width1/2 + 0.02

        ax.bar(pos_first, wf, width1, color=COLOR_WIN, edgecolor='black', linewidth=1)
        ax.bar(pos_first, tf, width1, bottom=wf, color=COLOR_TIE, edgecolor='black', linewidth=1)
        ax.bar(pos_first, lf, width1, bottom=wf+tf, color=COLOR_LOSS, edgecolor='black', linewidth=1)
        
        ax.bar(pos_last, wl, width1, color=COLOR_WIN, edgecolor='black', linewidth=1)
        ax.bar(pos_last, tl, width1, bottom=wl, color=COLOR_TIE, edgecolor='black', linewidth=1)
        ax.bar(pos_last, ll, width1, bottom=wl+tl, color=COLOR_LOSS, edgecolor='black', linewidth=1)

        if wf > 10: ax.text(pos_first, wf/2, f'{wf:.1f}%', ha='center', va='center', color='white', fontweight='bold', fontsize=8)
        if wl > 10: ax.text(pos_last, wl/2, f'{wl:.1f}%', ha='center', va='center', color='white', fontweight='bold', fontsize=8)
        
        if tf > 5: ax.text(pos_first, wf + tf/2, f'{tf:.1f}%', ha='center', va='center', color='white', fontweight='bold', fontsize=8)
        if tl > 5: ax.text(pos_last, wl + tl/2, f'{tl:.1f}%', ha='center', va='center', color='white', fontweight='bold', fontsize=8)

        ax.text(pos_first, -2.5, '1º', ha='center', va='top', fontsize=10)
        ax.text(pos_last, -2.5, f'{players[i]}º', ha='center', va='top', fontsize=10)

    ax.set_xticks(x1)
    ax.set_xticklabels([f'{p} Jog.' for p in players], fontweight='bold')
    ax.tick_params(axis='x', pad=22) 
    
    ax.set_title(titulos[idx_op])
    ax.set_ylim(0, 105)
    ax.grid(axis='y', linestyle='--')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

ax1.set_ylabel('Taxa de Ocorrência (%)', fontweight='bold')
ax1.yaxis.set_major_formatter(ticker.PercentFormatter())

win_patch = mpatches.Patch(facecolor=COLOR_WIN, edgecolor='black', label='Vitória')
tie_patch = mpatches.Patch(facecolor=COLOR_TIE, edgecolor='black', label='Empate')
loss_patch = mpatches.Patch(facecolor=COLOR_LOSS, edgecolor='black', label='Derrota')

fig1.legend(handles=[win_patch, tie_patch, loss_patch], loc='upper center', bbox_to_anchor=(0.5, 0.01), ncol=3)
fig1.subplots_adjust(bottom=0.24, top=0.93)

plt.figure(fig1.number)
plt.savefig('images/grafico_produto_vs_ambos.pdf', dpi=300, bbox_inches='tight')
plt.close(fig1)  # Fecha a figura para liberar a memória
print("✅ Salvo: images/grafico_produto_vs_ambos.pdf")


# =========================================================================
# Cenário Misto (Todos contra Todos)
# =========================================================================
print("🎨 Gerando gráfico 2: Cenário Misto...")
fig2, ax_mix = plt.subplots(figsize=(10, 6))

agents = ['random', 'guloso', 'minimo', 'soma', 'produto']
labels = ['Aleatório', 'Guloso', 'Mínimo', 'Soma', 'Produto']

wins = []
ties = []
losses = []

for a in agents:
    d = data['group_test'][a]
    tot = d['wins'] + d['ties'] + d['losses']
    wins.append((d['wins'] / tot) * 100)
    ties.append((d['ties'] / tot) * 100)
    losses.append((d['losses'] / tot) * 100)

x2 = np.arange(len(agents))
width2 = 0.55

b_ties = np.array(wins)
b_losses = b_ties + np.array(ties)

ax_mix.bar(x2, wins, width2, color=COLOR_WIN, edgecolor='black', linewidth=1)
ax_mix.bar(x2, ties, width2, bottom=b_ties, color=COLOR_TIE, edgecolor='black', linewidth=1)
ax_mix.bar(x2, losses, width2, bottom=b_losses, color=COLOR_LOSS, edgecolor='black', linewidth=1)

for i in range(len(agents)):
    if wins[i] > 5:
        ax_mix.text(x2[i], wins[i]/2, f'{wins[i]:.1f}%', ha='center', va='center', color='white', fontweight='bold')
    if ties[i] > 5:
        ax_mix.text(x2[i], wins[i] + ties[i]/2, f'{ties[i]:.1f}%', ha='center', va='center', color='white', fontweight='bold')

ax_mix.set_xticks(x2)
ax_mix.set_xticklabels(labels, fontweight='bold', fontsize=13)
ax_mix.set_title('Desempenho no Cenário Misto (Todos contra Todos)', pad=15)
ax_mix.set_ylabel('Taxa de Ocorrência (%)', fontweight='bold')
ax_mix.set_ylim(0, 105)
ax_mix.yaxis.set_major_formatter(ticker.PercentFormatter())
ax_mix.grid(axis='y', linestyle='--')

ax_mix.spines['top'].set_visible(False)
ax_mix.spines['right'].set_visible(False)

fig2.legend(handles=[win_patch, tie_patch, loss_patch], loc='lower center', bbox_to_anchor=(0.5, -0.05), ncol=3)

plt.figure(fig2.number)
plt.savefig('images/grafico_cenario_misto.pdf', bbox_inches='tight')
plt.close(fig2)
print("✅ Salvo: images/grafico_cenario_misto.pdf")
print("🎉 Processo concluído com sucesso!")