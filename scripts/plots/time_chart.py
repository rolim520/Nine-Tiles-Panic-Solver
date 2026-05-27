import json
import os
import matplotlib.pyplot as plt
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import matplotlib.image as mpimg

os.makedirs('images', exist_ok=True)

print("📊 Lendo dados de execução...")
with open('results/gantt_chart_data.json', 'r') as f:
    data = json.load(f)

task_mapping = {
    0: (4, 0), 1: (5, 1), 2: (3, 0), 3: (2, 1), 4: (0, 1), 5: (6, 0),
    6: (1, 0), 7: (8, 1), 8: (1, 1), 9: (0, 0), 10: (3, 1), 11: (5, 0),
    12: (2, 0), 13: (7, 1), 14: (7, 0), 15: (4, 1), 16: (8, 0), 17: (6, 1)
}

data = sorted(data, key=lambda x: x["Duration"], reverse=True)
ordered_tasks = [task_mapping[d["Task"]] for d in data]
tempos_horas = [d["Duration"] / 3600 for d in data]

print("🎨 Gerando o gráfico...")

fig, ax = plt.subplots(figsize=(14, 7))
x_pos = range(len(tempos_horas))
barras = ax.bar(x_pos, tempos_horas, color="#4A90E2", edgecolor="black", width=0.6)

ax.set_xticks(x_pos)
ax.set_xticklabels([])

ax.set_ylabel("Tempo de Execução (Horas)", fontsize=12, fontweight='bold')
ax.set_title("Tempo de Execução das Subárvores de Busca", fontsize=14, fontweight='bold', pad=15)
ax.grid(axis='y', linestyle='--', alpha=0.7)

for index, value in enumerate(tempos_horas):
    ax.text(index, value + 0.15, f'{value:.1f}h', ha='center', va='bottom', fontsize=10)

caminho_imagens = "docs/assets/tile_images/"

for i, (peca, face) in enumerate(ordered_tasks):
    image_path = f"{caminho_imagens}{face}_{peca}.webp"
    
    try:
        img = mpimg.imread(image_path)
        imagebox = OffsetImage(img, zoom=0.075)
        
        ab = AnnotationBbox(imagebox, (i, 0), frameon=True, 
                            xybox=(0, -28), xycoords='data', boxcoords="offset points")
        ax.add_artist(ab)
    except FileNotFoundError:
        print(f"⚠️ Imagem não encontrada: {image_path}")

ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

plt.subplots_adjust(bottom=0.15, left=0.06, right=0.98, top=0.9)

output_path = "images/grafico_tarefas_vertical.pdf"
plt.savefig(output_path, format='pdf')
print(f"✅ Salvo: {output_path}")