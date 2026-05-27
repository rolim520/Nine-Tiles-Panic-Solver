import os
import re
import json
import duckdb
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# =============================================================================
# CONFIGURAÇÕES DE CAMINHOS
# =============================================================================
SOURCE_SOLUTIONS_DIR = 'generated_solutions'
PERCENTILES_JSON_PATH = 'docs/data/percentiles.json' 
OUTPUT_IMAGE_1 = 'images/longest_road_freq_relativa.pdf'
OUTPUT_IMAGE_2 = 'images/longest_road_fda.pdf'

plt.rcParams.update({
    'font.size': 12,
    'axes.titlesize': 14,
    'axes.labelsize': 12,
    'grid.alpha': 0.5,
    'figure.autolayout': True
})

def find_latest_solution_file(directory, base_name="tiling_solutions", extension="parquet"):
    if not os.path.isdir(directory): return None
    pattern = re.compile(rf"{base_name}_(\d+)\.{extension}")
    highest_index = -1
    latest_file_path = None
    for filename in os.listdir(directory):
        match = pattern.match(filename)
        if match:
            index = int(match.group(1))
            if index > highest_index:
                highest_index = index
                latest_file_path = os.path.join(directory, filename)
    return latest_file_path

def main():
    print("📊 Gerando gráficos separados para publicação...")
    os.makedirs('images', exist_ok=True)
    
    parquet_file = find_latest_solution_file(SOURCE_SOLUTIONS_DIR)
    if not parquet_file:
        print("❌ Erro: Parquet não encontrado.")
        return

    if not os.path.exists(PERCENTILES_JSON_PATH):
        print(f"❌ Erro: JSON de percentis não encontrado em {PERCENTILES_JSON_PATH}")
        return

    print("Consultando o banco de dados...")
    con = duckdb.connect()
    query = f"""
        SELECT 
            longest_road_size AS metric_value, 
            CAST(count(*) AS DOUBLE) AS frequency
        FROM read_parquet('{parquet_file}')
        GROUP BY longest_road_size
        ORDER BY longest_road_size
    """
    df_freq = con.execute(query).fetchdf()
    total_solutions = df_freq['frequency'].sum()
    df_freq['relative_frequency'] = (df_freq['frequency'] / total_solutions) * 100

    with open(PERCENTILES_JSON_PATH, 'r') as f:
        percentiles_data = json.load(f)
    
    longest_road_data = percentiles_data.get('longest_road_size', {})
    x_percentile = sorted([int(k) for k in longest_road_data.keys()])
    y_percentile = [longest_road_data[str(k)] for k in x_percentile]

    # =========================================================================
    # GRÁFICO 1: Distribuição de Frequência Relativa
    # =========================================================================
    plt.figure(figsize=(7, 5))
    plt.bar(df_freq['metric_value'], df_freq['relative_frequency'], color='#4a90e2', edgecolor='black', linewidth=1)
    plt.title("Distribuição de Frequência Relativa")
    plt.xlabel("Tamanho da Maior Estrada (Nº de Segmentos)")
    plt.ylabel("Proporção de Soluções (%)")
    plt.xticks(df_freq['metric_value'])
    plt.grid(axis='y', linestyle='--')
    plt.gca().yaxis.set_major_formatter(ticker.PercentFormatter())
    
    plt.savefig(OUTPUT_IMAGE_1, format='pdf', bbox_inches='tight')
    plt.close()
    print(f"✅ Gráfico 1 salvo: {OUTPUT_IMAGE_1}")

    # =========================================================================
    # GRÁFICO 2: Função de Distribuição Acumulada (Pontuação)
    # =========================================================================
    plt.figure(figsize=(7, 5))
    plt.plot(x_percentile, y_percentile, marker='o', markersize=8, linestyle='-', color='#e74c3c', linewidth=2.5)
    plt.fill_between(x_percentile, y_percentile, color='#e74c3c', alpha=0.1)
    plt.title("Função de Distribuição Acumulada (Pontuação Final)")
    plt.xlabel("Tamanho da Maior Estrada (Nº de Segmentos)")
    plt.ylabel("Pontuação Alcançada (%)")
    plt.xticks(x_percentile)
    plt.ylim(0, 105)
    plt.grid(True, linestyle='--')
    
    plt.savefig(OUTPUT_IMAGE_2, format='pdf', bbox_inches='tight')
    plt.close()
    print(f"✅ Gráfico 2 salvo: {OUTPUT_IMAGE_2}")
    
    con.close()

if __name__ == "__main__":
    main()
