from PIL import Image
import os

def dividir_imagem_em_grid(caminho_imagem, linhas=3, colunas=3):
    """
    Divide uma imagem em um grid (por padrão, 3x3) e salva cada parte como um novo arquivo.

    Args:
        caminho_imagem (str): O caminho para a imagem de entrada.
        linhas (int): O número de linhas no grid.
        colunas (int): O número de colunas no grid.
    """
    try:
        # Abre a imagem original
        img_original = Image.open(caminho_imagem)
        largura, altura = img_original.size

        # Calcula a largura e altura de cada célula do grid
        largura_celula = largura // colunas
        altura_celula = altura // linhas

        # Cria um diretório para salvar as imagens cortadas, se não existir
        output_dir = "tile_images"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        print(f"Dimensões da imagem original: {largura}x{altura}")
        print(f"Tamanho de cada célula do grid: {largura_celula}x{altura_celula}")

        # Loop para percorrer as linhas e colunas do grid
        contador = 0
        for i in range(linhas):
            for j in range(colunas):
                # Define as coordenadas da caixa de corte (esquerda, cima, direita, baixo)
                esquerda = j * largura_celula
                cima = i * altura_celula
                direita = esquerda + largura_celula
                baixo = cima + altura_celula

                # Cria a caixa de corte
                caixa_corte = (esquerda, cima, direita, baixo)

                # Corta a imagem usando a caixa definida
                img_cortada = img_original.crop(caixa_corte)

                # Define o nome do arquivo de saída e o salva
                nome_arquivo = os.path.join(output_dir, f"1_{contador}.png")
                img_cortada.save(nome_arquivo)
                print(f"Salvo: {nome_arquivo}")

                contador += 1
        
        print(f"\nOperação concluída! {contador} imagens foram salvas no diretório '{output_dir}'.")

    except FileNotFoundError:
        print(f"Erro: O arquivo '{caminho_imagem}' não foi encontrado.")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")

# --- Início da Execução do Script ---
if __name__ == "__main__":
    # Coloque o nome do seu arquivo de imagem PNG aqui
    nome_do_arquivo_de_entrada = "/home/rolim/Meus Arquivos/Projetos/Pessoal/Nine Tiles Panic Solver/tiles/tiles_lado_1.png"
    
    # Para fins de demonstração, vamos criar uma imagem de exemplo se a sua não existir
    if not os.path.exists(nome_do_arquivo_de_entrada):
        print(f"'{nome_do_arquivo_de_entrada}' não encontrado. Criando uma imagem de exemplo para demonstração...")
        try:
            from PIL import ImageDraw, ImageFont
            # Cria uma imagem de 600x450 com fundo cinza
            img_exemplo = Image.new('RGB', (600, 450), color = 'gray')
            d = ImageDraw.Draw(img_exemplo)
            
            # Adiciona um texto simples para visualização
            try:
                # Tenta usar uma fonte padrão que geralmente está disponível
                font = ImageFont.truetype("arial.ttf", 40)
            except IOError:
                # Se a fonte não for encontrada, usa a fonte padrão do PIL
                font = ImageFont.load_default()
            
            d.text((150, 200), "IMAGEM DE EXEMPLO", fill=(255, 255, 0), font=font)
            img_exemplo.save(nome_do_arquivo_de_entrada)
            print(f"Imagem de exemplo '{nome_do_arquivo_de_entrada}' criada.")
        except ImportError:
             print("Pillow não está instalado. Não foi possível criar a imagem de exemplo.")
        except Exception as e:
            print(f"Ocorreu um erro ao criar a imagem de exemplo: {e}")


    # Chama a função para dividir a imagem
    dividir_imagem_em_grid(nome_do_arquivo_de_entrada)