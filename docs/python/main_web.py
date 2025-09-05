# docs/python/main_web.py
from browser import window # type: ignore
import json
import analysis 

# Acessa as variáveis globais que o script.js irá criar
js_gameData = window.gameData
js_appState = window.appState

def _get_solution_grid():
    """Função auxiliar para converter o estado do tabuleiro do JS para um grid Python."""
    board_list = js_appState.board
    solution_grid = []
    for i in range(3):
        row = [tuple(x) if x else (-1, -1, -1) for x in board_list[i*3:(i+1)*3]]
        solution_grid.append(row)
    return solution_grid

def validate_current_board_js(*args):
    """
    Função chamada pelo JS para validar o tabuleiro.
    Chama a função Python e devolve o resultado para um callback no JS.
    """
    solution_grid = _get_solution_grid()
    game_tiles = json.loads(window.JSON.stringify(js_gameData.tiles))
    
    validation_result = analysis.is_board_valid(solution_grid, game_tiles)
    
    window.validationCallback(json.dumps(validation_result))

def analyze_current_board_js(*args):
    """
    Função chamada pelo JS para calcular as estatísticas.
    """
    solution_grid = _get_solution_grid()
    game_tiles = json.loads(window.JSON.stringify(js_gameData.tiles))
    
    stats_dict = analysis.calculate_solution_stats(solution_grid, game_tiles)
    
    window.updateStatsCallback(json.dumps(stats_dict))

# Expõe as funções para o JavaScript, tornando-as globais
window.validate_current_board = validate_current_board_js
window.analyze_current_board = analyze_current_board_js