// docs/js/script.js

// =============================================================================
// Estado Global e Constantes
// =============================================================================
const ASSETS_PATH = 'assets'; 

let gameData = {
    tiles: [],
    cards: [],
    paretoFront: {},
    percentiles: {},
    cardMap: new Map(),
};

let appState = {
    board: Array(9).fill(null), 
    selectedCards: new Set(),
    selectedTile: null,
    firstSelectedTileIndex: null,
    longPressTimer: null,
    currentParetoSolutions: [],
    currentParetoIndex: 0,
};

// Expondo para o Brython
window.gameData = gameData;
window.appState = appState;

// =============================================================================
// Funções de Inicialização
// =============================================================================

async function loadFile(path, description) {
    const loadingText = document.getElementById('loading-text');
    if (loadingText) {
        loadingText.textContent = `Loading ${description}...`;
    }
    const response = await fetch(path);
    if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status} for file ${path}`);
    }
    try {
        return await response.json();
    } catch (error) {
        throw new Error(`Invalid JSON in ${path}. Check the console for details.`);
    }
}

async function loadData() {
    try {
        gameData.tiles = await loadFile('data/tiles.json', 'tile definitions');
        gameData.cards = await loadFile('data/cards.json', 'card definitions');
        gameData.cardMap = new Map(gameData.cards.map(card => [card.number, card]));
        gameData.paretoFront = await loadFile('data/pareto_front.json', 'pareto front solutions');
        gameData.percentiles = await loadFile('data/percentiles.json', 'percentile data');
        document.getElementById('loading-text').textContent = "Data loaded successfully!";
        return true;
    } catch (error) {
        console.error("Fatal error loading essential data:", error);
        document.getElementById('loading-text').textContent = `Fatal error: ${error.message}.`;
        return false;
    }
}

function initializeApp() {
    document.getElementById('loading-overlay').style.display = 'none';
    renderBoard();
    renderCardSelection();
    renderAvailableTiles();
    attachEventListeners();
    updateStats();
}

// =============================================================================
// Funções de Renderização (sem mudanças na lógica, apenas mantidas)
// =============================================================================
function renderBoard() {
    const boardEl = document.getElementById('board');
    boardEl.innerHTML = '';
    appState.board.forEach((tile, index) => {
        const cell = document.createElement('div');
        cell.className = 'tile';
        cell.dataset.index = index;
        
        if (tile) {
            const [piece, side, orientation] = tile;
            const tileBg = document.createElement('div');
            tileBg.className = 'tile-bg';
            tileBg.style.backgroundImage = `url('${ASSETS_PATH}/tile_images/${side}_${piece}.png')`;
            tileBg.style.transform = `rotate(${orientation * 90}deg)`;
            cell.appendChild(tileBg);

            const rotateBtn = document.createElement('div');
            rotateBtn.className = 'rotate-btn';
            rotateBtn.dataset.index = index;
            rotateBtn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M23 4v6h-6"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>`;
            cell.appendChild(rotateBtn);

            if (appState.firstSelectedTileIndex === index) {
                cell.classList.add('selected-for-swap');
            }
        } else {
            cell.classList.add('bg-gray-700/50', 'flex', 'items-center', 'justify-center', 'border-2', 'border-dashed', 'border-gray-600');
            cell.innerHTML = `<span class="text-4xl text-gray-600">+</span>`;
        }
        boardEl.appendChild(cell);
    });
}

function renderCardSelection() {
    const desktopGrid = document.getElementById('card-selection-grid-desktop');
    const mobileGrid = document.getElementById('card-selection-grid-mobile');
    desktopGrid.innerHTML = '';
    mobileGrid.innerHTML = '';

    // Verifica se já selecionamos 3 cartas
    const isAtLimit = appState.selectedCards.size >= 3;

    gameData.cards.filter(card => card.number !== 4).forEach(card => {
        // Cria uma div ao invés de uma tag img
        const cardEl = document.createElement('div');
        cardEl.dataset.tooltip = `${card.name}: ${card.description}`;
        cardEl.dataset.cardId = card.number;
        
        // Classes base usando Tailwind para criar a aparência de item de lista
        cardEl.className = 'card bg-gray-800 border-gray-700 rounded-lg p-2 cursor-pointer transition-all duration-200 hover:bg-gray-700 flex items-center space-x-3 w-full';
        
        // Estrutura interna: Círculo com o número e texto com o nome
        cardEl.innerHTML = `
            <span class="flex items-center justify-center bg-gray-900 text-blue-400 rounded-full w-8 h-8 font-bold text-sm shrink-0 border border-gray-700">
                ${card.number}
            </span>
            <span class="font-medium text-gray-200 text-sm leading-tight select-none">
                ${card.name}
            </span>
        `;
        
        const isSelected = appState.selectedCards.has(card.number);

        if (isSelected) {
            // Se estiver selecionado, a classe .card.selected do CSS fará o destaque
            cardEl.classList.add('selected');
        } else if (isAtLimit) {
            // SE não está selecionada E o limite foi atingido, aplica o "Dimming"
            cardEl.classList.add('opacity-40', 'cursor-not-allowed');
            cardEl.classList.remove('hover:bg-gray-700'); // Remove o hover se estiver desabilitado
        }
        
        desktopGrid.appendChild(cardEl);
        
        // Clona para a visualização mobile
        const mobileClone = cardEl.cloneNode(true);
        mobileGrid.appendChild(mobileClone);
    });
}

function renderAvailableTiles() {
    const palette = document.getElementById('available-tiles-grid');
    palette.innerHTML = '';
    const usedPieceIds = new Set(appState.board.filter(t => t !== null).map(t => t[0]));

    for (let i = 0; i < 9; i++) {
        const isUsed = usedPieceIds.has(i);
        for (let side = 0; side < 2; side++) {
            const tileEl = document.createElement('div');
            tileEl.className = 'palette-tile';
            tileEl.dataset.pieceId = i;
            tileEl.dataset.side = side;
            tileEl.style.backgroundImage = `url('${ASSETS_PATH}/tile_images/${side}_${i}.png')`;
            tileEl.style.gridColumn = i + 1;
            tileEl.style.gridRow = side + 1;
            if (isUsed) {
                tileEl.classList.add('used');
            }
            if (appState.selectedTile?.pieceId === i && appState.selectedTile?.side === side) {
                tileEl.classList.add('selected');
            }
            palette.appendChild(tileEl);
        }
    }
}

// =============================================================================
// LÓGICA DE ANÁLISE (AGORA USANDO PYTHON)
// =============================================================================

function updateStats() {
    const statsPanel = document.getElementById('stats-panel');
    const titleEl = document.getElementById('selected-cards-stats-title');

    if (appState.board.some(t => t === null)) {
        statsPanel.innerHTML = '<p class="text-gray-500">Fill the board to see the statistics.</p>';
        titleEl.innerHTML = '';
        return;
    }
    
    // Step 1: Call validation in Python
    if (typeof window.validate_current_board === 'function') {
        statsPanel.innerHTML = '<p class="text-gray-500">Validating board...</p>';
        window.validate_current_board();
    }
}

function validationCallback(resultJson) {
    const result = JSON.parse(resultJson); // Convert Python JSON to JS Object
    const statsPanel = document.getElementById('stats-panel');

    if (result.isValid) {
        // Step 2: If valid, call statistics analysis in Python
        if (typeof window.analyze_current_board === 'function') {
            statsPanel.innerHTML = '<p class="text-gray-500">Calculating statistics...</p>';
            window.analyze_current_board();
        }
    } else {
        // If invalid, show the error in normal colors
        statsPanel.innerHTML = `<p class="text-gray-200 font-bold">Invalid Board:</p><p class="text-gray-400">${result.error}</p>`;
        document.getElementById('selected-cards-stats-title').innerHTML = '';
    }
}
window.validationCallback = validationCallback; // Exposing to Python

function updateStatsCallback(statsJson) {
    const stats = JSON.parse(statsJson);
    const statsPanel = document.getElementById('stats-panel');
    const titleEl = document.getElementById('selected-cards-stats-title');

    titleEl.innerHTML = '';
    let html = '';

    if (appState.selectedCards.size > 0) {
        const sortedCardIds = Array.from(appState.selectedCards).sort((a,b)=>a-b);
        let product = 1;
        let count = 0;
        const cardScores = []; 
        
        // Passo 1: Calcular os scores individuais e o produto
        sortedCardIds.forEach(id => {
            const card = gameData.cardMap.get(id);
            if (card && card.key && card.type) {
                const rawValue = stats[card.key];
                
                let percentileValue = gameData.percentiles[card.key]?.[rawValue] ?? 0;
                const isMin = card.type.toLowerCase() === 'min';
                const score = isMin ? (100.0 - percentileValue) : percentileValue;
                
                // Trabalhamos na escala 0.0 a 1.0 para o cálculo da raiz não estourar os limites
                product *= (score / 100.0);
                count++;
                
                cardScores.push({ card, rawValue, score });
            }
        });

        // Passo 2: Calcular a Média Geométrica
        const geometricMean = count > 0 ? (Math.pow(product, 1 / count) * 100) : 0;
        
        // NOVO: Arredondamento forçado para baixo (Floor) com 1 casa decimal
        const flooredMean = Math.floor(geometricMean * 10) / 10;

        // DESTAQUE: SCORE TOTAL (Com subtítulo sutil)
        html += `
            <div class="bg-gradient-to-r from-blue-900/60 to-indigo-900/60 border border-blue-700/50 rounded-lg p-4 mb-5 shadow-md flex justify-between items-center">
                <div class="flex flex-col">
                    <span class="text-lg font-bold text-white leading-tight">Total Score</span>
                    <span class="text-xs text-blue-300/70 font-medium mt-0.5">Geometric Mean</span>
                </div>
                <span class="text-3xl font-bold text-blue-300 drop-shadow-md">${flooredMean.toFixed(1)}</span>
            </div>
        `;

        // SEÇÃO: SCORES DOS OBJETIVOS INDIVIDUAIS
        html += `<h3 class="font-bold text-white mb-3 border-b border-gray-700 pb-1">Objective Scores</h3>`;
        html += `<div class="space-y-2 mb-6">`; 
        
        cardScores.forEach(({ card, rawValue, score }) => {
            // NOVO: Arredondamento forçado para baixo também nos objetivos individuais
            const flooredScore = Math.floor(score * 10) / 10;
            
            html += `
                <div class="bg-blue-900/40 border border-blue-800/50 rounded p-2 shadow-sm">
                    <div class="flex justify-between items-center mb-1">
                        <span class="text-blue-100 font-bold text-sm">${card.name}</span>
                        <span class="text-blue-400 font-bold text-lg">${flooredScore.toFixed(1)}</span>
                    </div>
                    <div class="text-xs text-gray-400 flex justify-between items-center">
                        <span class="capitalize">${card.key.replace(/_/g, ' ')}</span>
                        <span class="bg-gray-800 px-2 py-0.5 rounded text-gray-300">Value: ${rawValue}</span>
                    </div>
                </div>
            `;
        });
        html += `</div>`;
    }

    // SEÇÃO: MÉTRICAS GERAIS DO TABULEIRO
    html += `<h3 class="font-bold text-gray-400 mb-2 border-b border-gray-700 pb-1">Board Metrics</h3>`;
    html += `<div class="space-y-1">`;

    const allStats = Object.entries(stats).sort(([keyA], [keyB]) => keyA.localeCompare(keyB));

    for (const [key, value] of allStats) {
        const isUsed = Array.from(appState.selectedCards).some(id => gameData.cardMap.get(id)?.key === key);
        const bgColor = isUsed ? 'bg-gray-800' : 'hover:bg-gray-800/50';
        const textColor = isUsed ? 'text-gray-300 font-semibold' : 'text-gray-500';

        html += `
            <div class="flex justify-between items-center py-1 px-2 rounded ${bgColor} transition-colors">
                <span class="${textColor} capitalize text-xs">${key.replace(/_/g, ' ')}:</span>
                <span class="font-mono text-gray-400 text-sm">${value}</span>
            </div>`;
    }
    html += `</div>`;

    statsPanel.innerHTML = html;
}
window.updateStatsCallback = updateStatsCallback; // Exposing to Python


// =============================================================================
// Manipuladores de Eventos (sem mudanças na lógica, apenas mantidos)
// =============================================================================
function handleBoardClick(e) {
    const rotateBtn = e.target.closest('.rotate-btn');
    if (rotateBtn) {
        hideParetoNav();
        const index = parseInt(rotateBtn.dataset.index);
        const tileOnBoard = appState.board[index];
        if (tileOnBoard) {
            tileOnBoard[2] = (tileOnBoard[2] + 1) % 4;
            renderBoard();
            updateStats();
        }
        return;
    }

    const cell = e.target.closest('.tile');
    if (!cell) return;
    hideParetoNav();
    
    const index = parseInt(cell.dataset.index);
    const tileOnBoard = appState.board[index];

    if (appState.selectedTile && !tileOnBoard) {
        const { pieceId, side } = appState.selectedTile;
        appState.board[index] = [pieceId, side, 0];
        appState.selectedTile = null;
        if (appState.firstSelectedTileIndex !== null) {
            appState.firstSelectedTileIndex = null;
        }
        renderBoard();
        renderAvailableTiles();
        updateStats();
        return;
    }

    if (tileOnBoard) {
        if (appState.firstSelectedTileIndex === null) {
            appState.firstSelectedTileIndex = index;
            cell.classList.add('selected-for-swap');
        } else {
            if (appState.firstSelectedTileIndex === index) {
                appState.firstSelectedTileIndex = null;
                cell.classList.remove('selected-for-swap');
            } else {
                const tempTile = appState.board[appState.firstSelectedTileIndex];
                appState.board[appState.firstSelectedTileIndex] = appState.board[index];
                appState.board[index] = tempTile;
                appState.firstSelectedTileIndex = null;
                renderBoard();
                updateStats();
            }
        }
    } else { 
        if (appState.firstSelectedTileIndex !== null) {
            appState.board[index] = appState.board[appState.firstSelectedTileIndex];
            appState.board[appState.firstSelectedTileIndex] = null;
            appState.firstSelectedTileIndex = null;
            renderBoard();
            updateStats();
        }
    }
}

function handlePaletteClick(e) {
    const tile = e.target.closest('.palette-tile');
    if (!tile || tile.classList.contains('used')) return;

    const pieceId = parseInt(tile.dataset.pieceId);
    const side = parseInt(tile.dataset.side);

    if (appState.selectedTile?.pieceId === pieceId && appState.selectedTile?.side === side) {
        appState.selectedTile = null;
    } else {
        appState.selectedTile = { pieceId, side };
    }
    
    if (appState.firstSelectedTileIndex !== null) {
        appState.firstSelectedTileIndex = null;
        renderBoard();
    }
    renderAvailableTiles();
}

function handleBoardDoubleClick(e) {
    const cell = e.target.closest('.tile');
    if (!cell) return;
    hideParetoNav();
    
    const index = parseInt(cell.dataset.index);
    if (appState.board[index]) {
        appState.board[index] = null;
        appState.firstSelectedTileIndex = null;
        renderBoard();
        renderAvailableTiles();
        updateStats();
    }
}

function handleBoardRightClick(e) {
    e.preventDefault();
    const cell = e.target.closest('.tile');
    if (!cell) return;
    hideParetoNav();
    const index = parseInt(cell.dataset.index);
    const tile = appState.board[index];
    if (tile) {
        tile[1] = (tile[1] + 1) % 2;
        appState.firstSelectedTileIndex = null;
    }
    renderBoard();
    updateStats();
}

function handleCardClick(e) {
    const cardEl = e.target.closest('.card');
    if (!cardEl) return;
    const cardId = parseInt(cardEl.dataset.cardId);

    if (appState.selectedCards.has(cardId)) {
        appState.selectedCards.delete(cardId);
    } else {
        if (appState.selectedCards.size < 3) {
            appState.selectedCards.add(cardId);
        }
    }
    document.getElementById('optimal-solution-btn').disabled = appState.selectedCards.size === 0;
    hideParetoNav();
    renderCardSelection();
    updateStats();
}

function hideParetoNav() {
    document.getElementById('pareto-nav').classList.add('hidden');
    appState.currentParetoSolutions = [];
}

function showOptimalSolution() {
    if (appState.selectedCards.size === 0) return;
    
    // Pega as cartas selecionadas em ordem crescente (a mesma ordem usada no Python)
    const sortedCardIds = Array.from(appState.selectedCards).sort((a, b) => a - b);
    const solutionKey = sortedCardIds.join('_');
    const paretoSolutions = gameData.paretoFront[solutionKey];
    
    if (paretoSolutions && paretoSolutions.length > 0) {
        
        // Pega os dados das cartas para saber a chave e o tipo (max/min)
        const selectedCardsData = sortedCardIds.map(id => gameData.cardMap.get(id));

        // Ordena o array da Fronteira de Pareto com base no maior produto dos PERCENTIS
        const sortedSolutions = [...paretoSolutions].sort((a, b) => {
            let prodA = 1;
            let prodB = 1;

            for (let i = 0; i < selectedCardsData.length; i++) {
                const card = selectedCardsData[i];
                const rawValA = a.scores[i];
                const rawValB = b.scores[i];

                // Busca o percentil correspondente; se não achar, assume 0
                let percA = gameData.percentiles[card.key]?.[rawValA] ?? 0;
                let percB = gameData.percentiles[card.key]?.[rawValB] ?? 0;

                // Inverte se for carta de minimizar
                if (card.type === 'min') {
                    percA = 100 - percA;
                    percB = 100 - percB;
                }

                prodA *= percA;
                prodB *= percB;
            }

            return prodB - prodA; // Ordem decrescente
        });

        appState.currentParetoSolutions = sortedSolutions;
        appState.currentParetoIndex = 0; // Começa pelo maior produto
        
        document.getElementById('pareto-nav').classList.remove('hidden');
        loadParetoBoard(0);
    } else {
        alert("Pareto Front solutions not found for this combination of cards.");
        hideParetoNav();
    }
}

function loadParetoBoard(index) {
    const solutionObj = appState.currentParetoSolutions[index];
    if (!solutionObj) return;

    // O duckdb já salva a matrix plana em `board`, é só mapear para o appState
    appState.board = solutionObj.board.map(tile => [...tile]);
    
    renderBoard();
    renderAvailableTiles();
    updateStats();
    updateParetoNavUI();
}

function updateParetoNavUI() {
    const total = appState.currentParetoSolutions.length;
    const current = appState.currentParetoIndex + 1; // Para exibir começando de 1
    
    document.getElementById('pareto-indicator').innerText = `${current} / ${total}`;
    document.getElementById('pareto-prev-btn').disabled = (current <= 1);
    document.getElementById('pareto-next-btn').disabled = (current >= total);
}

function handleParetoPrev() {
    if (appState.currentParetoIndex > 0) {
        appState.currentParetoIndex--;
        loadParetoBoard(appState.currentParetoIndex);
    }
}

function handleParetoNext() {
    if (appState.currentParetoIndex < appState.currentParetoSolutions.length - 1) {
        appState.currentParetoIndex++;
        loadParetoBoard(appState.currentParetoIndex);
    }
}

function resetBoard() {
    hideParetoNav();
    appState.board.fill(null);
    appState.selectedTile = null;
    appState.firstSelectedTileIndex = null;
    renderBoard();
    renderAvailableTiles();
    updateStats();
}

function handleCardMouseOver(e) {
    const card = e.target.closest('.card');
    if (!card) return;
    const tooltip = document.getElementById('card-tooltip');
    tooltip.textContent = card.dataset.tooltip;
    tooltip.style.display = 'block';
}
function handleCardMouseLeave() {
    document.getElementById('card-tooltip').style.display = 'none';
}
function handleCardMouseMove(e) {
    const tooltip = document.getElementById('card-tooltip');
    tooltip.style.left = `${e.clientX + 15}px`;
    tooltip.style.top = `${e.clientY + 15}px`;
}

function handleTouchStart(e) {
    const cell = e.target.closest('.tile');
    if (!cell || !appState.board[cell.dataset.index]) return;
    hideParetoNav();
    
    appState.longPressTimer = setTimeout(() => {
        const index = parseInt(cell.dataset.index);
        const tile = appState.board[index];
        if (tile) {
            tile[1] = (tile[1] + 1) % 2;
            renderBoard();
            updateStats();
        }
        appState.longPressTimer = null;
    }, 500);
}

function handleTouchEnd(e) {
    if (appState.longPressTimer) {
        clearTimeout(appState.longPressTimer);
        appState.longPressTimer = null;
    }
}

function attachEventListeners() {
    const board = document.getElementById('board');
    board.addEventListener('click', handleBoardClick);
    board.addEventListener('dblclick', handleBoardDoubleClick);
    board.addEventListener('contextmenu', handleBoardRightClick);
    board.addEventListener('touchstart', handleTouchStart, { passive: true });
    board.addEventListener('touchend', handleTouchEnd);

    const desktopCardGrid = document.getElementById('card-selection-grid-desktop');
    desktopCardGrid.addEventListener('click', handleCardClick);
    desktopCardGrid.addEventListener('mouseover', handleCardMouseOver);
    desktopCardGrid.addEventListener('mouseleave', handleCardMouseLeave);
    desktopCardGrid.addEventListener('mousemove', handleCardMouseMove);

    const mobileCardGrid = document.getElementById('card-selection-grid-mobile');
    mobileCardGrid.addEventListener('click', handleCardClick);
    
    document.getElementById('available-tiles-container').addEventListener('click', handlePaletteClick);
    document.getElementById('optimal-solution-btn').addEventListener('click', showOptimalSolution);
    document.getElementById('reset-board-btn').addEventListener('click', resetBoard);
    document.getElementById('optimal-solution-btn').disabled = true;
    document.getElementById('pareto-prev-btn').addEventListener('click', handleParetoPrev);
    document.getElementById('pareto-next-btn').addEventListener('click', handleParetoNext);

    // Modal Events
    const helpBtn = document.getElementById('help-btn');
    const closeHelpBtn = document.getElementById('close-help-btn');
    const helpModal = document.getElementById('help-modal');

    if (helpBtn && closeHelpBtn && helpModal) {
        helpBtn.addEventListener('click', () => helpModal.classList.remove('hidden'));
        closeHelpBtn.addEventListener('click', () => helpModal.classList.add('hidden'));
        
        // Fecha o modal ao clicar fora da caixa preta
        helpModal.addEventListener('click', (e) => {
            if (e.target === helpModal) {
                helpModal.classList.add('hidden');
            }
        });
    }
}

// =============================================================================
// Ponto de Entrada da Aplicação
// =============================================================================
document.addEventListener('DOMContentLoaded', async () => {
    if (await loadData()) {
        initializeApp();
    }
});