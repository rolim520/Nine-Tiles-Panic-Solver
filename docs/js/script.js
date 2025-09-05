// docs/js/script.js

// =============================================================================
// Estado Global e Constantes
// =============================================================================
const ASSETS_PATH = 'assets'; 

let gameData = {
    tiles: [],
    cards: [],
    solutions: {},
    percentiles: {},
    cardMap: new Map(),
};

let appState = {
    board: Array(9).fill(null), 
    selectedCards: new Set(),
    selectedTile: null,
    firstSelectedTileIndex: null,
    longPressTimer: null,
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
        gameData.solutions = await loadFile('data/best_solutions.json', 'optimal solutions');
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

    gameData.cards.filter(card => card.number !== 4).forEach(card => {
        const cardEl = document.createElement('img');
        cardEl.src = `${ASSETS_PATH}/card_images/${card.number}.jpg`;
        cardEl.alt = card.name;
        cardEl.dataset.tooltip = `${card.name}: ${card.description}`;
        cardEl.className = 'card rounded-md cursor-pointer w-full h-auto';
        cardEl.dataset.cardId = card.number;
        if (appState.selectedCards.has(card.number)) {
            cardEl.classList.add('selected');
        }
        
        desktopGrid.appendChild(cardEl);
        mobileGrid.appendChild(cardEl.cloneNode(true));
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
        // If invalid, show the error
        statsPanel.innerHTML = `<p class="text-red-400 font-bold">Invalid Board:</p><p class="text-red-400">${result.error}</p>`;
        document.getElementById('selected-cards-stats-title').innerHTML = '';
    }
}
window.validationCallback = validationCallback; // Exposing to Python

function updateStatsCallback(statsJson) {
    const stats = JSON.parse(statsJson); // Convert to a pure JS object
    const statsPanel = document.getElementById('stats-panel');
    const titleEl = document.getElementById('selected-cards-stats-title');

    if (appState.selectedCards.size > 0) {
        const cardNames = Array.from(appState.selectedCards).map(id => gameData.cardMap.get(id)?.name || `Card ${id}`);
        titleEl.innerHTML = `<strong>Objectives:</strong> ${cardNames.join(', ')}`;
    } else {
        titleEl.innerHTML = '';
    }
    
    const selectedCardKeys = new Set(Array.from(appState.selectedCards).map(id => gameData.cardMap.get(id)?.key));
    
    const allStats = Object.entries(stats);
    
    const selectedStats = allStats
        .filter(([key]) => selectedCardKeys.has(key))
        .sort(([keyA], [keyB]) => keyA.localeCompare(keyB));

    const otherStats = allStats
        .filter(([key]) => !selectedCardKeys.has(key))
        .sort(([keyA], [keyB]) => keyA.localeCompare(keyB));

    const sortedStats = [...selectedStats, ...otherStats];
    
    let html = '';
    for (const [key, value] of sortedStats) {
        let percentileText = '';
        let highlightClass = '';
        
        if (selectedCardKeys.has(key)) {
            highlightClass = 'bg-blue-900/50';
            const cardData = gameData.cards.find(c => c.key === key);
            const percentileValue = gameData.percentiles[key]?.[value];

            if (cardData && percentileValue !== undefined) {
                const score = cardData.type === 'min' ? 100 - percentileValue : percentileValue;
                percentileText = `<span class="text-blue-400 font-semibold">(Score: ${score.toFixed(1)})</span>`;
            }
        }

        html += `
            <div class="flex justify-between items-center py-1 px-2 rounded ${highlightClass}">
                <span class="text-gray-400 capitalize">${key.replace(/_/g, ' ')}:</span>
                <div>
                    <span class="font-bold stat-value text-base">${value}</span>
                    ${percentileText}
                </div>
            </div>`;
    }
    statsPanel.innerHTML = html;
}
window.updateStatsCallback = updateStatsCallback; // Exposing to Python


// =============================================================================
// Manipuladores de Eventos (sem mudanças na lógica, apenas mantidos)
// =============================================================================
function handleBoardClick(e) {
    const rotateBtn = e.target.closest('.rotate-btn');
    if (rotateBtn) {
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
    renderCardSelection();
    updateStats();
}

function showOptimalSolution() {
    if (appState.selectedCards.size === 0) return;
    
    const sortedCardIds = Array.from(appState.selectedCards).sort((a,b)=>a-b);
    const solutionKey = sortedCardIds.join('_');
    const optimalSolution = gameData.solutions[solutionKey];
    
    if (optimalSolution) {
        const newBoardState = [];
        for (let r = 0; r < 3; r++) {
            for (let c = 0; c < 3; c++) {
                newBoardState.push([...optimalSolution[`p${r}${c}`]]);
            }
        }
        appState.board = newBoardState;
        renderBoard();
        renderAvailableTiles();
        updateStats();
    } else {
        alert("Optimal solution not found for this combination of cards.");
    }
}

function resetBoard() {
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
}

// =============================================================================
// Ponto de Entrada da Aplicação
// =============================================================================
document.addEventListener('DOMContentLoaded', async () => {
    if (await loadData()) {
        initializeApp();
    }
});