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
    selectedTile: null, // { pieceId, side } - para peças da paleta
    firstSelectedTileIndex: null, // index - para peças do tabuleiro
    longPressTimer: null,
};

// =============================================================================
// Funções de Inicialização
// =============================================================================

async function loadFile(path, description) {
    const loadingText = document.getElementById('loading-text');
    if (loadingText) {
        loadingText.textContent = `Carregando ${description}...`;
    }
    const response = await fetch(path);
    if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status} for file ${path}`);
    }
    try {
        return await response.json();
    } catch (error) {
        throw new Error(`JSON inválido em ${path}. Verifique o console para detalhes.`);
    }
}

async function loadData() {
    const loadingText = document.getElementById('loading-text');
    try {
        gameData.tiles = await loadFile('data/tiles.json', 'definições das peças');
        gameData.cards = await loadFile('data/cards.json', 'definições das cartas');
        gameData.cardMap = new Map(gameData.cards.map(card => [card.number, card]));
        
        try {
            gameData.solutions = await loadFile('data/best_solutions.json', 'soluções ótimas');
            gameData.percentiles = await loadFile('data/percentiles.json', 'dados de percentis');
        } catch (analysisError) {
            console.warn("Aviso: Falha ao carregar arquivos de análise. A funcionalidade de 'Solução Ótima' será desabilitada.", analysisError);
            const optimalBtn = document.getElementById('optimal-solution-btn');
            if (optimalBtn) {
                optimalBtn.disabled = true;
                optimalBtn.title = "Não foi possível carregar os dados das soluções ótimas.";
            }
        }
        
        if (loadingText) loadingText.textContent = "Dados carregados com sucesso!";
        return true;

    } catch (error) {
        console.error("Erro fatal ao carregar dados essenciais:", error);
        if (loadingText) loadingText.textContent = `Erro fatal ao carregar dados: ${error.message}. Verifique o console (F12).`;
        return false;
    }
}

function initializeApp() {
    const loadingOverlay = document.getElementById('loading-overlay');
    if (loadingOverlay) {
        loadingOverlay.style.display = 'none';
    }

    renderBoard();
    renderCardSelection();
    renderAvailableTiles();
    attachEventListeners();
    updateStats();
}

// =============================================================================
// Funções de Renderização
// =============================================================================
function renderBoard() {
    const boardEl = document.getElementById('board');
    if (!boardEl) return;
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
    if (!desktopGrid || !mobileGrid) return;

    desktopGrid.innerHTML = '';
    mobileGrid.innerHTML = '';

    if (!gameData.cards || gameData.cards.length === 0) {
        desktopGrid.innerHTML = `<p class="text-red-400 col-span-full">Não foi possível carregar as cartas.</p>`;
        return;
    }

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
    if (!palette) return;

    palette.innerHTML = '';

    const usedPieceIds = new Set(appState.board.filter(t => t !== null).map(t => t[0]));

    for (let i = 0; i < 9; i++) {
        const isUsed = usedPieceIds.has(i);

        const tile0 = document.createElement('div');
        tile0.className = 'palette-tile';
        tile0.dataset.pieceId = i;
        tile0.dataset.side = 0;
        tile0.style.backgroundImage = `url('${ASSETS_PATH}/tile_images/0_${i}.png')`;
        tile0.style.gridColumn = i + 1;
        tile0.style.gridRow = 1;
        if (isUsed) {
            tile0.classList.add('used');
        }
        if (appState.selectedTile?.pieceId === i && appState.selectedTile?.side === 0) {
            tile0.classList.add('selected');
        }
        palette.appendChild(tile0);

        const tile1 = document.createElement('div');
        tile1.className = 'palette-tile';
        tile1.dataset.pieceId = i;
        tile1.dataset.side = 1;
        tile1.style.backgroundImage = `url('${ASSETS_PATH}/tile_images/1_${i}.png')`;
        tile1.style.gridColumn = i + 1;
        tile1.style.gridRow = 2;
        if (isUsed) {
            tile1.classList.add('used');
        }
        if (appState.selectedTile?.pieceId === i && appState.selectedTile?.side === 1) {
            tile1.classList.add('selected');
        }
        palette.appendChild(tile1);
    }
}


function updateStats() {
    const statsPanel = document.getElementById('stats-panel');
    const titleEl = document.getElementById('selected-cards-stats-title');
    if (!statsPanel || !titleEl) return;

    if (appState.board.some(t => t === null)) {
        statsPanel.innerHTML = '<p class="text-gray-500">Preencha o tabuleiro para ver as estatísticas.</p>';
        titleEl.innerHTML = '';
        return;
    }

    const validity = isBoardValid(appState.board, gameData.tiles);
    if (!validity.isValid) {
        statsPanel.innerHTML = `<p class="text-red-400 font-bold">Tabuleiro Inválido:</p><p class="text-red-400">${validity.error}</p>`;
        titleEl.innerHTML = '';
        return;
    }

    if (appState.selectedCards.size > 0) {
        const cardNames = Array.from(appState.selectedCards).map(id => gameData.cardMap.get(id)?.name || `Carta ${id}`);
        titleEl.innerHTML = `<strong>Objetivos:</strong> ${cardNames.join(', ')}`;
    } else {
        titleEl.innerHTML = '';
    }
    
    const stats = calculateSolutionStats(appState.board, gameData.tiles);
    
    // Hoist selected stats to the top
    const selectedCardKeys = new Set(Array.from(appState.selectedCards).map(id => gameData.cardMap.get(id)?.key));
    const allEntries = Object.entries(stats);
    
    const selectedEntries = allEntries
        .filter(([key]) => selectedCardKeys.has(key))
        .sort(([keyA], [keyB]) => keyA.localeCompare(keyB));

    const otherEntries = allEntries
        .filter(([key]) => !selectedCardKeys.has(key))
        .sort(([keyA], [keyB]) => keyA.localeCompare(keyB));

    const sortedStats = [...selectedEntries, ...otherEntries];

    let html = '';
    for (const [key, value] of sortedStats) {
        let percentileText = '';
        let highlightClass = '';
        
        if (selectedCardKeys.has(key)) {
            highlightClass = 'bg-blue-900/50';
            const cardData = [...gameData.cards].find(c => c.key === key);
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

// =============================================================================
// Manipuladores de Eventos
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
                newBoardState.push(optimalSolution[`p${r}${c}`]);
            }
        }
        appState.board = newBoardState;
        renderBoard();
        renderAvailableTiles();
        updateStats();
    } else {
        alert("Solução ótima não encontrada para esta combinação de cartas.");
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
    const tooltip = document.getElementById('card-tooltip');
    tooltip.style.display = 'none';
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
    
    const availableTilesContainer = document.getElementById('available-tiles-container');
    availableTilesContainer.addEventListener('click', handlePaletteClick);

    document.getElementById('optimal-solution-btn').addEventListener('click', showOptimalSolution);
    document.getElementById('reset-board-btn').addEventListener('click', resetBoard);
    
    document.getElementById('optimal-solution-btn').disabled = true;

    window.addEventListener('resize', () => {});
}

// =============================================================================
// SECTION: BOARD VALIDATION & ANALYSIS
// =============================================================================

const TILE_NODES = [
    [3, 0, 4, 7],    // Pos 0
    [4, 1, 5, 8],    // Pos 1
    [5, 2, 6, 9],    // Pos 2
    [10, 7, 11, 14], // Pos 3
    [11, 8, 12, 15], // Pos 4
    [12, 9, 13, 16], // Pos 5
    [17, 14, 18, 21],// Pos 6
    [18, 15, 19, 22],// Pos 7
    [19, 16, 20, 23] // Pos 8
];

class UnionFind {
    constructor(size) {
        this.parent = Array.from({ length: size }, (_, i) => i);
    }
    find(i) {
        if (this.parent[i] === i) return i;
        return this.parent[i] = this.find(this.parent[i]);
    }
    union(i, j) {
        const rootI = this.find(i);
        const rootJ = this.find(j);
        if (rootI !== rootJ) {
            this.parent[rootI] = rootJ;
            return false;
        }
        return true;
    }
}

function getTileConnections(tileData, orientation) {
    const connections = [0, 0, 0, 0]; // Left, Top, Right, Bottom
    if (tileData && tileData.roads) {
        for (const road of tileData.roads) {
            connections[(road.connection[0] + orientation) % 4] = 1;
            connections[(road.connection[1] + orientation) % 4] = 1;
        }
    }
    return connections;
}

function isBoardValid(board, gameTiles) {
    const NUM_NODES_VALIDATION = 24;
    const uf = new UnionFind(NUM_NODES_VALIDATION);

    // Check horizontal connections
    for (let r = 0; r < 3; r++) {
        for (let c = 0; c < 2; c++) {
            const tile1Data = board[r * 3 + c];
            const tile2Data = board[r * 3 + c + 1];
            const tile1Conns = getTileConnections(gameTiles[tile1Data[0]][tile1Data[1]], tile1Data[2]);
            const tile2Conns = getTileConnections(gameTiles[tile2Data[0]][tile2Data[1]], tile2Data[2]);
            if (tile1Conns[2] !== tile2Conns[0]) { // Right of tile1 vs Left of tile2
                return { isValid: false, error: `Peça ${tile1Data[0]} (lado ${tile1Data[1]}) e Peça ${tile2Data[0]} (lado ${tile2Data[1]}) não se conectam.` };
            }
        }
    }

    // Check vertical connections
    for (let r = 0; r < 2; r++) {
        for (let c = 0; c < 3; c++) {
            const tile1Data = board[r * 3 + c];
            const tile2Data = board[(r + 1) * 3 + c];
            const tile1Conns = getTileConnections(gameTiles[tile1Data[0]][tile1Data[1]], tile1Data[2]);
            const tile2Conns = getTileConnections(gameTiles[tile2Data[0]][tile2Data[1]], tile2Data[2]);
            if (tile1Conns[3] !== tile2Conns[1]) { // Bottom of tile1 vs Top of tile2
                return { isValid: false, error: `Peça ${tile1Data[0]} (lado ${tile1Data[1]}) e Peça ${tile2Data[0]} (lado ${tile2Data[1]}) não se conectam.` };
            }
        }
    }

    // Check for cycles
    for (let i = 0; i < 9; i++) {
        const [piece, side, orientation] = board[i];
        const tileData = gameTiles[piece][side];
        if (tileData.roads) {
            for (const road of tileData.roads) {
                const [c1, c2] = road.connection;
                const g1 = TILE_NODES[i][(c1 + orientation) % 4];
                const g2 = TILE_NODES[i][(c2 + orientation) % 4];
                if (uf.union(g1, g2)) {
                    return { isValid: false, error: "Ciclo detectado na rede de estradas." };
                }
            }
        }
    }

    return { isValid: true, error: null };
}

const STAT_KEYS = [
    "houses", "ufos", "girls", "boys", "dogs", "hamburgers",
    "aliens", "agents", "captured_aliens", "curves"
];

function _calculateCapturedIndices(agents, aliens) {
    const capturedIndices = new Set();
    const sortedAgents = [...agents].sort((a, b) => a.pos - b.pos);

    for (const agent of sortedAgents) {
        const { pos: agentPos, dir: agentDir } = agent;
        let potentialTargets = [];
        if (agentDir === 1) { // Facing forward
            potentialTargets = aliens.filter(a => a.pos > agentPos && !capturedIndices.has(a.pos));
            if (potentialTargets.length > 0) {
                const target = potentialTargets.reduce((min, a) => a.pos < min.pos ? a : min, potentialTargets[0]);
                capturedIndices.add(target.pos);
            }
        } else if (agentDir === 0) { // Facing backward
            potentialTargets = aliens.filter(a => a.pos < agentPos && !capturedIndices.has(a.pos));
            if (potentialTargets.length > 0) {
                const target = potentialTargets.reduce((max, a) => a.pos > max.pos ? a : max, potentialTargets[0]);
                capturedIndices.add(target.pos);
            }
        }
    }
    return capturedIndices;
}

function _findSetsInSequence(road, sequence) {
    if (!sequence.length) return 0;

    const items = road.map(r => r[0]);
    let numSets = 0;
    const usedIndices = new Set();

    // Forward pass
    let seqIdx = 0;
    let currentSetIndices = [];
    for (let i = 0; i < items.length; i++) {
        if (items[i] === "") continue;
        if (items[i] === sequence[seqIdx]) {
            currentSetIndices.push(i);
            seqIdx++;
        } else {
            currentSetIndices = [];
            if (items[i] === sequence[0]) {
                seqIdx = 1;
                currentSetIndices.push(i);
            } else {
                seqIdx = 0;
            }
        }
        if (seqIdx === sequence.length) {
            numSets++;
            currentSetIndices.forEach(idx => usedIndices.add(idx));
            seqIdx = 0;
            currentSetIndices = [];
        }
    }

    // Backward pass
    seqIdx = 0;
    const reversedSequence = [...sequence].reverse();
    for (let i = 0; i < items.length; i++) {
        if (items[i] === "" || usedIndices.has(i)) continue;
        if (items[i] === reversedSequence[seqIdx]) {
            seqIdx++;
        } else {
            if (items[i] === reversedSequence[0]) {
                seqIdx = 1;
            } else {
                seqIdx = 0;
            }
        }
        if (seqIdx === reversedSequence.length) {
            numSets++;
            seqIdx = 0;
        }
    }
    return numSets;
}

function _calculateMaxAliensRunningTowardsAgent(aliens, agent_indices) {
    if (!agent_indices.size) return 0;
    let countRight = 0;
    let countLeft = 0;
    for (const alien of aliens) {
        const { pos: alienPos, dir: alienDir } = alien;
        if (alienDir === 1 && [...agent_indices].some(a_idx => a_idx > alienPos)) {
            countRight++;
        } else if (alienDir === 0 && [...agent_indices].some(a_idx => a_idx < alienPos)) {
            countLeft++;
        }
    }
    return Math.max(countRight, countLeft);
}

function _calculateMaxHamburgersInFrontOfAlien(road, aliens, captured_indices) {
    let maxHamburgers = 0;
    const uncapturedAliens = aliens.filter(a => !captured_indices.has(a.pos));

    for (const alien of uncapturedAliens) {
        const { pos: alienPos, dir: alienDir } = alien;
        let currentHamburgers = 0;

        if (alienDir === 1) { // Facing forward
            for (let i = alienPos + 1; i < road.length; i++) {
                const [item, itemDir] = road[i];
                if (item === "hamburger") {
                    currentHamburgers++;
                } else if (item === "alien" && itemDir === 1 && !captured_indices.has(i)) {
                    break;
                }
            }
        } else if (alienDir === 0) { // Facing backward
            for (let i = alienPos - 1; i >= 0; i--) {
                const [item, itemDir] = road[i];
                if (item === "hamburger") {
                    currentHamburgers++;
                } else if (item === "alien" && itemDir === 0 && !captured_indices.has(i)) {
                    break;
                }
            }
        }
        maxHamburgers = Math.max(currentHamburgers, maxHamburgers);
    }
    return maxHamburgers;
}

function _calculateMaxAliensBetweenAgents(road, agents) {
    let maxAliens = 0;
    for (let i = 0; i < agents.length; i++) {
        for (let j = 0; j < agents.length; j++) {
            if (agents[i].pos >= agents[j].pos) continue;

            const agent1 = agents[i];
            const agent2 = agents[j];

            if (agent1.dir === 1 && agent2.dir === 0) {
                const aliensInBetween = road.slice(agent1.pos + 1, agent2.pos)
                    .filter(([item]) => item === "alien").length;
                maxAliens = Math.max(maxAliens, aliensInBetween);
            }
        }
    }
    return maxAliens;
}

function _processRoadForStats(road) {
    if (!road.length) return {};

    const allItems = { 'alien': [], 'agent': [], 'hamburger': [] };
    road.forEach(([item, direction], i) => {
        if (item in allItems) {
            allItems[item].push({ pos: i, dir: direction });
        }
    });

    const agentIndices = new Set(allItems.agent.map(a => a.pos));
    const capturedIndices = _calculateCapturedIndices(allItems.agent, allItems.alien);

    return {
        num_agents: allItems.agent.length,
        num_aliens: allItems.alien.length,
        aliens_caught: capturedIndices.size,
        max_aliens_running_towards_agent: _calculateMaxAliensRunningTowardsAgent(allItems.alien, agentIndices),
        max_hamburgers_in_front_of_alien: _calculateMaxHamburgersInFrontOfAlien(road, allItems.alien, capturedIndices),
        max_aliens_between_two_agents: _calculateMaxAliensBetweenAgents(road, allItems.agent),
        food_chain_sets: _findSetsInSequence(road, ['agent', 'alien', 'hamburger']),
    };
}

function calculateSolutionStats(solution, gameTiles) {
    const gridSolution = [];
    for(let i = 0; i < 3; i++) {
        gridSolution.push(solution.slice(i*3, i*3+3));
    }

    const stats = {};
    STAT_KEYS.forEach(key => stats[`total_${key}`] = 0);
    stats["total_tiles_without_roads"] = 0;

    for (let i = 0; i < solution.length; i++) {
        const [piece, side] = solution[i];
        const tileData = gameTiles[piece][side];
        for (const key of STAT_KEYS) {
            if (tileData[key]) stats[`total_${key}`] += tileData[key];
        }
        if (!tileData.roads || tileData.roads.length === 0) {
            stats["total_tiles_without_roads"]++;
        }
    }
    
    const roadStats = analyzeRoadNetwork(solution, gameTiles);
    stats["total_captured_aliens"] = roadStats.aliens_caught;
    delete roadStats.aliens_caught;
    Object.assign(stats, roadStats);

    stats["aliens_times_ufos"] = (stats["total_aliens"] - stats["total_captured_aliens"]) * stats["total_ufos"];
    stats["aliens_times_hamburgers"] = (stats["total_aliens"] - stats["total_captured_aliens"]) * stats["total_hamburgers"];
    stats["citizen_dog_pairs"] = Math.min((stats["total_boys"] + stats["total_girls"]), stats["total_dogs"]);

    const adjacencyStats = calculateAdjacencyStats(solution, gameTiles);
    Object.assign(stats, adjacencyStats);
                    
    return stats;
}

function analyzeRoadNetwork(solution, gameTiles) {
    const allRoads = _buildAllRoads(solution, gameTiles);
    
    const aggStats = {
        "total_roads": allRoads.length, "aliens_caught": 0, "max_aliens_running_towards_agent": 0,
        "max_hamburgers_in_front_of_alien": 0, "max_agents_on_one_road": 0, "max_aliens_on_one_road": 0,
        "max_aliens_between_two_agents": 0, "total_food_chain_sets": 0
    };
    
    const roadLengths = [];
    for (const road of allRoads) {
        roadLengths.push(road.length);
        const roadStats = _processRoadForStats(road);
        if (Object.keys(roadStats).length === 0) continue;

        aggStats["aliens_caught"] += roadStats.aliens_caught || 0;
        aggStats["total_food_chain_sets"] += roadStats.food_chain_sets || 0;
        aggStats["max_hamburgers_in_front_of_alien"] = Math.max(aggStats["max_hamburgers_in_front_of_alien"], roadStats.max_hamburgers_in_front_of_alien || 0);
        aggStats["max_aliens_running_towards_agent"] = Math.max(aggStats["max_aliens_running_towards_agent"], roadStats.max_aliens_running_towards_agent || 0);
        aggStats["max_agents_on_one_road"] = Math.max(aggStats["max_agents_on_one_road"], roadStats.num_agents || 0);
        aggStats["max_aliens_on_one_road"] = Math.max(aggStats["max_aliens_on_one_road"], roadStats.num_aliens || 0);
        aggStats["max_aliens_between_two_agents"] = Math.max(aggStats["max_aliens_between_two_agents"], roadStats.max_aliens_between_two_agents || 0);
    }

    if (roadLengths.length > 0) {
        aggStats["longest_road_size"] = Math.max(...roadLengths);
        const counts = roadLengths.reduce((acc, val) => (acc[val] = (acc[val] || 0) + 1, acc), {});
        aggStats["max_roads_of_same_length"] = Math.max(...Object.values(counts));
    } else {
        aggStats["longest_road_size"] = 0;
        aggStats["max_roads_of_same_length"] = 0;
    }
        
    return aggStats;
}

function findLargestComponentSize(gridProperties, propertyKey) {
    let maxSize = 0;
    const visited = new Set();
    for (let r = 0; r < 3; r++) {
        for (let c = 0; c < 3; c++) {
            if ((gridProperties[r][c][propertyKey] > 0) && !visited.has(`${r},${c}`)) {
                let currentSize = 0;
                const q = [[r, c]];
                visited.add(`${r},${c}`);
                while (q.length > 0) {
                    const [currR, currC] = q.shift();
                    currentSize++;
                    for (const [dr, dc] of [[0, 1], [0, -1], [1, 0], [-1, 0]]) {
                        const nextR = currR + dr;
                        const nextC = currC + dc;
                        if (nextR >= 0 && nextR < 3 && nextC >= 0 && nextC < 3 && !visited.has(`${nextR},${nextC}`) && (gridProperties[nextR][nextC][propertyKey] > 0)) {
                            visited.add(`${nextR},${nextC}`);
                            q.push([nextR, nextC]);
                        }
                    }
                }
                maxSize = Math.max(maxSize, currentSize);
            }
        }
    }
    return maxSize;
}

function calculateAdjacencyStats(solution, gameTiles) {
    const gridProperties = Array(3).fill(null).map(() => Array(3).fill(null));
    for (let r = 0; r < 3; r++) {
        for (let c = 0; c < 3; c++) {
            const [piece, side] = solution[r * 3 + c];
            const tileData = gameTiles[piece][side];
            gridProperties[r][c] = {
                'dogs': tileData.dogs || 0,
                'houses': tileData.houses || 0,
                'citizens': (tileData.boys || 0) + (tileData.girls || 0),
                'is_safe': (tileData.aliens || 0) === 0 ? 1 : 0
            };
        }
    }
    
    return {
        "largest_dog_group": findLargestComponentSize(gridProperties, 'dogs'),
        "largest_house_group": findLargestComponentSize(gridProperties, 'houses'),
        "largest_citizen_group": findLargestComponentSize(gridProperties, 'citizens'),
        "largest_safe_zone_size": findLargestComponentSize(gridProperties, 'is_safe')
    };
}

function _buildAllRoads(solution, gameTiles) {
    const adj = Array.from({ length: 42 }, () => []);
    const edgeMap = {};

    for (let r = 0; r < 3; r++) {
        for (let c = 0; c < 3; c++) {
            const [piece, side, orientation] = solution[r * 3 + c];
            const position = r * 3 + c;
            const roads = gameTiles[piece][side].roads || [];

            for (const roadInfo of roads) {
                const [c1, c2] = roadInfo.connection;
                const g1 = TILE_NODES[position][(c1 + orientation) % 4];
                const g2 = TILE_NODES[position][(c2 + orientation) % 4];
                adj[g1].push(g2); 
                adj[g2].push(g1);

                const d = roadInfo.direction !== undefined ? roadInfo.direction : -1;
                let targetNode = -1;
                if (d !== -1) {
                    targetNode = TILE_NODES[position][(d + orientation) % 4];
                }
                const edge = [g1, g2].sort((a, b) => a - b);
                edgeMap[edge.join('-')] = { item: roadInfo.item || '', target_node: targetNode };
            }
        }
    }

    const visitedNodes = new Set();
    const allRoads = [];
    for (let i = 0; i < 42; i++) {
        if (!visitedNodes.has(i) && adj[i].length > 0) {
            const componentNodes = new Set();
            const q = [i];
            visitedNodes.add(i);
            while (q.length > 0) {
                const u = q.shift();
                componentNodes.add(u);
                for (const v of adj[u]) {
                    if (!visitedNodes.has(v)) {
                        visitedNodes.add(v);
                        q.push(v);
                    }
                }
            }

            const endpoints = [...componentNodes].filter(n => adj[n].filter(neighbor => componentNodes.has(neighbor)).length === 1);
            const startNode = endpoints.length > 0 ? endpoints[0] : Math.min(...componentNodes);
            
            const path = [startNode];
            let prev = -1;
            let curr = startNode;
            while (path.length < componentNodes.size) {
                let found = false;
                for (const neighbor of adj[curr]) {
                    if (componentNodes.has(neighbor) && neighbor !== prev) {
                        path.push(neighbor);
                        prev = curr;
                        curr = neighbor;
                        found = true;
                        break;
                    }
                }
                if (!found) break;
            }

            const roadItems = [];
            for (let idx = 0; idx < path.length - 1; idx++) {
                const u = path[idx];
                const v = path[idx + 1];
                const edge = [u, v].sort((a, b) => a - b).join('-');
                if (edgeMap[edge]) {
                    const data = edgeMap[edge];
                    let direction = -1;
                    if (data.target_node !== -1) {
                        direction = data.target_node === v ? 1 : 0;
                    }
                    roadItems.push([data.item, direction]);
                }
            }
            allRoads.push(roadItems);
        }
    }
    return allRoads;
}

// =============================================================================
// Ponto de Entrada da Aplicação
// =============================================================================
document.addEventListener('DOMContentLoaded', async () => {
    if (await loadData()) {
        initializeApp();
    }
});
