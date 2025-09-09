# 🧩 Nine Tiles Panic Solver

This repository contains a comprehensive solver and analysis suite for the board game **Nine Tiles Panic**. The primary goal is to exhaustively generate every possible valid board configuration. After a massive search, a total of **2,922,907,648** unique solutions were found (ignoring the 4 board rotations that are physically identical). The project then performs a deep analysis on these solutions to find the single "best" configuration for any given combination of scoring cards.

The full 4.27 GB dataset of all solutions is publicly available on the Hugging Face Hub:
👉 **Dataset**: [rolim520/Nine-Tiles-Panic-Solutions](https://huggingface.co/datasets/rolim520/Nine-Tiles-Panic-Solutions)

You can try the interactive solution visualizer deployed on **GitHub Pages** here:
👉 **Web App**: [Nine Tiles Panic Solver Interface](https://rolim520.github.io/Nine-Tiles-Panic-Solver/)

## Project at a Glance

* **Objective**: To exhaustively find the single optimal 3x3 board layout for any combination of scoring objectives in the board game *Nine Tiles Panic*.
* **Method**: A two-phase process involving a **parallelized backtracking solver** to generate all valid layouts, followed by a **large-scale data analysis** script using DuckDB to pre-calculate the best configuration for every scenario.
* **Key Technologies**: Python (`multiprocessing`), DuckDB, and PyArrow (Parquet) for the backend solver; Brython and JavaScript for the interactive web UI.
* **Result**: A total of **2,922,907,648** unique valid solutions were discovered. The optimal layout was found for all **2,625 scorable card combinations**.

![](./images/interface.png)

## Table of Contents

1.  [The Game: Nine Tiles Panic](#the-game-nine-tiles-panic)
2.  [The Goal: Finding the "Best" Town](#the-goal-finding-the-best-town)
    * [The Scoring Cards](#the-scoring-cards)
3.  [Key Findings & Game Limits](#key-findings--game-limits)
4.  [The Combinatorial Challenge](#the-combinatorial-challenge)
5.  [How the Solver Works](#how-the-solver-works)
    * [Phase 1: Generating All Valid Solutions](#phase-1-generating-all-valid-solutions)
    * [Phase 2: Post-Processing and Analysis](#phase-2-post-processing-and-analysis)
6.  [Key Technical Decisions & Optimizations](#key-technical-decisions--optimizations)
7.  [Game Modeling and Scoring System](#game-modeling-and-scoring-system)
    * [Board and Tile Representation](#board-and-tile-representation)
    * [Individual Card Score (Rank-Based Score)](#individual-card-score-rank-based-score)
    * [Combined Score (Geometric Mean)](#combined-score-geometric-mean)
8.  [How to Run the Code](#how-to-run-the-code)
    * [Step 1: Install Dependencies](#step-1-install-dependencies)
    * [Step 2: Download the Solutions Dataset (Optional)](#step-2-download-the-solutions-dataset-optional)
    * [Step 3: Analyze and Find Best Solutions](#step-3-analyze-and-find-best-solutions)
    * [Step 4: Generate Solutions from Scratch (Computationally Intensive)](#step-4-generate-solutions-from-scratch-computationally-intensive)
    * [Step 5: Run the Interactive Interface](#step-5-run-the-interactive-interface)
9.  [Future Work & Potential Improvements](#future-work--potential-improvements)
10. [Data & Directory Structure](#data--directory-structure)

-----

## The Game: Nine Tiles Panic

**Nine Tiles Panic** is a real-time puzzle game where players race to build a 3x3 city grid using their set of nine double-sided tiles. In each round, three "Theme Cards" are revealed, which set the scoring objectives for everyone.  Players must arrange their tiles to create a valid road network while simultaneously trying to score as highly as possible on the three objectives. A town is considered valid only if all roads on adjacent tiles connect correctly and do not form closed loops.

![](./images/game.png)

## The Goal: Finding the "Best" Town

While the game is played in real-time against other players, this project poses a different question: **What is the single best possible 3x3 town for any combination of scoring objectives?**

This solver was built to answer that question by:

1.  Finding every single valid arrangement of the nine tiles.
2.  Calculating dozens of statistics for each valid arrangement (e.g., number of houses, length of roads, etc.).
3.  Determining the optimal arrangement for every possible combination of scoring cards.

### The Scoring Cards

The game includes 26 "Theme Cards," each defining a unique scoring objective. However, one card, *"Finish Quickly!"* (#4), rewards the fastest player and is not based on the final board layout. Therefore, it is excluded from this analysis.

This leaves **25 scorable objective cards**. The solver calculates the best layout for every possible game scenario:

* **25 combinations** for a single objective card.
* **300 combinations** for any pair of objective cards ($_{25}C_2$).
* **2,300 combinations** for any trio of objective cards ($_{25}C_3$).

In total, the project finds the single best solution for **2,625 unique scoring scenarios**.

-----

## Key Findings & Game Limits

The analysis of all ~2.9 billion solutions established the definitive range of possible values for every scorable statistic in the game. 

#### 🏙️ Town & Items
* **Total Agents**: 0 to 5
* **Total Captured Aliens**: 0 to 4
* **Total Boys**: 0 to 4
* **Total Girls**: 0 to 3
* **Total Dogs**: 1 to 6
* **Total Houses**: 1 to 6
* **Citizen + Dog Pairs**: 0 to 5
* **Aliens x Hamburgers**: 0 to 20
* **Aliens x UFOs**: 0 to 8

#### 🚗 Road Network
* **Total Roads**: 1 to 6
* **Longest Road Size**: 2 to 11 segments
* **Total Curves**: 1 to 11
* **Max Roads of Same Length**: 1 to 5
* **Tiles Without Roads**: 0 to 3

#### 🧑‍🤝‍🧑 Adjacency & Grouping
* **Largest Dog Group**: 1 to 5 tiles
* **Largest House Group**: 1 to 5 tiles
* **Largest Citizen Group**: 0 to 6 tiles
* **Largest Safe Zone (No Aliens)**: 1 to 9 tiles

#### 👽 Alien & Agent Interactions
* **Max Aliens Between Two Agents**: 0 to 4
* **Max Agents on One Road**: 0 to 4
* **Max Aliens on One Road**: 0 to 4
* **Max Aliens Running Towards an Agent**: 0 to 4
* **Max Hamburgers in Front of an Alien**: 0 to 7
* **Total Food Chain Sets**: 0 to 3
-----

## The Combinatorial Challenge

The search space for this puzzle is immense. Breaking it down, we have:

  * **Tile Placement**: There are 9 tiles, leading to $9\!$ (362,880) ways to place them on the grid.
  * **Tile Sides**: Each of the 9 tiles is double-sided, resulting in $2^9$ (512) possible side combinations.
  * **Tile Orientation**: Each tile can be rotated in 4 ways, giving $4^9$ (262,144) orientation combinations.

The total number of theoretical arrangements is:

$9! \times 2^9 \times 4^9 = 362,880 \times 512 \times 262,144 = 48,704,929,136,640$

This number is astronomically large. However, the vast majority of these are invalid because of the strict road connection rule. The solver's first job is to navigate this space to find only the valid layouts. This number is also accounting for the 4 city arrangement orientations, that in practice represents the same board.

-----

## How the Solver Works

The project is divided into two main phases: **Generation** and **Analysis**.

### Phase 1: Generating All Valid Solutions

This phase is handled by `main.py` and `solver.py`. The goal is to intelligently search the massive combinatorial space and save every valid board configuration.

  * **Algorithm**: An optimized **backtracking search algorithm** with **constraint propagation** and **forward checking** is used (`solver.py`). Instead of blindly trying every combination, it places one tile at a time. After each placement, it prunes entire branches of the search tree that cannot possibly lead to a valid solution.
  * **Key Optimizations**:
      * **Parallel Processing**: The entire search is split into smaller, independent tasks that are executed in parallel across all available CPU cores using Python's `multiprocessing` library.
      * **Union-Find Data Structure**: A `UnionFind` class is used to instantly detect if placing a tile would create an illegal closed loop in the road network, allowing for extremely fast validation.
  * **Output**: The discovered solutions, along with their pre-calculated statistics, are written in chunks to `.parquet` files inside the `generated_solutions/` directory. The Parquet format is highly efficient for storing large-scale tabular data.

### Phase 2: Post-Processing and Analysis

Once all solutions are generated into a 5 GB Parquet file, `post_process.py` takes over to analyze them. This script relies heavily on **DuckDB**, an in-process analytical database perfect for running complex queries on large Parquet files without needing to load them into memory.

The analysis follows these steps:

1.  **Create a Database View**: A DuckDB view is created that points directly to the Parquet file. This is an instantaneous, zero-copy operation.
2.  **Calculate Percentiles**: To create a fair scoring system, the script first calculates the percentile rank for every possible value of each statistic. This normalizes all objectives onto a consistent 0-100 scale.
3.  **Pre-compute All Scores**: A new table, `solution_scores`, is created. In this table, every solution is scored from 0 to 100 for each of the 25 scorable cards based on the calculated percentiles.
4.  **Find the Best**: The script then queries the `solution_scores` table to find the optimal solution for every combination:
    * The single best solution for each of the **25 scorable cards**.
    * The best solution for all **300 unique pairs** of cards.
    * The best solution for all **2,300 unique trios** of cards.

-----

## Game Modeling and Scoring System

### Board and Tile Representation

  * **Board Graph**: The 3x3 grid is abstractly modeled as a graph with **24 nodes**, where each node represents a connection point on the edge of the grid. This topology is defined in `constants.py`.
  * **Tile Data**: The properties of each tile (items, road connections, etc.) are defined in `game/tiles/tiles.json`.

  ```json
{
      "houses": 0,
      "ufos": 0,
      "girls": 0,
      "boys": 1,
      "dogs": 0,
      "hamburgers": 1,
      "aliens": 0,
      "agents": 0,
      "captured_aliens": 0,
      "curves": 0,
      "roads": [
           {
           "connection": [0,2],
           "item": "",
           "direction": -1
           },
           {
           "connection": [1,3],
           "item": "hamburger",
           "direction": -1
           }
      ]
 },
  ``` 

  * **Card Objectives**: The scoring cards and the specific statistic they correspond to are defined in `game/cards/cards.json`.

  ```json
{
      "number": 1,
      "name": "Capture",
      "description": "Most Aliens caught",
      "key": "total_captured_aliens",
      "type": "max"
 },
 {
      "number": 2,
      "name": "Surrounded",
      "description": "Most Aliens stuck between 2 Agents",
      "key": "max_aliens_between_two_agents",
      "type": "max"
 },
  ```

### Individual Card Score (Rank-Based Score)

A solution's score for a single card is based on the **percentile rank of its statistic's value**. This method normalizes all objectives onto a consistent 0-100 scale, where the score represents how good a value is relative to all ~2.9 billion solutions.

* **`max` type cards** (e.g., "Most houses"): The score is the direct rank-based percentile. A value in the 99th percentile gets a score of 99.
    $Score = RankPercentile$
* **`min` type cards** (e.g., "Fewest roads"): The score is the inverse of the rank-based percentile. A value in the 1st percentile (very low) gets a score of 99.
    $Score = 100 - RankPercentile$

### Combined Score (Geometric Mean)

When evaluating a combination of 2 or 3 cards, a simple average is insufficient. A high score on one card could mask a terrible score on another. To find truly balanced and versatile solutions, the **geometric mean** is used.

The geometric mean strongly rewards solutions that perform well across *all* objectives and heavily penalizes any solution that scores poorly on even one objective. If any card scores a 0, the entire combined score becomes 0.

* **For a pair of cards**: $Score_{pair} = \sqrt{Score_{card1} \times Score_{card2}}$
* **For a trio of cards**: $Score_{trio} = \sqrt[3]{Score_{card1} \times Score_{card2} \times Score_{card3}}$

To ensure numerical stability when multiplying many scores, the calculation is performed using logarithms in the SQL query within `post_process.py`.

-----

## How to Run the Code

**Prerequisites:** Python 3.8+ and the packages in `requirements.txt`.

### Step 1: Install Dependencies

It is recommended to use a virtual environment.

```bash
# Install all dependencies from the requirements file
pip install -r requirements.txt
```

### Step 2: Download the Solutions Dataset (Optional)

To run the analysis (`post_process.py`) without first generating the solutions yourself, you can download the complete dataset from Hugging Face.

1.  Create a directory for the data: `mkdir generated_solutions`
2.  Download the `tiling_solutions.parquet` file from [**Hugging Face**](https://huggingface.co/datasets/rolim520/Nine-Tiles-Panic-Solutions) and place it inside the `generated_solutions/` directory.

### Step 3: Analyze and Find Best Solutions

Once the dataset is in place, run the post-processing script. This script will analyze the data and produce the final JSON files used by the web interface.

```bash
python3 post_process.py
```

### Step 4: Generate Solutions from Scratch (Computationally Intensive)

If you wish to generate the solutions yourself instead of downloading them:

```bash
python3 main.py
```

**Important**: This can take **hours or days** to complete. It will generate the `tiling_solutions.parquet` file (\~4.27 GB) in the `generated_solutions/` directory.

### Step 5: Run the Interactive Interface

1.  **Navigate to the `docs` directory**: `cd docs`
2.  **Start a local web server**: `python3 -m http.server`
3.  **Open your browser** to `http://localhost:8000`.

## Data & Directory Structure

  * **/docs**: Contains the static web interface and final JSON data.
      * **/docs/data/best\_solutions.json**: The key output file with the optimal layout for all 2,625 card combinations.
  * **/generated\_solutions**: A local directory where the large `.parquet` file containing all solutions is stored. **Note**: This data is not in the repository due to its size. It can be downloaded from [Hugging Face](https://huggingface.co/datasets/rolim520/Nine-Tiles-Panic-Solutions) or generated locally.
  * **/game**: Contains the JSON definitions for game tiles and cards.
  * **main.py**: The entry point script to start the parallel solution generation.
  * **solver.py**: Implements the core backtracking search algorithm.
  * **post\_process.py**: The script for analyzing generated solutions with DuckDB.
  * **analysis.py**: Contains all functions for calculating statistics for a board layout.
