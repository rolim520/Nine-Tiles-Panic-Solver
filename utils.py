# utils.py
import os
import re
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from analysis import calculate_solution_stats

def get_next_filename(directory, base_name="solutions", extension="parquet"):
    """
    Finds the next available indexed filename in a directory.
    Example: If solutions_1.parquet exists, this will return 'solutions/solutions_2.parquet'.
    """
    # Ensure the output directory exists
    os.makedirs(directory, exist_ok=True)

    # Regex to find files like 'solutions_1.parquet', 'solutions_25.parquet', etc.
    pattern = re.compile(rf"{base_name}_(\d+)\.{extension}")
    
    max_index = 0
    # Check existing files in the directory to find the highest index
    for filename in os.listdir(directory):
        match = pattern.match(filename)
        if match:
            index = int(match.group(1))
            if index > max_index:
                max_index = index
                
    # The new file will have the next index
    new_index = max_index + 1
    new_filename = f"{base_name}_{new_index}.{extension}"
    
    # Return the full path for the new file
    return os.path.join(directory, new_filename)

def solution_to_flat_dict(solution):
    """Converts a 3x3 grid solution into a flat dictionary for a DataFrame."""
    flat_data = {}
    for r, row_data in enumerate(solution):
        for c, tile_data in enumerate(row_data):
            flat_data[f'piece_{r}{c}'] = tile_data[0]
            flat_data[f'side_{r}{c}'] = tile_data[1]
            flat_data[f'orient_{r}{c}'] = tile_data[2]
    return flat_data

class SolutionWriter:
    """Manages writing solutions to a Parquet file in chunks."""
    def __init__(self, file_path, chunk_size=100_000, silent=False, worker_id=None):
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.silent = silent
        self.worker_id = worker_id
        self.writer = None
        self._solutions_chunk = []
        self.total_solutions_found = 0

    def __enter__(self):
        # The os.remove logic is now gone. We just return self.
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._solutions_chunk:
            self._write_chunk()
        if self.writer:
            self.writer.close()
        if not self.silent:
            print("\n-------------------------------------------")
            print(f"✅ Finished! Found and saved a total of {self.total_solutions_found} solutions.")
            print("-------------------------------------------")
    
    def _get_schema(self):
        """Creates the data type schema for the DataFrame."""
        if hasattr(self, '_schema') and self._schema:
            return self._schema

        # Define the types for all columns
        schema = {}
        # 1. Grid layout columns (piece, side, orient)
        for r in range(3):
            for c in range(3):
                schema[f'piece_{r}{c}'] = 'uint8'
                schema[f'side_{r}{c}'] = 'uint8'
                schema[f'orient_{r}{c}'] = 'uint8'

        # 2. Statistical columns (totals, etc.)
        stat_keys = [
            # Original keys
            "total_houses", "total_ufos", "total_girls", "total_boys", "total_dogs",
            "total_hamburgers", "total_aliens", "total_agents", "total_captured_aliens",
            "total_curves", "total_tiles_without_roads",
            "largest_dog_group", "largest_house_group", "largest_citizen_group",
            "largest_safe_zone_size", "total_roads", "max_aliens_running_towards_agent",
            "max_hamburgers_in_front_of_alien", "max_agents_on_one_road",
            "max_aliens_on_one_road", "max_aliens_between_two_agents",
            "total_food_chain_sets", "longest_road_size", "max_roads_of_same_length",
            "aliens_times_ufos", "aliens_times_hamburgers", "citizen_dog_pairs"
        ]
        for key in stat_keys:
             # Use a 8-bit integer (0-255), which is more than enough for counts.
            schema[key] = 'uint8'

        self._schema = schema
        return self._schema

    def _write_chunk(self):
        """Converts the chunk to a DataFrame, applies the schema, and writes to Parquet."""
        if not self._solutions_chunk:
            return

        # 1. Convert list of dicts to a Pandas DataFrame
        df = pd.DataFrame(self._solutions_chunk)

        # 2. Get the predefined schema and apply it
        schema = self._get_schema()
        # Ensure all columns exist in the DataFrame before trying to set the type
        # This handles cases where some stat columns might not be present in all chunks
        applicable_schema = {col: dtype for col, dtype in schema.items() if col in df.columns}
        df = df.astype(applicable_schema)

        # 3. Convert the typed DataFrame to a PyArrow Table
        table = pa.Table.from_pandas(df, preserve_index=False)

        # 4. Write to Parquet file
        if self.writer is None:
            self.writer = pq.ParquetWriter(self.file_path, table.schema)
        self.writer.write_table(table)

        # Logging and cleanup
        log_prefix = f"[Worker #{self.worker_id}]" if self.worker_id is not None else ""
        print(f"{log_prefix} ... Wrote chunk. Total solutions for this worker: {self.total_solutions_found}")
        self._solutions_chunk = []
        
    def process_solutions(self, solution_generator, game_tiles):
        """
        Consumes solutions, calculates stats, and writes the combined data to the file.
        """
        # Unpack the solution and the uf object
        for solution, uf_structure in solution_generator:
            flat_solution = solution_to_flat_dict(solution)

            # Pass the uf_structure to the stats calculation!
            solution_stats = calculate_solution_stats(solution, game_tiles, uf_structure)
            
            # 3. Merge the two dictionaries into a single record.
            #    This combines the grid layout with the calculated totals.
            combined_data = {**flat_solution, **solution_stats}
            
            # 4. Append the complete, combined data to the chunk.
            self._solutions_chunk.append(combined_data)
            self.total_solutions_found += 1
            if len(self._solutions_chunk) >= self.chunk_size:
                self._write_chunk()