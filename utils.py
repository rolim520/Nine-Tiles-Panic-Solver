# utils.py
import os
import re
import pyarrow as pa
import pyarrow.parquet as pq

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

    def _write_chunk(self):
        table = pa.Table.from_pylist(self._solutions_chunk)
        if self.writer is None:
            self.writer = pq.ParquetWriter(self.file_path, table.schema)
        self.writer.write_table(table)
        self._solutions_chunk = []
        # Create a prefix for the log message
        log_prefix = f"[Worker #{self.worker_id}]" if self.worker_id is not None else ""
        
        # Modify the print statement to be more informative
        print(f"{log_prefix} ... Wrote chunk. Total solutions for this worker: {self.total_solutions_found}")
        
        self._solutions_chunk = []
        
    def process_solutions(self, solution_generator):
        for solution in solution_generator:
            self._solutions_chunk.append(solution_to_flat_dict(solution))
            self.total_solutions_found += 1
            if len(self._solutions_chunk) >= self.chunk_size:
                self._write_chunk()