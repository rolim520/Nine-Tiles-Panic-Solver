import pyarrow.parquet as pq

metadata = pq.read_metadata('generated_solutions/tiling_solutions_1.parquet')
print(f"Total de entradas: {metadata.num_rows}")
