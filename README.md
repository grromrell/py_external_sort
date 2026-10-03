# py_external_sort

Disk-backed multi-process external sort for CSV files larger than RAM.

## Usage

```python
from external_sort import external_sort

external_sort(
    file_loc="huge_input.csv",
    sort_keys=[0],        # Column indices to sort on
    n_proc=8,             # Parallel sorting processes
    max_size=200,         # Run chunk size in MB
    header=True,
    delimiter=",",
    overwrite=False,      # Outputs huge_input_sorted.csv
)
```

## Options

| Parameter | Default | Description |
| :--- | :--- | :--- |
| `file_loc` | *required* | Path to CSV file. |
| `sort_keys` | *required* | List of 0-based column indices. |
| `n_proc` | `cpu_count()` | Worker process count. |
| `n_way` | `64` | Merge fan-out (runs merged per pass). |
| `max_size` | `100` | Chunk buffer in MB before spilling sorted run. |
| `header` | `True` | Preserve first line as header. |
| `delimiter` | `','` | Field delimiter. |
| `overwrite` | `False` | Overwrite input file or write `<name>_sorted.<ext>`. |

## Benchmarks

Tested on Apple Silicon (11 cores, NVMe SSD):

| Dataset | Rows | csvsort 1.3 | Baseline (2017) | Optimized Engine |
| :--- | :--- | :--- | :--- | :--- |
| **2 GB** | 40.7M | — | 234.8s (3.9 min) | **27.6s (8.5x)** |
| **20 GB** | 406.9M | 19,006s (316 min) | 1,860s (31 min) | **725.5s (12.1 min)** |