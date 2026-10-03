import os
import csv
import heapq
import tempfile
import shutil
from operator import itemgetter
from multiprocessing import Pool, cpu_count

def _make_key_extractor(sort_keys, delimiter=','):
    """Return key extraction callable for a raw CSV line."""
    if len(sort_keys) == 1:
        col = sort_keys[0]
        return lambda line: (
            next(csv.reader([line], delimiter=delimiter))[col]
            if '"' in line
            else line.rstrip('\r\n').split(delimiter)[col]
        )
    getter = itemgetter(*sort_keys)
    return lambda line: (
        getter(next(csv.reader([line], delimiter=delimiter)))
        if '"' in line
        else getter(line.rstrip('\r\n').split(delimiter))
    )

def _sort_chunk_worker(args):
    """Sort a byte range of input file and write sorted run to disk."""
    file_loc, start, end, run_path, sort_keys, delimiter = args
    with open(file_loc, 'r', buffering=1024 * 1024, encoding='utf-8', errors='replace') as f:
        f.seek(start)
        data = f.read(end - start)

    lines = data.splitlines(keepends=True)
    extractor = _make_key_extractor(sort_keys, delimiter)
    keyed = [(extractor(line), line) for line in lines]
    keyed.sort(key=itemgetter(0))

    with open(run_path, 'w', buffering=1024 * 1024, encoding='utf-8') as out:
        out.writelines(line for _, line in keyed)
    return run_path

def _yield_run(file_path, extractor):
    """Stream (key, raw_line) pairs from a sorted run."""
    with open(file_path, 'r', buffering=256 * 1024, encoding='utf-8', errors='replace') as f:
        for line in f:
            yield (extractor(line), line)

def _merge_files_task(args):
    """Merge a batch of sorted runs directly to an output file."""
    files, out_path, sort_keys, delimiter, header_line = args
    extractor = _make_key_extractor(sort_keys, delimiter)
    with open(out_path, 'w', buffering=2 * 1024 * 1024, encoding='utf-8') as out:
        if header_line:
            out.write(header_line if header_line.endswith('\n') else header_line + '\n')
        generators = [_yield_run(f, extractor) for f in files]
        for _, line in heapq.merge(*generators, key=itemgetter(0)):
            out.write(line)
    for f in files:
        try:
            os.remove(f)
        except OSError:
            pass
    return out_path

def external_sort(file_loc, sort_keys, n_proc=None, n_way=64, max_size=100,
                  header=True, delimiter=',', overwrite=False):
    """Sort a CSV file on disk using parallel chunking and K-way merge."""
    if n_proc is None:
        n_proc = cpu_count() or 4
    n_way = max(2, n_way)

    file_size = os.path.getsize(file_loc)
    if file_size == 0:
        return

    base, ext = os.path.splitext(file_loc)
    final_target = file_loc if overwrite else f'{base}_sorted{ext}'

    tmp_dir = tempfile.mkdtemp(prefix='py_ext_sort_')
    tmp_target = os.path.join(tmp_dir, 'final_sorted.csv')

    try:
        header_line = None
        start_offset = 0
        with open(file_loc, 'r', encoding='utf-8', errors='replace') as f:
            if header:
                header_line = f.readline()
                start_offset = f.tell()

        chunk_bytes = int(max_size * 1024 * 1024)
        chunks = []
        idx = 0
        with open(file_loc, 'rb') as f:
            f.seek(start_offset)
            curr = start_offset
            while curr < file_size:
                target = curr + chunk_bytes
                if target >= file_size:
                    run_path = os.path.join(tmp_dir, f'run_{idx}.csv')
                    chunks.append((file_loc, curr, file_size, run_path, sort_keys, delimiter))
                    break
                f.seek(target)
                f.readline()
                end = f.tell()
                run_path = os.path.join(tmp_dir, f'run_{idx}.csv')
                chunks.append((file_loc, curr, end, run_path, sort_keys, delimiter))
                curr = end
                idx += 1

        # Phase 1: Parallel in-memory chunk sorting
        with Pool(processes=n_proc) as pool:
            active_runs = pool.map(_sort_chunk_worker, chunks)

        # Phase 2: K-Way Merge
        if len(active_runs) == 1:
            single_run = active_runs[0]
            if header_line:
                with open(single_run, 'r', encoding='utf-8', errors='replace') as rf:
                    content = rf.read()
                with open(tmp_target, 'w', encoding='utf-8') as wf:
                    wf.write(header_line if header_line.endswith('\n') else header_line + '\n')
                    wf.write(content)
            else:
                shutil.move(single_run, tmp_target)
        else:
            merge_round = 0
            with Pool(processes=n_proc) as pool:
                while len(active_runs) > 1:
                    is_final = (len(active_runs) <= n_way)
                    tasks = []
                    next_runs = []
                    for i in range(0, len(active_runs), n_way):
                        batch = active_runs[i:i + n_way]
                        if len(batch) == 1 and not is_final:
                            next_runs.append(batch[0])
                            continue
                        if is_final and len(batch) == len(active_runs):
                            out_p = tmp_target
                            h = header_line
                        else:
                            out_p = os.path.join(tmp_dir, f'm_{merge_round}_{i}.csv')
                            h = None
                            next_runs.append(out_p)
                        tasks.append((batch, out_p, sort_keys, delimiter, h))
                    pool.map(_merge_files_task, tasks)
                    active_runs = next_runs
                    merge_round += 1

        shutil.move(tmp_target, final_target)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
