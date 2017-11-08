import os
import csv
import heapq
import hashlib
from multiprocessing import Queue, Pool

def create_temp_files(file_loc, max_size, header=True):
    """
    Splits input file into smaller chunks as dictated by max_size. Files
    are put into a temporary directory for sorting and merging

    Inputs
    ------
    file_loc : str
        File location of csv to sort
    max_size : float
        Size of maximum file in mb
    header : bool
        Whether is a header record or not

    Returns
    -------
    temp_files : list
        List of temporary file locations
    header_cols : str
        String of header record
    """
    max_size *= 1048576
    os.makedirs('./.tmp')
    
    data = ''
    file_num = 0
    temp_files = []
    write_file = None
    header_cols = None
    with open(file_loc) as f:
        while True:
            row = f.readline()
            if not row:
                break
            elif header and not header_cols:
                header_cols = row
                continue
            
            if not write_file:
                file_name = './.tmp/split{0}.csv'.format(file_num)
                temp_files.append(file_name)
                write_file = open(file_name, 'w')
            
            data += row
            if len(data) >= max_size:
                file_num += 1

                """
                TODO: Can't use max size larger then 2gb because of Python 
                      issue 24658. Will throw an oserr 22 here.
                """

                write_file.write(data)
                write_file.close()
                
                data = ''
                write_file = None

        #catch last file
        if write_file:
            write_file.write(data)
            write_file.close()

    return temp_files, header_cols

def sort_file(file_loc, sort_keys, delimiter=','):
    """
    Sorts a file in memory based on the index of the columns in sortkeys
    
    Inputs
    ------
    file_loc : str
        Location of file to sort
    sort_keys : list
        Indices of columns to sort on
    delimiter : str
        Delimiter to split columns on
    """
    #must use csv library to ensure quotes are respected
    with open(file_loc) as f:
        rows = [row for row in csv.reader(f, delimiter=delimiter)]
    rows.sort(key=lambda row: [row[i] for i in sort_keys])

    with open(file_loc, 'w') as f:
        w = csv.writer(f, delimiter=delimiter)
        for row in rows:
            w.writerow(row)

def merge_files(files, sort_keys, header=None, delimiter=','):
    """
    Merges already sorted files and writes to disk

    Inputs
    ------
    files : list
        List of sorted files to merge
    sort_keys : list
        Indices of columns to sort on
    header : list
        List of header values
    delimiter : str
        Delimiter to split columns on
    """
    #generate unique file name with hash
    m = hashlib.md5()
    m.update((''.join(files)).encode('utf-8'))
    merge_name = './.tmp/' + m.hexdigest() + '.csv'

    with open(merge_name, 'w') as out:
        w = csv.writer(out, delimiter=delimiter)
        
        #write header on final merge
        if header:
            w.writerow(header)

        sort_func = lambda row: [row[i] for i in sort_keys]
        gen_files = [_yield_rows(f, delimiter=delimiter) for f in files]
        for row in heapq.merge(*gen_files, key=sort_func):
            w.writerow(row)

    for f in files:
        os.remove(f)

def _yield_rows(file_loc, delimiter=','):
    with open(file_loc) as f:
        for row in csv.reader(f):
            yield row

def _build_merge_tasks(files, n_way):
    batch = []
    merge_tasks = []
    for f in files:
        batch.append(f)
        if len(batch) == n_way:
            merge_tasks.append(batch)
            batch = []
    if batch:
        merge_tasks.append(batch)
    
    return merge_tasks

def external_sort(file_loc, sort_keys, n_proc, n_way=2, max_size=100,
                  header=True, delimiter=',', overwrite=False):
    
    """
    Sort a file on disk rather than in memory. Many factors can make this
    method of sorting faster or slower, but in general more processes and
    merge ways is faster. Size of temporary files is a 'just right' situation
    where too big can exceed memory and too small results in i/o limiatations.

    Inputs
    ------
    file_loc : str
        Location of file to sort
    sort_keys : list
        Indices of columns to sort on
    n_proc : int
        Number of processes to spawn
    n_way : int
        Number of files to merge at once
    max_size : float
        Maximum temporary file size in mb
    header : bool
        Whether the file has a header
    delimiter : str
        Delimiter to split columns on
    overwrite : bool
        Whether to overwrite the original file or not
    """
    import time
    temp_files, header_row = create_temp_files(file_loc, max_size, header)
    header_row = header_row.strip().split(delimiter)
    
    #sort split files
    s_tasks = []
    for f in temp_files:
        s_tasks.append(f)

    s_pool = Pool(processes=n_proc) 
    while s_tasks:
        args = (s_tasks.pop(), sort_keys, delimiter)
        s_pool.apply_async(args)
    
    s_pool.close()
    s_pool.join()
    
    #merge split files
    while len(os.listdir('./.tmp/')) > 1:
        m_pool = Pool(processes=n_proc)
        m_files = ['./.tmp/' + f for f in os.listdir('./.tmp/')]
        m_tasks = _build_merge_tasks(m_files, n_way)
        
        #catch last merge and add header
        if len(m_tasks) == 1:
            header_arg = (header_row, )
        else:
            header_arg = (None, )

        while m_tasks:
            args = (m_tasks.pop(), sort_keys) + header_arg
            m_pool.apply_async(merge_files, args)
        
        m_pool.close()
        m_pool.join()
    
    #clean up tmp directory
    if overwrite:
        new_file_loc = file_loc
    else:
        file_ext = file_loc.split('.')[-1]
        file_name = '.'.join(file_loc.split('.')[:-1])
        new_file_loc = file_name + '_sorted.' +  file_ext
    
    merged_file = './.tmp/' + os.listdir('./.tmp/')[0]
    os.rename(merged_file, new_file_loc)
    os.rmdir('./.tmp/')
