def chunk_string(string, chunk_size):
    str_len = len(string)
    if str_len % chunk_size == 0:
        n_chunks = str_len // chunk_size
    else:
        n_chunks = str_len // chunk_size + 1
    return (string[i*chunk_size:(i+1)*chunk_size] for i in range(n_chunks))