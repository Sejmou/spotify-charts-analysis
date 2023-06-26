def split_into_chunks_of_size(list_to_split: list, chunk_size: int):
    return [
        list_to_split[i : i + chunk_size]
        for i in range(0, len(list_to_split), chunk_size)
    ]
