def per_table_evaluate(func_str="", table_indexes=0, col_indexes=0):
        if isinstance(col_indexes, "int"):
            # to do
        else:
            for col_index in col_indexes: # 1 -> many
                # to do
    return None

def single_column_evaluate(func_str="", table_indexes=0, col_indexes=0):
    profile = []
    if isinstance(table_indexes, "int"): # 1 -> 1/many
        profile += per_table_evaluate(func_str, table_indexes, col_indexes)
    else: # many -> namy
        assert len(table_indexes) ==  len(col_indexes), "f'Number of tables and number of column sets \
            does not match: {len(table_indexes)} != {len(col_indexes)}"
        for table_index in table_indexes:
            profile += per_table_evaluate(func_str, table_index, col_indexes)
    return None

