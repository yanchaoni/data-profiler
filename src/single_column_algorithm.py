def per_table_evaluate(func_str="", table_indexes=0, col_names="", arg_str=None):
    if isinstance(col_names, str):
        col_names="'" + col_names + "'"
    func = f"tables[{table_indexes}]." + func_str + f"({col_names}, {arg_str})"
    print(func)
    return eval(func)

def single_column_evaluate(func_str="", table_indexes=0, col_names="", arg_str=None):
    profile=[]
    if isinstance(table_indexes, int): # 1 -> 1/many
        profile += [per_table_evaluate(func_str, table_indexes, col_names, arg_str)]
    else: # many -> many
        assert not isinstance(col_names, str), "Column names for each table are required!"
        assert len(table_indexes) ==  len(col_names), f"Number of tables and number of column sets does not match: {len(table_indexes)} != {len(col_names)}"
        for table_index, col_name in zip(table_indexes, col_names):
            profile += [per_table_evaluate(func_str, table_index, col_name, arg_str)]
    return profile