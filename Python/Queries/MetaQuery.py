import re

# ============================================================
# get_Query:
# params:
# ============================================================
def get_Query(filename: str) -> str:

    try:
        
        with open(filename, 'r', encoding='utf-8') as file:
            
            return file.read()
            
    except FileNotFoundError:
        
        return f"Error:'{filename}' does not exist in directory."
# ============================================================

# ============================================================
# get_TableName:
# params:
# ============================================================
def get_TableName(query: str) -> str:

    match := re.search(r"FROM\s+(\w+)", query))
    
    if not match:
        
        raise ValueError(f'Unable to locate the 
                           table name in your SQL query') 
    return match.group(1)
