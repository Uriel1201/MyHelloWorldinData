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

    m = match.group(1) if (match := re.search(r"FROM\s+(\w+)", query)) else ""

    return m
