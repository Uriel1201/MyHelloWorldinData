from dataclasses import dataclass
import pyarrow as pa

@dataclass
class MyArrowTable:
    table: pa.Table
    alias: str
