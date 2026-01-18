To both classes via their config files, I want to add the concept of extra schema. This will be a dictionary of columns to types. For every entry in this dictionary, I want the column of the appropriate type stored in the DuckDB database, and I want that same column available in the Parquet file if Parquet files are used.

The values for these extra columns should be in data with all of the other fields. If they are not there, then the value should be null.

## Q&A

**1. Supported types:**
Use PyArrow type names for the different types (e.g., `"string"`, `"int64"`, `"float64"`, `"bool"`, `"timestamp[us]"`, etc.).

**2. Type mapping:**
Map PyArrow type names to DuckDB types:
- `"string"` → `VARCHAR`
- `"int64"` → `BIGINT`
- `"int32"` → `INTEGER`
- `"float64"` → `DOUBLE`
- `"float32"` → `FLOAT`
- `"bool"` → `BOOLEAN`
- `"timestamp[us]"` / `"timestamp[ns]"` → `TIMESTAMP`
- etc.

**3. Existing data handling:**
This will only apply to newly created tables. No ALTER TABLE migrations for existing tables.

**4. Key naming conflicts:**
If an `extra_schema` column name conflicts with an existing column (e.g., `key`, `timestamp`, `data`, `version`, `process_name`, etc.), an exception should be thrown at initialization.

**5. Parquet behavior for `collection_fast_persist`:**
Only apply Parquet logic to `dated_fast_persist` for now. `collection_fast_persist` does not have Parquet export.

**6. Config structure:**
```python
@dataclass
class WALConfig:
    # ... existing fields ...
    extra_schema: dict[str, str] | None = None  # column_name -> PyArrow type name

@dataclass
class CollectionConfig:
    # ... existing fields ...
    extra_schema: dict[str, str] | None = None  # column_name -> PyArrow type name
```

**Example usage:**
```python
config = WALConfig(
    extra_schema={
        "customer_id": "int64",
        "price": "float64",
        "is_active": "bool",
        "notes": "string",
    }
)
``` 