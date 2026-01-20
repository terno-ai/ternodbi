import logging
import math
from decimal import Decimal
from typing import Dict, Any, List, Optional

from sqlalchemy import MetaData, Table, select, func, text, inspect, case
from sqlalchemy.sql.sqltypes import (
    Integer, Float, Numeric, BigInteger, SmallInteger, DECIMAL,
    String, Text, Enum, DateTime, Date, TIMESTAMP
)

logger = logging.getLogger(__name__)


def safe_float(val):
    """Convert Decimal to float safely."""
    if isinstance(val, Decimal):
        return float(val)
    return val


def get_column_stats(conn, table_inspector, table_name: str, column_name: str, cardinality_limit: int = 20) -> Dict[str, Any]:
    logger.debug(f"Analyzing column: {table_name}.{column_name}")
    stats = {}

    try:
        if column_name not in table_inspector.columns:
            logger.error(f"Column '{column_name}' not found in table '{table_name}'")
            return {}
        
        col = table_inspector.c[column_name]
        dialect_name = conn.dialect.name.lower()

        try:
            basic_query = select(
                func.count().label("row_count"),
                func.sum(case((col.is_(None), 1), else_=0)).label("null_count"),
                func.count(func.distinct(col)).label("cardinality")
            ).select_from(table_inspector)
            row_count, null_count, cardinality = conn.execute(basic_query).fetchone()
            stats.update({
                "row_count": row_count,
                "null_count": null_count,
                "cardinality": cardinality,
                "null_percentage": round((null_count / row_count) * 100, 2) if row_count else 0
            })
        except Exception as e:
            logger.warning(f"Basic stats failed for {table_name}.{column_name}: {e}")

        try:
            inspector = inspect(conn)
            indexed_columns = {c for idx in inspector.get_indexes(table_name) for c in idx["column_names"]}
            stats["is_indexed"] = column_name in indexed_columns
        except Exception as e:
            logger.warning(f"Index check failed for {table_name}: {e}")
            stats["is_indexed"] = False

        is_date_type = isinstance(col.type, (DateTime, Date, TIMESTAMP))
        
        if isinstance(col.type, (Integer, Float, Numeric, BigInteger, SmallInteger, DECIMAL)) and not is_date_type:
            try:
                basic_numeric_query = select(
                    func.avg(col).label("mean"),
                    func.min(col).label("min"),
                    func.max(col).label("max")
                ).where(col.isnot(None)).select_from(table_inspector)
                
                result = conn.execute(basic_numeric_query).fetchone()
                mean, min_val, max_val = result

                stats.update({
                    "mean": safe_float(mean),
                    "min": safe_float(min_val),
                    "max": safe_float(max_val),
                    "range": (safe_float(max_val) - safe_float(min_val)) if (min_val is not None and max_val is not None) else None
                })
            except Exception as e:
                logger.warning(f"Basic numeric stats failed for {table_name}.{column_name}: {e}")

            try:
                variance_query = select(
                    (func.avg(col * col) - func.avg(col) * func.avg(col)).label("variance")
                ).where(col.isnot(None)).select_from(table_inspector)
                
                variance = conn.execute(variance_query).scalar()
                variance = safe_float(variance)

                std_dev = math.sqrt(variance) if (variance is not None and variance >= 0) else None
                stats["std_dev"] = std_dev
            except Exception as e:
                logger.warning(f"Variance/StdDev stats failed for {table_name}.{column_name}: {e}")

        elif isinstance(col.type, (String, Text, Enum)):
            try:
                if stats.get("cardinality", 0) <= cardinality_limit:
                    unique_query = select(col, func.count().label("cnt")).group_by(col).order_by(func.count().desc()).limit(cardinality_limit)
                    rows = conn.execute(unique_query).fetchall()
                    stats["unique_values"] = [{"value": str(row[0]), "count": row[1]} for row in rows]
                else:
                    top_query = select(col, func.count().label("cnt")).group_by(col).order_by(func.count().desc()).limit(5)
                    rows = conn.execute(top_query).fetchall()
                    stats["top_values"] = [{"value": str(row[0]), "count": row[1]} for row in rows]
            except Exception as e:
                logger.warning(f"String frequency analysis failed for {table_name}.{column_name}: {e}")

            try:
                length_func = func.length
                length_query = select(
                    func.min(length_func(col)).label("min_length"),
                    func.max(length_func(col)).label("max_length")
                ).select_from(table_inspector)
                min_length, max_length = conn.execute(length_query).fetchone()
                stats["min_length"] = min_length
                stats["max_length"] = max_length
            except Exception as e:
                logger.warning(f"String length stats failed for {table_name}.{column_name}: {e}")

        elif isinstance(col.type, (DateTime, Date, TIMESTAMP)):
            try:
                dt_query = select(
                    func.min(col).label("min_date"),
                    func.max(col).label("max_date")
                ).where(col.isnot(None)).select_from(table_inspector)
                min_date, max_date = conn.execute(dt_query).fetchone()
                stats["min_date"] = str(min_date) if min_date else None
                stats["max_date"] = str(max_date) if max_date else None
                stats["date_range_days"] = (max_date - min_date).days if (min_date and max_date) else None
            except Exception as e:
                logger.warning(f"Date range stats failed for {table_name}.{column_name}: {e}")

    except Exception as top_level_err:
        logger.error(f"Top-level failure while processing {table_name}.{column_name}: {top_level_err}")
        try:
            conn.rollback()
        except Exception:
            pass
        return {}

    return stats


def get_sample_rows(conn, table_inspector, n: int = 10) -> List[List[Any]]:
    try:
        latest_column = None
        for col in table_inspector.columns:
            if col.primary_key:
                latest_column = col
                break
            if isinstance(col.type, (DateTime, Date, TIMESTAMP)):
                latest_column = col

        if latest_column is not None:
            query = select(table_inspector).order_by(latest_column.desc()).limit(n)
        else:
            query = select(table_inspector).limit(n)

        result = conn.execute(query).fetchall()
        return [list(row) for row in result]

    except Exception as e:
        logger.error(f"Error fetching sample rows for table {table_inspector.name}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return []


def get_table_info(datasource, table_name: str, sample_rows_count: int = 10) -> Dict[str, Any]:
    from terno_dbi.connectors import ConnectorFactory
    from terno_dbi.core import models
    connector = ConnectorFactory.create_connector(
        datasource.type,
        datasource.connection_str,
        credentials=datasource.connection_json
    )
    
    result = {
        "table_name": table_name,
        "datasource_name": datasource.display_name,
        "datasource_type": datasource.type,
        "columns": [],
        "sample_rows": [],
        "column_names": [],
        "relationships": []
    }
    
    try:
        with connector.get_connection() as conn:
            metadata = MetaData()
            table_inspector = Table(table_name, metadata, autoload_with=conn.engine)
            for col in table_inspector.columns:
                col_info = {
                    "name": col.name,
                    "type": str(col.type),
                    "nullable": col.nullable,
                    "primary_key": col.primary_key,
                    "stats": get_column_stats(conn, table_inspector, table_name, col.name)
                }
                result["columns"].append(col_info)
                result["column_names"].append(col.name)
            sample = get_sample_rows(conn, table_inspector, sample_rows_count)
            result["sample_rows"] = [[str(v) if v is not None else None for v in row] for row in sample]
            try:
                inspector = inspect(conn)
                fks = inspector.get_foreign_keys(table_name)
                result["relationships"] = [
                    {
                        "column": fk["constrained_columns"],
                        "references_table": fk["referred_table"],
                        "references_column": fk["referred_columns"]
                    }
                    for fk in fks
                ]
            except Exception as e:
                logger.warning(f"Could not get foreign keys for {table_name}: {e}")
  
        try:
            table_model = models.Table.objects.get(name=table_name, data_source=datasource)
            result["existing_description"] = table_model.description
            result["existing_public_name"] = table_model.public_name
            result["table_id"] = table_model.id

            for col_info in result["columns"]:
                try:
                    col_model = models.TableColumn.objects.get(table=table_model, name=col_info["name"])
                    col_info["existing_description"] = col_model.description
                    col_info["existing_public_name"] = col_model.public_name
                    col_info["column_id"] = col_model.id
                except models.TableColumn.DoesNotExist:
                    pass
        except models.Table.DoesNotExist:
            pass
            
    except Exception as e:
        logger.exception(f"Error getting table info for {table_name}: {e}")
        result["error"] = str(e)
    
    return result


def get_datasource_tables_info(datasource_id: int, table_names: Optional[List[str]] = None) -> Dict[str, Any]:
    from terno_dbi.core import models
    
    try:
        datasource = models.DataSource.objects.get(id=datasource_id, enabled=True)
    except models.DataSource.DoesNotExist:
        return {"error": f"Datasource {datasource_id} not found or not enabled"}

    if table_names:
        tables = models.Table.objects.filter(data_source=datasource, name__in=table_names)
    else:
        tables = models.Table.objects.filter(data_source=datasource)
    
    result = {
        "datasource_id": datasource_id,
        "datasource_name": datasource.display_name,
        "datasource_type": datasource.type,
        "tables_count": tables.count(),
        "tables": []
    }
    
    for table in tables:
        table_info = get_table_info(datasource, table.name)
        result["tables"].append(table_info)
    
    return result


# System schemas to exclude when syncing from INFORMATION_SCHEMA
SYSTEM_SCHEMAS = {'INFORMATION_SCHEMA', 'information_schema', 'pg_catalog', 'pg_toast'}


def _sync_from_information_schema(connector, datasource, result, overwrite=False):
    """
    Fallback sync using INFORMATION_SCHEMA when SQLAlchemy/SQLShield reflection fails.
    
    This is useful for databases where only views exist (no tables), or when
    views are stored in a way that SQLAlchemy's reflection doesn't pick them up.
    
    Works for Snowflake, BigQuery, PostgreSQL, MySQL, etc.
    
    Args:
        connector: Database connector instance
        datasource: DataSource model instance
        result: Result dict to update (mutated in place)
        overwrite: If True, update existing table/column metadata
        
    Returns:
        Number of tables/views discovered
    """
    from terno_dbi.core import models
    from sqlalchemy import text
    
    tables_discovered = 0
    
    # Extract schema from connection string if present
    # Format: snowflake://user@account/database/schema
    target_schema = None
    conn_str = datasource.connection_str
    
    # Try to extract schema from Snowflake connection string
    if 'snowflake://' in conn_str:
        parts = conn_str.split('/')
        if len(parts) >= 5:  # Has schema
            target_schema = parts[-1].split('?')[0]  # Remove query params
    
    logger.info(f"Syncing from INFORMATION_SCHEMA (target_schema={target_schema})")
    
    try:
        with connector.get_connection() as conn:
            # Build query to get all columns from INFORMATION_SCHEMA
            # Filter by target schema if specified, otherwise get all non-system schemas
            if target_schema and target_schema.upper() not in SYSTEM_SCHEMAS:
                query = text("""
                    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE UPPER(TABLE_SCHEMA) = UPPER(:schema)
                    ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
                """)
                rows = conn.execute(query, {"schema": target_schema}).fetchall()
            else:
                # Get all non-system schemas
                query = text("""
                    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE UPPER(TABLE_SCHEMA) NOT IN ('INFORMATION_SCHEMA', 'PG_CATALOG', 'PG_TOAST')
                    ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
                """)
                rows = conn.execute(query).fetchall()
            
            logger.info(f"INFORMATION_SCHEMA returned {len(rows)} column rows")
            
            # Group columns by table
            tables_dict = {}  # {(schema, table_name): [(col_name, data_type), ...]}
            for row in rows:
                schema_name, table_name, column_name, data_type, _ = row
                key = (schema_name, table_name)
                if key not in tables_dict:
                    tables_dict[key] = []
                tables_dict[key].append((column_name, data_type))
            
            logger.info(f"Found {len(tables_dict)} tables/views in INFORMATION_SCHEMA")
            
            # Create/update tables and columns
            for (schema_name, table_name), columns in tables_dict.items():
                try:
                    # Use schema.table format for multi-schema databases
                    # or just table name if schema matches target
                    if target_schema and schema_name.upper() == target_schema.upper():
                        full_table_name = table_name
                    else:
                        full_table_name = f"{schema_name}.{table_name}"
                    
                    # Check if table already exists
                    existing_table = models.Table.objects.filter(
                        data_source=datasource,
                        name=full_table_name
                    ).first()
                    
                    if existing_table and not overwrite:
                        result["tables_skipped"] += 1
                        result["tables"].append({
                            "name": full_table_name,
                            "status": "skipped",
                            "id": existing_table.id
                        })
                        continue
                    
                    # Create or update table
                    if existing_table:
                        table_model = existing_table
                        result["tables_updated"] += 1
                    else:
                        table_model = models.Table.objects.create(
                            data_source=datasource,
                            name=full_table_name,
                            public_name=full_table_name,
                        )
                        result["tables_created"] += 1
                        tables_discovered += 1
                    
                    # Create/update columns
                    columns_count = 0
                    for col_name, data_type in columns:
                        existing_col = models.TableColumn.objects.filter(
                            table=table_model,
                            name=col_name
                        ).first()
                        
                        if existing_col:
                            if overwrite:
                                existing_col.data_type = str(data_type) if data_type else 'UNKNOWN'
                                existing_col.save()
                        else:
                            models.TableColumn.objects.create(
                                table=table_model,
                                name=col_name,
                                public_name=col_name,
                                data_type=str(data_type) if data_type else 'UNKNOWN',
                            )
                            result["columns_created"] += 1
                        columns_count += 1
                    
                    result["tables"].append({
                        "name": full_table_name,
                        "status": "created" if not existing_table else "updated",
                        "id": table_model.id,
                        "columns": columns_count,
                        "source": "information_schema"
                    })
                    
                except Exception as e:
                    logger.exception(f"Error syncing table {schema_name}.{table_name}: {e}")
                    result["tables"].append({
                        "name": f"{schema_name}.{table_name}",
                        "status": "error",
                        "error": str(e)
                    })
            
    except Exception as e:
        logger.exception(f"Error querying INFORMATION_SCHEMA: {e}")
    
    return tables_discovered


def sync_metadata(datasource_id: int, overwrite: bool = False) -> Dict[str, Any]:
    """
    Sync metadata from database to Django models.
    Uses ConnectorFactory.get_metadata() to discover tables, columns, and foreign keys.
    
    This mirrors TernoAI's load_metadata task functionality.
    
    Args:
        datasource_id: ID of the datasource to sync
        overwrite: If True, update existing table/column metadata
        
    Returns:
        Dictionary with sync results
    """
    from terno_dbi.core import models
    from terno_dbi.connectors import ConnectorFactory
    
    try:
        datasource = models.DataSource.objects.get(id=datasource_id, enabled=True)
    except models.DataSource.DoesNotExist:
        return {"error": f"Datasource {datasource_id} not found or not enabled"}
    
    # Get connector
    connector = ConnectorFactory.create_connector(
        datasource.type,
        datasource.connection_str,
        credentials=datasource.connection_json
    )
    
    result = {
        "datasource_id": datasource_id,
        "datasource_name": datasource.display_name,
        "tables_created": 0,
        "tables_updated": 0,
        "tables_skipped": 0,
        "columns_created": 0,
        "foreign_keys_created": 0,
        "tables": []
    }
    
    try:
        # Update dialect info if not already set
        if not datasource.dialect_name or not datasource.dialect_version:
            try:
                dialect_name, dialect_version = connector.get_dialect_info()
                datasource.dialect_name = dialect_name
                datasource.dialect_version = dialect_version
                datasource.save(update_fields=['dialect_name', 'dialect_version'])
            except Exception as e:
                logger.warning(f"Could not get dialect info: {e}")
        
        # Get metadata using the connector (returns MDatabase with tables, columns, FKs)
        mdb = connector.get_metadata()
        
        logger.info(f"Found {len(mdb.tables)} tables in database via SQLShield")
        
        # Fallback: If SQLShield found 0 tables, try INFORMATION_SCHEMA
        # This handles databases with only views (no tables)
        if len(mdb.tables) == 0:
            logger.info("SQLShield found 0 tables, falling back to INFORMATION_SCHEMA")
            tables_discovered = _sync_from_information_schema(
                connector, datasource, result, overwrite
            )
            result["tables_synced"] = result["tables_created"] + result["tables_updated"]
            result["sync_method"] = "information_schema"
            logger.info(f"INFORMATION_SCHEMA fallback discovered {tables_discovered} tables/views")
            return result
        
        result["sync_method"] = "sqlshield"
        
        # Phase 1: Create/update tables and columns
        for tbl_name, tbl in mdb.tables.items():
            try:
                # Use tbl.name (actual case from DB) not tbl_name (lowercase dict key)
                actual_table_name = tbl.name
                
                # Check if table already exists
                existing_table = models.Table.objects.filter(
                    data_source=datasource, 
                    name=actual_table_name
                ).first()
                
                if existing_table and not overwrite:
                    result["tables_skipped"] += 1
                    result["tables"].append({
                        "name": actual_table_name, 
                        "status": "skipped",
                        "id": existing_table.id
                    })
                    continue
                
                # Create or update table
                if existing_table:
                    table_model = existing_table
                    result["tables_updated"] += 1
                else:
                    table_model = models.Table.objects.create(
                        data_source=datasource,
                        name=actual_table_name,
                        public_name=actual_table_name,
                    )
                    result["tables_created"] += 1
                
                # Create/update columns from MDatabase table
                columns_count = 0
                for col_name, col in tbl.columns.items():
                    existing_col = models.TableColumn.objects.filter(
                        table=table_model,
                        name=col_name
                    ).first()
                    
                    if existing_col:
                        if overwrite:
                            existing_col.data_type = str(col.type)
                            existing_col.save()
                    else:
                        models.TableColumn.objects.create(
                            table=table_model,
                            name=col_name,
                            public_name=col_name,
                            data_type=str(col.type),
                        )
                        result["columns_created"] += 1
                    columns_count += 1
                
                result["tables"].append({
                    "name": actual_table_name,
                    "status": "created" if not existing_table else "updated",
                    "id": table_model.id,
                    "columns": columns_count
                })
                
            except Exception as e:
                logger.exception(f"Error syncing table {tbl_name}: {e}")
                result["tables"].append({
                    "name": tbl_name,
                    "status": "error",
                    "error": str(e)
                })
        
        for tbl_name, tbl in mdb.tables.items():
            try:
                foreign_keys = tbl.Foreign_Keys
                if not foreign_keys:
                    continue
                    
                table = models.Table.objects.filter(
                    name=tbl.name, data_source=datasource
                ).first()
                
                if not table:
                    continue
                
                for fk in foreign_keys:
                    try:
                        constrained_columns = models.TableColumn.objects.filter(
                            name=fk.constrained_columns[0].name,
                            table__data_source=datasource
                        ).first()
                        
                        referred_table = models.Table.objects.filter(
                            name=fk.referred_table.name, 
                            data_source=datasource
                        ).first()
                        
                        referred_columns = models.TableColumn.objects.filter(
                            name=fk.referred_columns[0].name,
                            table__data_source=datasource
                        ).first()
                        
                        if not all([constrained_columns, referred_table, referred_columns]):
                            continue
                        
                        # Check if FK already exists
                        existing_fk = models.ForeignKey.objects.filter(
                            constrained_table=table,
                            constrained_columns=constrained_columns,
                            referred_table=referred_table,
                            referred_columns=referred_columns
                        ).first()
                        
                        if not existing_fk:
                            models.ForeignKey.objects.create(
                                constrained_table=table,
                                constrained_columns=constrained_columns,
                                referred_table=referred_table,
                                referred_columns=referred_columns
                            )
                            result["foreign_keys_created"] += 1
                    except Exception as fk_error:
                        logger.warning(f"Error creating FK for {tbl_name}: {fk_error}")
                        
            except Exception as e:
                logger.warning(f"Error processing FKs for table {tbl_name}: {e}")
        
        result["tables_synced"] = result["tables_created"] + result["tables_updated"]
        return result
        
    except Exception as e:
        logger.exception(f"Error syncing metadata for datasource {datasource_id}: {e}")
        return {"error": str(e)}

