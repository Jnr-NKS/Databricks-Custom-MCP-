from typing import Optional
import asyncio
from mcp.server.fastmcp import FastMCP  # type: ignore[import-not-found]
from databricks_formatter import format_query_results
from databricks_sdk_utils import (
    execute_databricks_sql,
    get_uc_all_catalogs_summary,
    get_uc_catalog_details,
    get_uc_schema_details,
    get_uc_table_details,
    clear_lineage_cache
)
import google.generativeai as genai
import os
from dotenv import load_dotenv
import sys
import logging
import re
import json
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Load environment variables
load_dotenv()

# Configure Gemini API
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

mcp = FastMCP("databricks")

def extract_keywords(query: str) -> list:
    """
    Extract simple keywords from a natural language query.
    Removes stopwords and punctuation.
    Example: 
      "Give me the customer email of customer with order ID 105"
      -> ["customer", "email", "order", "id", "105"]
    """
    stopwords = {"the", "a", "an", "of", "with", "all", "get", "give", "show", "list", "for", "me", "whose"}
    
    # Lowercase + split words
    tokens = re.findall(r"[A-Za-z0-9_]+", query.lower())
    
    # Filter out stopwords
    keywords = [t for t in tokens if t not in stopwords]
    return keywords

def generate_sql_from_natural_language(
    query: str,
    catalog_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    schema_context: Optional[str] = None
) -> str:
    """
    Uses Gemini AI to convert natural language to Databricks SQL.
    """
    if not GEMINI_API_KEY:
        return "Error: GEMINI_API_KEY not set. Cannot generate SQL from natural language."

    try:
        # Build STRONGER context for Gemini with stricter rules
        context = (
            "You are a SQL assistant that converts natural language into Databricks SQL queries.\n"
            "CRITICAL RULES - DO NOT BREAK THESE:\n"
            "1. You MUST return ONLY a single valid SQL query, nothing else\n"
            "2. NEVER add explanations, comments, or alternative queries\n"
            "3. If schema context is provided, use ONLY those tables/columns\n"
            "4. If schema context is not provided, scan entire hive metastore for the best possible schema based on column names & table names \n"
            "5. If you cannot generate a query, return: 'SELECT 1 AS error_no_valid_query'\n"
            "6. ALWAYS wrap the SQL inside <SQL>...</SQL> tags\n"
            "7. NEVER add any text before or after the <SQL> tags\n"
            "8. If tables don't match the query, still generate the best possible SQL\n"
            "9. NEVER say 'Unable to provide a query' - always generate SQL\n"
            "\n"
            "FAILURE TO FOLLOW THESE RULES WILL CAUSE SYSTEM ERRORS\n"
        )

        if catalog_name:
            context += f"\nDefault catalog: {catalog_name}."
        if schema_name:
            context += f"\nDefault schema: {schema_name}."
        if schema_context:
            context += f"\n\nAVAILABLE SCHEMA INFORMATION (USE THESE TABLES ONLY):\n{schema_context}\n"
        else:
            context += "\nWARNING: No specific schema context provided."

        # Construct prompt with explicit instruction
        prompt = f"{context}\n\nNatural language query to convert to SQL:\n{query}\n\nRemember: ONLY return SQL inside <SQL> tags, no explanations!"

        model = genai.GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)

        sql_text = response.text or ""

        # Debug: log what Gemini returned
        print(f"Gemini raw response: {repr(sql_text)}", file=sys.stderr)

        # Extract only the SQL inside <SQL>...</SQL>
        match = re.search(r"<SQL>(.*?)</SQL>", sql_text, re.S | re.I)
        if match:
            sql_content = match.group(1).strip()
            # Clean up any remaining non-SQL content
            sql_content = re.sub(r'^[^A-Za-z0-9\s\(\)\*]*', '', sql_content)
            
            # Validate it's actually SQL
            valid_starters = ['SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP', 'SHOW', 'DESCRIBE', 'EXPLAIN']
            if not any(sql_content.upper().startswith(keyword) for keyword in valid_starters):
                return "SELECT 1 AS error_invalid_sql_generated"
                
            return sql_content
        else:
            # If no SQL tags found, check if it's pure SQL
            if any(sql_text.upper().startswith(keyword) for keyword in valid_starters):
                return sql_text.strip()
            
            # If Gemini returned explanatory text, return a safe fallback
            return "SELECT 1 AS error_no_sql_generated"

    except Exception as e:
        return f"SELECT 1 AS error_generation_failed"
# ----------------
# Catalog / Schema / Table listing
# ----------------

@mcp.tool()
async def list_all_catalogs() -> str:
    try:
        result = await asyncio.to_thread(get_uc_all_catalogs_summary)
        return result
    except Exception as e:
        return f"Error listing catalogs: {str(e)}"

@mcp.tool()
async def list_schemas_in_catalog(catalog_name: str) -> str:
    try:
        result = await asyncio.to_thread(get_uc_catalog_details, catalog_name)
        return result
    except Exception as e:
        return f"Error listing schemas in catalog '{catalog_name}': {str(e)}"

@mcp.tool()
async def list_tables_in_schema(catalog_name: str, schema_name: str, include_columns: bool = True) -> str:
    try:
        result = await asyncio.to_thread(get_uc_schema_details, catalog_name, schema_name, include_columns)
        return result
    except Exception as e:
        return f"Error listing tables in schema '{catalog_name}.{schema_name}': {str(e)}"

@mcp.tool()
async def get_table_details(full_table_name: str, include_lineage: bool = False) -> str:
    try:
        result = await asyncio.to_thread(get_uc_table_details, full_table_name, include_lineage)
        return result
    except Exception as e:
        return f"Error getting table details for '{full_table_name}': {str(e)}"

# ----------------
# Search functions (now include Hive Metastore + UC)
# ----------------

@mcp.tool()
async def search_tables_by_name(table_name_pattern: str, catalog_name: Optional[str] = None) -> str:
    try:
        search_sql = f"""
        SELECT table_catalog, table_schema, table_name, table_type, comment
        FROM system.information_schema.tables 
        WHERE LOWER(table_name) LIKE LOWER('%{table_name_pattern}%')
        ORDER BY table_catalog, table_schema, table_name
        LIMIT 100
        """
        if catalog_name:
            search_sql = search_sql.replace("WHERE", f"WHERE table_catalog = '{catalog_name}' AND")

        sdk_result = await asyncio.to_thread(execute_databricks_sql, search_sql)
        return format_query_results(sdk_result)
    except Exception as e:
        return f"Error searching for tables with pattern '{table_name_pattern}': {str(e)}"

@mcp.tool()
async def search_columns_by_name(column_name_pattern: str, catalog_name: Optional[str] = None,
                                 schema_name: Optional[str] = None) -> str:
    try:
        search_sql = f"""
        SELECT table_catalog, table_schema, table_name, column_name, data_type, is_nullable
        FROM system.information_schema.columns 
        WHERE LOWER(column_name) LIKE LOWER('%{column_name_pattern}%')
        """
        if catalog_name:
            search_sql += f" AND table_catalog = '{catalog_name}'"
        if schema_name:
            search_sql += f" AND table_schema = '{schema_name}'"

        search_sql += " ORDER BY table_catalog, table_schema, table_name, ordinal_position LIMIT 100"

        sdk_result = await asyncio.to_thread(execute_databricks_sql, search_sql)
        return format_query_results(sdk_result)
    except Exception as e:
        return f"Error searching for columns with pattern '{column_name_pattern}': {str(e)}"

@mcp.tool()
async def search_tables_by_description(description_pattern: str, catalog_name: Optional[str] = None) -> str:
    try:
        search_sql = f"""
        SELECT table_catalog, table_schema, table_name, table_type, comment
        FROM system.information_schema.tables 
        WHERE LOWER(comment) LIKE LOWER('%{description_pattern}%')
        ORDER BY table_catalog, table_schema, table_name
        LIMIT 100
        """
        if catalog_name:
            search_sql = search_sql.replace("WHERE", f"WHERE table_catalog = '{catalog_name}' AND")

        sdk_result = await asyncio.to_thread(execute_databricks_sql, search_sql)
        return format_query_results(sdk_result)
    except Exception as e:
        return f"Error searching for tables with description pattern '{description_pattern}': {str(e)}"

# ----------------
# Natural language query with dynamic discovery
# ----------------

@mcp.tool()
async def smart_natural_language_query(query: str, catalog_name: Optional[str] = None, schema_name: Optional[str] = None) -> str:
    """
    Dynamically discovers the correct tables/columns and generates SQL
    from natural language. Always enforces fully-qualified table names.
    """

    try:
        # 1. Extract keywords
        keywords = extract_keywords(query)
        print(f"Keywords extracted: {keywords}", file=sys.stderr)

        # 2. Search for matching tables in Hive Metastore & Unity Catalog
        discovered_tables = set()
        for kw in keywords:
            # Search in Hive Metastore
            tables_hive = await search_tables_by_name(kw, catalog_name="hive_metastore")
            if tables_hive:
                discovered_tables.update(tables_hive)

            # Search in Unity Catalog (if provided)
            if catalog_name:
                tables_uc = await search_tables_by_name(kw, catalog_name=catalog_name)
                if tables_uc:
                    discovered_tables.update(tables_uc)

        if not discovered_tables:
            return f"No tables found matching keywords: {keywords}"

        # 3. Rank tables by keyword overlap
        ranked_tables = []
        for tbl in discovered_tables:
            score = sum(kw in tbl.lower() for kw in keywords)
            ranked_tables.append((score, tbl))
        ranked_tables.sort(reverse=True)

        # Pick top 2 candidates
        top_tables = [t for _, t in ranked_tables[:2]]

        # 4. Build schema context with full table names + columns
        schema_context = "The following tables are available. You MUST use only these, exactly as written:\n\n"
        for table in top_tables:
            try:
                details = await get_table_details(table, include_lineage=False)
                schema_context += f"Table: {table}\n"
                schema_context += f"{details}\n---\n"
            except Exception as e:
                print(f"Error getting details for {table}: {e}", file=sys.stderr)

        # 5. Generate SQL from NL query
        sql_query = generate_sql_from_natural_language(
            query=query,
            catalog_name=catalog_name,
            schema_name=schema_name,
            schema_context=schema_context
        )

        # 6. Execute SQL
        result = execute_databricks_sql(sql_query)
        return f"**Generated SQL:**\n```sql\n{sql_query}\n```\n\n**Results:**\n{result}"

    except Exception as e:
        return f"Error in smart_natural_language_query: {str(e)}"


# ----------------
# Utilities
# ----------------

@mcp.tool()
async def clear_cache() -> str:
    try:
        await asyncio.to_thread(clear_lineage_cache)
        return "Cache cleared successfully."
    except Exception as e:
        return f"Error clearing cache: {str(e)}"

@mcp.tool()
async def execute_sql_query(sql: str) -> str:
    try:
        sdk_result = await asyncio.to_thread(execute_databricks_sql, sql_query=sql)
        status = sdk_result.get("status")
        if status == "failed":
            return f"SQL Query Failed: {sdk_result.get('error','Unknown error')}\nDetails: {sdk_result.get('details','N/A')}"
        elif status == "error":
            return f"Error during SQL Execution: {sdk_result.get('error','Unknown error')}\nDetails: {sdk_result.get('details','N/A')}"
        elif status == "success":
            return format_query_results(sdk_result)
        else:
            return f"Unexpected status: {status}. Result: {sdk_result}"
    except Exception as e:
        return f"Error executing SQL query: {str(e)}"

@mcp.tool()
async def natural_language_query(query: str,
                                 catalog_name: Optional[str] = None,
                                 schema_name: Optional[str] = None) -> str:
    generated_sql = generate_sql_from_natural_language(query, catalog_name, schema_name)
    if generated_sql.startswith("Error:"):
        return generated_sql
    try:
        result = await execute_sql_query(generated_sql)
        return f"Generated SQL: ```sql\n{generated_sql}\n```\n\nResults:\n{result}"
    except Exception as e:
        return f"Error executing generated SQL '{generated_sql}': {str(e)}"

# ----------------
# JSON-RPC Handling
# ----------------

def handle_json_rpc_request(request_data: str) -> str:
    try:
        request = json.loads(request_data)
        method = request.get("method")
        params = request.get("params", {})
        request_id = request.get("id", 1)

        method_map = {
            "list_all_catalogs": list_all_catalogs,
            "list_schemas_in_catalog": list_schemas_in_catalog,
            "list_tables_in_schema": list_tables_in_schema,
            "get_table_details": get_table_details,
            "search_tables_by_name": search_tables_by_name,
            "search_columns_by_name": search_columns_by_name,
            "search_tables_by_description": search_tables_by_description,
            "smart_natural_language_query": smart_natural_language_query,
            "execute_sql_query": execute_sql_query,
            "natural_language_query": natural_language_query,
            "clear_cache": clear_cache
        }

        if method in method_map:
            result = asyncio.run(method_map[method](**params))
            response = {"jsonrpc": "2.0", "id": request_id, "result": result}
        else:
            response = {"jsonrpc": "2.0","id": request_id,
                        "error":{"code":-32601,"message":f"Method not found: {method}"}}
        return json.dumps(response) + "\n"
    except Exception as e:
        return json.dumps({"jsonrpc":"2.0","id":request.get("id",1),
                           "error":{"code":-32603,"message":f"Internal error: {str(e)}"}}) + "\n"

if __name__ == "__main__":
    print("Starting Databricks MCP server in stdio mode...", file=sys.stderr)
    logger.info("MCP Server starting up")
    while True:
        try:
            line = sys.stdin.readline()
            if not line:
                break
            response = handle_json_rpc_request(line)
            sys.stdout.write(response)
            sys.stdout.flush()
        except KeyboardInterrupt:
            break
        except Exception as e:
            error_response = json.dumps({
                "jsonrpc":"2.0","id":None,
                "error":{"code":-32603,"message":f"Server error: {str(e)}"}}) + "\n"
            sys.stdout.write(error_response)
            sys.stdout.flush()
