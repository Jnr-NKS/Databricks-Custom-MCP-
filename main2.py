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

def generate_sql_from_natural_language(
    query: str,
    catalog_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    schema_context: Optional[str] = None
) -> str:
    """
    Uses Gemini AI to convert natural language to Databricks SQL.
    Enforces SQL-only output using <SQL>...</SQL> tags.
    Prefers actual discovered schema context over generic guesses.
    """
    if not GEMINI_API_KEY:
        return "Error: GEMINI_API_KEY not set. Cannot generate SQL from natural language."

    try:
        # Build strong context for Gemini
        context = (
            "You are a SQL assistant that converts natural language into Databricks SQL queries.\n"
            "Rules:\n"
            "1. Return ONLY a single valid SQL query.\n"
            "2. Do NOT add explanations, comments, or alternative queries.\n"
            "3. If schema context is provided, use only those tables/columns.\n"
            "4. Always wrap the SQL inside <SQL>...</SQL> tags.\n"
        )

        if catalog_name:
            context += f"\nDefault catalog: {catalog_name}."
        if schema_name:
            context += f"\nDefault schema: {schema_name}."
        if schema_context:
            context += f"\n\nAvailable schema information:\n{schema_context}\n"

        # Construct prompt
        prompt = f"{context}\nNatural language query:\n{query}"

        model = genai.GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)

        sql_text = response.text or ""

        # Extract only the SQL inside <SQL>...</SQL>
        import re
        match = re.search(r"<SQL>(.*?)</SQL>", sql_text, re.S | re.I)
        if match:
            return match.group(1).strip()
        else:
            # fallback: return first SQL-like block
            return sql_text.strip().split(";")[0] + ";"

    except Exception as e:
        return f"Error generating SQL: {str(e)}"


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
async def smart_natural_language_query(query: str,
                                       catalog_name: Optional[str] = None,
                                       schema_name: Optional[str] = None,
                                       auto_discover_schema: bool = True) -> str:
    try:
        schema_context = ""
        
        if auto_discover_schema:
            query_lower = query.lower()
            stop_words = {'give','me','all','the','a','an','and','or','but','in','on',
                          'at','to','for','of','with','by','is','are','was','were','whose',
                          'what','which','when','where','why','how','many','much','few',
                          'show','display','list','find','search','get','return'}
            words = query_lower.split()
            keywords = [w for w in words if w not in stop_words and len(w) > 2]
            numbers = re.findall(r'\b\d+\b', query)
            keywords.extend(numbers)

            column_variations = {
                'name': ['name','names','firstname','lastname','fullname'],
                'id': ['id','identifier','code','number'],
                'customer': ['customer','client','user','account'],
                'order': ['order','purchase','transaction'],
                'product': ['product','item','sku','goods']
            }
            expanded_keywords = []
            for kw in keywords:
                expanded_keywords.append(kw)
                for base, variations in column_variations.items():
                    if kw in variations:
                        expanded_keywords.extend(variations)
                        break
            keywords = list(set(expanded_keywords))
            print(f"Extracted keywords: {keywords}", file=sys.stderr)

            discovered_tables = set()
            table_details_map = {}

            for kw in keywords[:5]:
                # Search name
                search_result = await search_tables_by_name(kw)
                if "Error" not in search_result and search_result.strip():
                    for line in search_result.splitlines()[2:]:
                        if '|' in line and not line.startswith('-'):
                            parts = [p.strip() for p in line.split('|')]
                            if len(parts) >= 3:
                                full_table = f"{parts[0]}.{parts[1]}.{parts[2]}"
                                discovered_tables.add(full_table)

                # Search description
                desc_result = await search_tables_by_description(kw)
                if "Error" not in desc_result and desc_result.strip():
                    for line in desc_result.splitlines()[2:]:
                        if '|' in line and not line.startswith('-'):
                            parts = [p.strip() for p in line.split('|')]
                            if len(parts) >= 3:
                                full_table = f"{parts[0]}.{parts[1]}.{parts[2]}"
                                discovered_tables.add(full_table)

                # Search columns
                try:
                    col_result = await search_columns_by_name(kw)
                    if "Error" not in col_result and col_result.strip():
                        for line in col_result.splitlines()[2:]:
                            if '|' in line and not line.startswith('-'):
                                parts = [p.strip() for p in line.split('|')]
                                if len(parts) >= 3:
                                    full_table = f"{parts[0]}.{parts[1]}.{parts[2]}"
                                    discovered_tables.add(full_table)
                except Exception as e:
                    print(f"Error searching columns for '{kw}': {e}", file=sys.stderr)

           
            # Rank discovered tables: prioritize ones with multiple keyword matches
            ranked_tables = []
            for table in discovered_tables:
                score = sum(kw in table.lower() for kw in keywords)
                ranked_tables.append((score, table))
            ranked_tables.sort(reverse=True)

            # Take top 2 most relevant tables only
            top_tables = [t for _, t in ranked_tables[:2]]

            schema_context = "The following tables are available. You MUST use only these:\n"
            for table in top_tables:
                try:
                    table_details = await get_table_details(table, include_lineage=False)
                    schema_context += f"\n{table_details}\n---\n"
                except Exception as e:
                    print(f"Error getting details for table '{table}': {e}", file=sys.stderr)

        generated_sql = generate_sql_from_natural_language(query, catalog_name, schema_name, schema_context)
        if generated_sql.startswith("Error:"):
            return generated_sql

        result = await execute_sql_query(generated_sql)
        response = f"**Generated SQL:**\n```sql\n{generated_sql}\n```\n\n"
        if schema_context:
            response += f"**Discovered Schema Context:**\n{schema_context}\n\n"
        response += f"**Results:**\n{result}"
        return response
    except Exception as e:
        return f"Error in smart natural language query: {str(e)}"

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
