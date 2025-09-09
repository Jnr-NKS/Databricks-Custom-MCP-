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

def generate_sql_from_natural_language(query: str, catalog_name: Optional[str] = None, schema_name: Optional[str] = None, schema_context: Optional[str] = None) -> str:
    """
    Uses Gemini AI to convert natural language to SQL.
    """
    if not GEMINI_API_KEY:
        return "Error: GEMINI_API_KEY not set. Cannot generate SQL from natural language."
    
    try:
        # Create context for the AI
        context = "You are a SQL expert that converts natural language questions to Databricks SQL."
        if catalog_name:
            context += f" The default catalog is {catalog_name}."
        if schema_name:
            context += f" The default schema is {schema_name}."
        
        # Add schema context if provided
        if schema_context:
            context += f"\n\nAvailable schema information:\n{schema_context}"
            context += "\n\nUse the table and column names from the schema information above when generating SQL."
        
        # Use Gemini to generate SQL
        model = genai.GenerativeModel('gemini-pro')
        prompt = f"{context}\n\nConvert this natural language query to Databricks SQL:\n\n{query}"
        
        response = model.generate_content(prompt)
        return response.text.strip().replace("```sql", "").replace("```", "").strip()
    except Exception as e:
        return f"Error generating SQL: {str(e)}"

@mcp.tool()
async def list_all_catalogs() -> str:
    """
    Lists all available Unity Catalog catalogs with their descriptions and types.
    
    Use this tool to discover what catalogs are available in your Databricks environment.
    This is typically the first step in exploring your data structure.
    """
    try:
        result = await asyncio.to_thread(get_uc_all_catalogs_summary)
        return result
    except Exception as e:
        return f"Error listing catalogs: {str(e)}"

@mcp.tool()
async def list_schemas_in_catalog(catalog_name: str) -> str:
    """
    Lists all schemas within a specific catalog with their descriptions.
    
    Use this tool to explore the schemas available within a particular catalog.
    
    Args:
        catalog_name: Name of the catalog to explore
    """
    try:
        result = await asyncio.to_thread(get_uc_catalog_details, catalog_name)
        return result
    except Exception as e:
        return f"Error listing schemas in catalog '{catalog_name}': {str(e)}"

@mcp.tool()
async def list_tables_in_schema(catalog_name: str, schema_name: str, include_columns: bool = True) -> str:
    """
    Lists all tables within a specific schema, optionally including column details.
    
    Use this tool to explore tables within a schema and understand their structure.
    Set include_columns=True to see column names, types, and descriptions.
    
    Args:
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        include_columns: Whether to include detailed column information (default: True)
    """
    try:
        result = await asyncio.to_thread(get_uc_schema_details, catalog_name, schema_name, include_columns)
        return result
    except Exception as e:
        return f"Error listing tables in schema '{catalog_name}.{schema_name}': {str(e)}"

@mcp.tool()
async def get_table_details(full_table_name: str, include_lineage: bool = False) -> str:
    """
    Gets detailed information about a specific table including columns, types, and optionally lineage.
    
    Use this tool to understand the structure of a specific table, its columns, data types,
    partition information, and relationships with other tables/notebooks.
    
    Args:
        full_table_name: Full table name in format 'catalog.schema.table'
        include_lineage: Whether to include lineage information (upstream/downstream tables and notebooks)
    """
    try:
        result = await asyncio.to_thread(get_uc_table_details, full_table_name, include_lineage)
        return result
    except Exception as e:
        return f"Error getting table details for '{full_table_name}': {str(e)}"

@mcp.tool()
async def search_tables_by_name(table_name_pattern: str, catalog_name: Optional[str] = None) -> str:
    """
    Searches for tables by name pattern across catalogs/schemas.
    
    Use this tool to find tables when you know part of the table name but not the full path.
    
    Args:
        table_name_pattern: Pattern to search for (case-insensitive, supports wildcards with %)
        catalog_name: Optional catalog to limit search to
    """
    try:
        # Build search query
        if catalog_name:
            search_sql = f"""
            SELECT table_catalog, table_schema, table_name, table_type, comment
            FROM system.information_schema.tables 
            WHERE table_catalog = '{catalog_name}' 
            AND LOWER(table_name) LIKE LOWER('%{table_name_pattern}%')
            ORDER BY table_catalog, table_schema, table_name
            LIMIT 50
            """
        else:
            search_sql = f"""
            SELECT table_catalog, table_schema, table_name, table_type, comment
            FROM system.information_schema.tables 
            WHERE LOWER(table_name) LIKE LOWER('%{table_name_pattern}%')
            ORDER BY table_catalog, table_schema, table_name
            LIMIT 50
            """
        
        sdk_result = await asyncio.to_thread(execute_databricks_sql, search_sql)
        return format_query_results(sdk_result)
    except Exception as e:
        return f"Error searching for tables with pattern '{table_name_pattern}': {str(e)}"

@mcp.tool()
async def search_columns_by_name(column_name_pattern: str, catalog_name: Optional[str] = None, schema_name: Optional[str] = None) -> str:
    """
    Searches for columns by name pattern across tables.
    
    Use this tool to find columns when you know part of the column name.
    
    Args:
        column_name_pattern: Pattern to search for (case-insensitive, supports wildcards with %)
        catalog_name: Optional catalog to limit search to
        schema_name: Optional schema to limit search to
    """
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
        
        search_sql += " ORDER BY table_catalog, table_schema, table_name, ordinal_position LIMIT 50"
        
        sdk_result = await asyncio.to_thread(execute_databricks_sql, search_sql)
        return format_query_results(sdk_result)
    except Exception as e:
        return f"Error searching for columns with pattern '{column_name_pattern}': {str(e)}"

@mcp.tool()
async def search_tables_by_description(description_pattern: str, catalog_name: Optional[str] = None) -> str:
    """
    Searches for tables by description/comment pattern across catalogs/schemas.
    
    Use this tool to find tables when you know what they contain but not their exact names.
    
    Args:
        description_pattern: Pattern to search for in table descriptions (case-insensitive)
        catalog_name: Optional catalog to limit search to
    """
    try:
        if catalog_name:
            search_sql = f"""
            SELECT table_catalog, table_schema, table_name, table_type, comment
            FROM system.information_schema.tables 
            WHERE table_catalog = '{catalog_name}' 
            AND LOWER(comment) LIKE LOWER('%{description_pattern}%')
            ORDER BY table_catalog, table_schema, table_name
            LIMIT 50
            """
        else:
            search_sql = f"""
            SELECT table_catalog, table_schema, table_name, table_type, comment
            FROM system.information_schema.tables 
            WHERE LOWER(comment) LIKE LOWER('%{description_pattern}%')
            ORDER BY table_catalog, table_schema, table_name
            LIMIT 50
            """
        
        sdk_result = await asyncio.to_thread(execute_databricks_sql, search_sql)
        return format_query_results(sdk_result)
    except Exception as e:
        return f"Error searching for tables with description pattern '{description_pattern}': {str(e)}"

@mcp.tool()
async def smart_natural_language_query(
    query: str,
    catalog_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    auto_discover_schema: bool = True
) -> str:
    """
    Enhanced natural language query that dynamically discovers relevant schema information.
    
    This tool extracts keywords, searches for relevant tables and columns, then generates
    and executes appropriate SQL.
    
    Args:
        query: Natural language question about your data
        catalog_name: Optional catalog name for context
        schema_name: Optional schema name for context  
        auto_discover_schema: Whether to automatically discover relevant schema info (default: True)
    """
    try:
        schema_context = ""
        
        if auto_discover_schema:
            # Extract keywords from the query
            query_lower = query.lower()
            
            # Remove common stop words and extract meaningful keywords
            stop_words = {'give', 'me', 'all', 'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 
                         'at', 'to', 'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were', 'whose',
                         'what', 'which', 'when', 'where', 'why', 'how', 'many', 'much', 'few',
                         'show', 'display', 'list', 'find', 'search', 'get', 'return'}
            
            words = query_lower.split()
            keywords = [word for word in words if word not in stop_words and len(word) > 2]
            
            # Also look for specific patterns like numbers (e.g., "105")
            numbers = re.findall(r'\b\d+\b', query)
            keywords.extend(numbers)
            
            # Add common column name variations
            column_variations = {
                'name': ['name', 'names', 'firstname', 'lastname', 'fullname'],
                'id': ['id', 'identifier', 'code', 'number'],
                'customer': ['customer', 'client', 'user', 'account'],
                'order': ['order', 'purchase', 'transaction'],
                'product': ['product', 'item', 'sku', 'goods']
            }
            
            # Expand keywords with variations
            expanded_keywords = []
            for keyword in keywords:
                expanded_keywords.append(keyword)
                for base, variations in column_variations.items():
                    if keyword in variations:
                        expanded_keywords.extend(variations)
                        break
            
            keywords = list(set(expanded_keywords))  # Remove duplicates
            
            print(f"Extracted keywords: {keywords}", file=sys.stderr)
            
            # Search for tables and columns containing keywords
            discovered_tables = set()
            table_details_map = {}
            
            for keyword in keywords[:5]:  # Limit to top 5 keywords to avoid too many searches
                # Search for tables with keyword in name
                search_result = await search_tables_by_name(keyword, catalog_name)
                
                if "Error" not in search_result and search_result.strip():
                    # Parse the search results to get table names
                    lines = search_result.split('\n')
                    for line in lines[2:]:  # Skip header lines
                        if '|' in line and line.strip() and not line.startswith('-'):
                            parts = [p.strip() for p in line.split('|')]
                            if len(parts) >= 3 and parts[2]:  # table_name is in position 2
                                full_table = f"{parts[0]}.{parts[1]}.{parts[2]}"
                                discovered_tables.add(full_table)
                
                # Also search for tables with keyword in description
                desc_result = await search_tables_by_description(keyword, catalog_name)
                if "Error" not in desc_result and desc_result.strip():
                    lines = desc_result.split('\n')
                    for line in lines[2:]:
                        if '|' in line and line.strip() and not line.startswith('-'):
                            parts = [p.strip() for p in line.split('|')]
                            if len(parts) >= 3 and parts[2]:
                                full_table = f"{parts[0]}.{parts[1]}.{parts[2]}"
                                discovered_tables.add(full_table)
                
                # Search for columns containing keywords
                try:
                    column_result = await search_columns_by_name(keyword, catalog_name, schema_name)
                    if "Error" not in column_result and column_result.strip():
                        lines = column_result.split('\n')
                        for line in lines[2:]:
                            if '|' in line and line.strip() and not line.startswith('-'):
                                parts = [p.strip() for p in line.split('|')]
                                if len(parts) >= 3 and parts[2]:  # table_name is in position 2
                                    full_table = f"{parts[0]}.{parts[1]}.{parts[2]}"
                                    discovered_tables.add(full_table)
                except Exception as e:
                    print(f"Error searching columns for '{keyword}': {e}", file=sys.stderr)
                    continue
            
            # Get detailed information for discovered tables
            for table in list(discovered_tables)[:5]:  # Limit to top 5 tables
                try:
                    table_details = await get_table_details(table, include_lineage=False)
                    table_details_map[table] = table_details
                    schema_context += f"\n{table_details}\n---\n"
                except Exception as e:
                    print(f"Error getting details for table '{table}': {e}", file=sys.stderr)
                    continue
        
        # Generate SQL with the discovered schema context
        generated_sql = generate_sql_from_natural_language(query, catalog_name, schema_name, schema_context)
        
        # Check if there was an error generating SQL
        if generated_sql.startswith("Error:"):
            return generated_sql
        
        # Execute the generated SQL
        result = await execute_sql_query(generated_sql)
        
        response = f"**Generated SQL:**\n```sql\n{generated_sql}\n```\n\n"
        if schema_context:
            response += f"**Discovered Schema Context:**\n{schema_context}\n\n"
        response += f"**Results:**\n{result}"
        
        return response
        
    except Exception as e:
        return f"Error in smart natural language query: {str(e)}"

@mcp.tool()
async def clear_cache() -> str:
    """
    Clears internal caches used for lineage and metadata operations.
    
    Use this tool if you want to free up memory or ensure fresh data after schema changes.
    """
    try:
        await asyncio.to_thread(clear_lineage_cache)
        return "Cache cleared successfully."
    except Exception as e:
        return f"Error clearing cache: {str(e)}"

@mcp.tool()
async def execute_sql_query(sql: str) -> str:
    """
    Executes a given SQL query against the Databricks SQL warehouse and returns the formatted results.
    
    Use this tool when you need to run specific SQL queries, such as SELECT, SHOW, or other DQL statements.
    This is ideal for targeted data retrieval or for queries that are too complex for the structured description tools.
    The results are returned in a human-readable, Markdown-like table format.

    Args:
        sql: The complete SQL query string to execute.
    """
    try:
        sdk_result = await asyncio.to_thread(execute_databricks_sql, sql_query=sql)
        
        status = sdk_result.get("status")
        if status == "failed":
            error_message = sdk_result.get("error", "Unknown query execution error.")
            details = sdk_result.get("details", "No additional details provided.")
            return f"SQL Query Failed: {error_message}\nDetails: {details}"
        elif status == "error":
            error_message = sdk_result.get("error", "Unknown error during SQL execution.")
            details = sdk_result.get("details", "No additional details provided.")
            return f"Error during SQL Execution: {error_message}\nDetails: {details}"
        elif status == "success":
            return format_query_results(sdk_result)
        else:
            # Should not happen if execute_databricks_sql always returns a known status
            return f"Received an unexpected status from query execution: {status}. Result: {sdk_result}"
            
    except Exception as e:
        return f"An unexpected error occurred while executing SQL query: {str(e)}"

@mcp.tool()
async def natural_language_query(
    query: str,
    catalog_name: Optional[str] = None,
    schema_name: Optional[str] = None
) -> str:
    """
    Converts natural language to SQL and executes it against Databricks SQL Warehouse.
    
    Use this tool when you want to query data using natural language instead of writing SQL.
    The tool automatically generates appropriate SQL based on your question and executes it.
    
    NOTE: For better results with schema awareness, consider using smart_natural_language_query instead.
    
    Args:
        query: Natural language question about your data
        catalog_name: Optional catalog name for schema context
        schema_name: Optional schema name for schema context
    """
    # Generate SQL from natural language
    generated_sql = generate_sql_from_natural_language(query, catalog_name, schema_name)
    
    # Check if there was an error generating SQL
    if generated_sql.startswith("Error:"):
        return generated_sql
    
    # Execute the generated SQL
    try:
        result = await execute_sql_query(generated_sql)
        return f"Generated SQL: ```sql\n{generated_sql}\n```\n\nResults:\n{result}"
    except Exception as e:
        return f"Error executing generated SQL '{generated_sql}': {str(e)}"



def handle_json_rpc_request(request_data: str) -> str:
    """
    Handle JSON-RPC requests from the client
    """
    try:
        request = json.loads(request_data)
        method = request.get("method")
        params = request.get("params", {})
        request_id = request.get("id", 1)
        
        # Map methods to async functions
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
            # Run the async function synchronously for this simple implementation
            result = asyncio.run(method_map[method](**params))
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": result
            }
        else:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32601,
                    "message": f"Method not found: {method}"
                }
            }
        
        return json.dumps(response) + "\n"
        
    except Exception as e:
        return json.dumps({
            "jsonrpc": "2.0",
            "id": request.get("id", 1),
            "error": {
                "code": -32603,
                "message": f"Internal error: {str(e)}"
            }
        }) + "\n"

# Replace the main block with this:
if __name__ == "__main__":
    print("Starting Databricks MCP server in stdio mode...", file=sys.stderr)
    logger.info("MCP Server starting up")
    
    # Read from stdin and write to stdout for JSON-RPC communication
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
                "jsonrpc": "2.0",
                "id": None,
                "error": {
                    "code": -32603,
                    "message": f"Server error: {str(e)}"
                }
            }) + "\n"
            sys.stdout.write(error_response)
            sys.stdout.flush()