from typing import Optional
import asyncio
from mcp.server.fastmcp import FastMCP  # type: ignore[import-not-found]
from databricks_formatter import format_query_results
from databricks_sdk_utils import (
    execute_databricks_sql
)
import google.generativeai as genai
import os
from dotenv import load_dotenv
import sys
import logging

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

def generate_sql_from_natural_language(query: str, catalog_name: Optional[str] = None, schema_name: Optional[str] = None) -> str:
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
        
        # Use Gemini to generate SQL
        model = genai.GenerativeModel('gemini-pro')
        prompt = f"{context}\n\nConvert this natural language query to Databricks SQL:\n\n{query}"
        
        response = model.generate_content(prompt)
        return response.text.strip().replace("```sql", "").replace("```", "").strip()
    except Exception as e:
        return f"Error generating SQL: {str(e)}"

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

if __name__ == "__main__":
    print("Starting Databricks MCP server...", file=sys.stderr)
    logger.info("MCP Server starting up")
    print("Available tools: execute_sql_query, natural_language_query", file=sys.stderr)
    try:
        mcp.run(transport="stdio")
    except Exception as e:
        logger.error(f"Server failed to start: {e}")
        print(f"Server error: {e}", file=sys.stderr)