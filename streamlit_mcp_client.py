import streamlit as st
import requests
import json
import subprocess
import threading
import time
import os
from typing import Optional, Dict, Any
import sys

class MCPClient:
    def __init__(self):
        self.process = None
        self.server_stdin = None
        self.server_stdout = None
        
    def start_server(self):
        """Start the MCP server as a subprocess"""
        try:
            # Start the server process
            self.process = subprocess.Popen(
                [sys.executable, "main2.py"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            self.server_stdin = self.process.stdin
            self.server_stdout = self.process.stdout
            
            # Wait for server to initialize
            time.sleep(2)
            return True
        except Exception as e:
            st.error(f"Failed to start server: {e}")
            return False
    
    def stop_server(self):
        """Stop the MCP server"""
        if self.process:
            self.process.terminate()
            self.process.wait()
            self.process = None
    
    def send_request(self, method: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Send a JSON-RPC request to the server"""
        if not self.server_stdin or not self.server_stdout:
            return {"error": "Server not connected"}
        
        request_id = 1
        request = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params or {}
        }
        
        try:
            # Send request
            request_json = json.dumps(request) + "\n"
            self.server_stdin.write(request_json)
            self.server_stdin.flush()
            
            # Read response
            response_line = self.server_stdout.readline()
            if response_line:
                return json.loads(response_line)
            else:
                return {"error": "No response from server"}
                
        except Exception as e:
            return {"error": f"Communication error: {e}"}

def main():
    st.set_page_config(
        page_title="Databricks MCP Client",
        page_icon="🔍",
        layout="wide"
    )
    
    st.title("🔍 Databricks MCP Client")
    st.markdown("Interactive client for exploring Databricks Unity Catalog and executing SQL queries")
    
    # Initialize client
    if 'mcp_client' not in st.session_state:
        st.session_state.mcp_client = MCPClient()
    
    # Sidebar for server control
    with st.sidebar:
        st.header("Server Control")
        
        if st.button("🚀 Start Server"):
            if st.session_state.mcp_client.start_server():
                st.success("Server started successfully!")
            else:
                st.error("Failed to start server")
        
        if st.button("🛑 Stop Server"):
            st.session_state.mcp_client.stop_server()
            st.info("Server stopped")
        
        st.divider()
        st.header("Connection Info")
        st.info("Make sure your .env file contains:\n- DATABRICKS_HOST\n- DATABRICKS_TOKEN\n- DATABRICKS_SQL_WAREHOUSE_ID")
    
    # Main content tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Catalog Explorer", 
        "🔍 SQL Query", 
        "💬 Natural Language", 
        "📋 Table Details", 
        "⚙️ Tools"
    ])
    
    with tab1:
        st.header("Catalog Explorer")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📚 List All Catalogs"):
                result = st.session_state.mcp_client.send_request("list_all_catalogs")
                if "result" in result:
                    st.markdown(result["result"])
                else:
                    st.error(f"Error: {result.get('error', 'Unknown error')}")
        
        with col2:
            catalog_name = st.text_input("Enter Catalog Name for details:")
            if st.button("🔍 Explore Catalog") and catalog_name:
                result = st.session_state.mcp_client.send_request("list_schemas_in_catalog", {"catalog_name": catalog_name})
                if "result" in result:
                    st.markdown(result["result"])
                else:
                    st.error(f"Error: {result.get('error', 'Unknown error')}")
    
    with tab2:
        st.header("SQL Query Executor")
        
        sql_query = st.text_area(
            "Enter SQL Query:",
            height=150,
            placeholder="SELECT * FROM your_catalog.your_schema.your_table LIMIT 10"
        )
        
        if st.button("🚀 Execute SQL"):
            if sql_query.strip():
                result = st.session_state.mcp_client.send_request("execute_sql_query", {"sql": sql_query})
                if "result" in result:
                    st.text_area("Results:", result["result"], height=300)
                else:
                    st.error(f"Error: {result.get('error', 'Unknown error')}")
            else:
                st.warning("Please enter a SQL query")
    
    with tab3:
        st.header("Natural Language Query")
        
        nl_query = st.text_area(
            "Ask about your data:",
            height=100,
            placeholder="e.g., Show me the top 10 customers by total orders"
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            catalog_name_nl = st.text_input("Catalog (optional):", key="catalog_nl")
        
        with col2:
            schema_name_nl = st.text_input("Schema (optional):", key="schema_nl")
        
        if st.button("🔮 Generate & Execute"):
            if nl_query.strip():
                result = st.session_state.mcp_client.send_request("smart_natural_language_query", {
                    "query": nl_query,
                    "catalog_name": catalog_name_nl or None,
                    "schema_name": schema_name_nl or None
                })
                if "result" in result:
                    st.markdown(result["result"])
                else:
                    st.error(f"Error: {result.get('error', 'Unknown error')}")
            else:
                st.warning("Please enter a natural language query")
    
    with tab4:
        st.header("Table Details Explorer")
        
        full_table_name = st.text_input(
            "Full Table Name (catalog.schema.table):",
            placeholder="your_catalog.your_schema.your_table"
        )
        
        include_lineage = st.checkbox("Include Lineage Information")
        
        if st.button("📋 Get Table Details") and full_table_name:
            result = st.session_state.mcp_client.send_request("get_table_details", {
                "full_table_name": full_table_name,
                "include_lineage": include_lineage
            })
            if "result" in result:
                st.markdown(result["result"])
            else:
                st.error(f"Error: {result.get('error', 'Unknown error')}")
    
    with tab5:
        st.header("Additional Tools")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Search Tables")
            table_pattern = st.text_input("Table name pattern:")
            catalog_search = st.text_input("Catalog (optional):")
            
            if st.button("🔍 Search Tables"):
                result = st.session_state.mcp_client.send_request("search_tables_by_name", {
                    "table_name_pattern": table_pattern,
                    "catalog_name": catalog_search or None
                })
                if "result" in result:
                    st.text_area("Search Results:", result["result"], height=200)
                else:
                    st.error(f"Error: {result.get('error', 'Unknown error')}")
        
        with col2:
            st.subheader("Search Columns")
            column_pattern = st.text_input("Column name pattern:")
            catalog_col = st.text_input("Catalog (optional):", key="catalog_col")
            schema_col = st.text_input("Schema (optional):", key="schema_col")
            
            if st.button("🔍 Search Columns"):
                result = st.session_state.mcp_client.send_request("search_columns_by_name", {
                    "column_name_pattern": column_pattern,
                    "catalog_name": catalog_col or None,
                    "schema_name": schema_col or None
                })
                if "result" in result:
                    st.text_area("Column Results:", result["result"], height=200)
                else:
                    st.error(f"Error: {result.get('error', 'Unknown error')}")
        
        if st.button("🧹 Clear Cache"):
            result = st.session_state.mcp_client.send_request("clear_cache")
            if "result" in result:
                st.success(result["result"])
            else:
                st.error(f"Error: {result.get('error', 'Unknown error')}")

if __name__ == "__main__":
    main()