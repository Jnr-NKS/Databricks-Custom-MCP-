import streamlit as st
import subprocess
import time
import sys
import json

# Page config
st.set_page_config(
    page_title="Databricks Query Client",
    page_icon="🔍",
    layout="wide"
)

class MCPClient:
    def __init__(self):
        self.process = None
        self.server_stdin = None
        self.server_stdout = None

    def start_server(self):
        """Start the MCP server as a subprocess"""
        try:
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
            time.sleep(2)  # give server time to boot
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

    def send_request(self, method: str, params: dict = None) -> dict:
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
            self.server_stdin.write(json.dumps(request) + "\n")
            self.server_stdin.flush()
            response_line = self.server_stdout.readline()
            if response_line:
                return json.loads(response_line)
            return {"error": "No response from server"}
        except Exception as e:
            return {"error": f"Communication error: {e}"}


def main():
    st.title("🔍 Databricks Query Client")

    # Init client
    if "mcp_client" not in st.session_state:
        st.session_state.mcp_client = MCPClient()

    # Sidebar server control
    with st.sidebar:
        st.header("Server Control")
        if st.button("🚀 Start Server"):
            if st.session_state.mcp_client.start_server():
                st.success("Server started")
        if st.button("🛑 Stop Server"):
            st.session_state.mcp_client.stop_server()
            st.info("Server stopped")
        st.markdown("ℹ️ Ensure `.env` contains:\n- DATABRICKS_HOST\n- DATABRICKS_TOKEN\n- DATABRICKS_SQL_WAREHOUSE_ID")

    # Two tabs only
    tab1, tab2 = st.tabs(["💬 Natural Language Query", "📝 SQL Query"])

    with tab1:
        st.subheader("Ask your question in plain English")
        nl_query = st.text_area("Natural Language Query", placeholder="e.g. Show all customer names with ID 105")
        if st.button("Run Natural Language Query"):
            if nl_query.strip():
                result = st.session_state.mcp_client.send_request(
                    "smart_natural_language_query", {"query": nl_query}
                )
                if "result" in result:
                    st.markdown(result["result"])
                else:
                    st.error(result.get("error", "Unknown error"))
            else:
                st.warning("Please enter a query")

    with tab2:
        st.subheader("Write SQL directly")
        sql_query = st.text_area("SQL Query", placeholder="SELECT * FROM hive_metastore.default.customers LIMIT 10")
        if st.button("Run SQL Query"):
            if sql_query.strip():
                result = st.session_state.mcp_client.send_request(
                    "execute_sql_query", {"sql": sql_query}
                )
                if "result" in result:
                    st.text_area("Results", result["result"], height=300)
                else:
                    st.error(result.get("error", "Unknown error"))
            else:
                st.warning("Please enter a SQL query")


if __name__ == "__main__":
    main()
