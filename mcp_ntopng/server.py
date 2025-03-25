import logging
import concurrent.futures
import atexit
import requests

from clickhouse_driver import Client
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP, Context

load_dotenv()
from mcp_ntopng.mcp_config import config

import os
from typing import Dict, Any, List, Sequence

NTOPNG_HOST = os.getenv("NTOPNG_HOST")
if not NTOPNG_HOST:
    raise ValueError("NTOPNG_HOST environment variable not set")
BASE_URL = f"https://{NTOPNG_HOST}"

# Retrieve the API key from an environment variable
NTOPNG_API_KEY = os.getenv("NTOPNG_API_KEY")
if not NTOPNG_API_KEY:
    raise ValueError("NTOPNG_API_KEY environment variable not set")

# Headers for authentication
HEADERS = {
    "Authorization": f"Token {NTOPNG_API_KEY}",
    "Content-Type": "application/json"
}

MCP_SERVER_NAME = "mcp-ntopng"

deps = [
    "starlette",
    "clickhouse-driver",
    "python-dotenv",
    "uvicorn",
    "pip-system-certs",
    "requests"
]

# Basic logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(MCP_SERVER_NAME)

# Global settings for query execution
QUERY_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=10)
SELECT_QUERY_TIMEOUT_SECS = 30
# Wait for the pending queries to return at exit
atexit.register(lambda: QUERY_EXECUTOR.shutdown(wait=True))

mcp = FastMCP(MCP_SERVER_NAME, dependencies=deps)


######################################################
#    ntopng Clickhouse database access
######################################################

def create_clickhouse_client():
    """
    Creates and validates a connection to the ClickHouse database.
    
    Retrieves connection parameters from config, establishes a connection,
    and verifies it by checking the server version.
    
    Returns:
        Client: A configured and tested ClickHouse client instance
        
    Raises:
        ConnectionError: When connection cannot be established
        ConfigurationError: When configuration is invalid
    """
    # Get configuration from the global config instance
    client_config = config.get_client_config()
    
    logger.info(
        f"Creating ClickHouse client connection to {client_config['host']}:{client_config['port']} "
        f"as {client_config['user']} "
        f"(secure={client_config['secure']}, verify={client_config['verify']}, "
        f"connect_timeout={client_config['connect_timeout']}s, "
        f"send_receive_timeout={client_config['send_receive_timeout']}s, "
        f"database={client_config['database']})"
    )
    
    try:
        # Establish connection to ClickHouse using clickhouse_driver.Client
        client = Client(**client_config)
        
        # Test connection by querying server version
        version = client.execute("SELECT version()")[0][0]
        logger.info(f"Successfully connected to ClickHouse server version {version}")
        
        return client
        
    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Failed to connect to ClickHouse: {str(e)}", exc_info=True)
        raise ConnectionError(f"Unable to connect to ClickHouse: {str(e)}")

def execute_query(query: str):
    """
    Executes a ClickHouse query and returns structured results optimized for LLM function calling.
    
    Args:
        query (str): The SQL query to execute
    
    Returns:
        dict: A dictionary containing:
            - status (str): "success" or "error"
            - data (list): List of row dictionaries (on success)
            - metadata (dict): Information about the query results (on success)
            - error (str): Error message (on error)
    """
    import datetime
    client = create_clickhouse_client()
    
    # Create a response structure optimized for LLM consumption
    response = {
        "status": "success",
        "data": [],
        "metadata": {},
        "error": None
    }
    
    try:
        # Execute the query directly
        result = client.execute(query, with_column_types=True)
        
        # clickhouse-driver returns (data, column_types) when with_column_types=True
        rows = result[0]
        column_types = result[1]
        column_names = [col[0] for col in column_types]
        
        # Process result rows into dictionaries
        data_rows = []
        for row in rows:
            row_dict = {}
            for i, col_name in enumerate(column_names):
                row_dict[col_name] = row[i]
            data_rows.append(row_dict)
        
        # Add data and metadata to response
        response["data"] = data_rows
        response["metadata"] = {
            "row_count": len(data_rows),
            "column_names": column_names,
            "column_types": [col[1] for col in column_types],
            "query_time": datetime.datetime.now().isoformat(),
            "query": query,
        }
        
        logger.info(f"Query returned {len(data_rows)} rows")
        
    except Exception as err:
        # Consistent error handling with detailed information
        error_message = str(err)
        logger.error(f"Error executing query: {error_message}")
        
        # Update response for error case
        response["status"] = "error"
        response["error"] = error_message
        response["data"] = []  # Ensure empty data on error
    
    return response

from .ntopng_schema import NTOPNG_SCHEMA
@mcp.tool("list_tables_ntopng_database", description="List tables structure of the ntopng database")
def list_tables():
    logger.info("Returning predefined table schemas for 'ntopng'")

    return NTOPNG_SCHEMA

@mcp.tool(name="query_ntopng_database", description="Query the ntopng Clickhouse database.")
def query_ntopngdb(query: str, ctx: Context):
    """
    Executes a query against the ntopng database with timeout protection.
    
    Args:
        query (str): SQL query to execute
        
    Returns:
        dict: Response object with status, data, and error information
    """
    # Log query for debugging and audit purposes
    logger.info(f"Executing query: {query}")
    #ctx.log.info(f"Executing query: {query}")
    
    # Enforce SELECT query for security (prevent modification operations)
    if not query.strip().upper().startswith("SELECT"):
        return {
            "status": "error",
            "error": "Only SELECT queries are permitted",
            "data": [],
            "metadata": {"query": query}
        }
    
    # Submit query to thread pool
    future = QUERY_EXECUTOR.submit(execute_query, query)
    
    try:
        # Wait for result with timeout
        result = future.result(timeout=SELECT_QUERY_TIMEOUT_SECS)
        return result
        
    except concurrent.futures.TimeoutError:
        # Handle query timeout
        logger.warning(f"Query timed out after {SELECT_QUERY_TIMEOUT_SECS} seconds: {query}")
        
        # Attempt to cancel the running query (may not work depending on database driver)
        future.cancel()
        
        # Return a standardized error response
        return {
            "status": "error",
            "error": f"Query timeout after {SELECT_QUERY_TIMEOUT_SECS} seconds",
            "data": [],
            "metadata": {
                "query": query,
                "timeout_seconds": SELECT_QUERY_TIMEOUT_SECS
            }
        }
    
    except Exception as e:
        # Catch any other exceptions that might occur
        logger.error(f"Unexpected error executing query: {str(e)}")
        return {
            "status": "error",
            "error": f"Unexpected error: {str(e)}",
            "data": [],
            "metadata": {"query": query}
        }



######################################################
#    ntopng API
######################################################

# Function to fetch all ifid values - validated
@mcp.tool(name="fetch_ntopng_all_ifids", description="Retrieve all available interface IDs from ntopng.")
def get_all_ifids() -> List[int]:
    """
    Retrieve all available interface IDs (ifid) from ntopng.

    Returns:
        List[int]: A list of all ifid values.

    Raises:
        requests.RequestException: If the API call fails.
        KeyError: If the response JSON structure is unexpected.
    """
    url = f"{BASE_URL}/lua/rest/v2/get/ntopng/interfaces.lua"
    response = requests.get(url, headers=HEADERS, verify=True)
    response.raise_for_status()
    data = response.json()
    if data["rc"] != 0:
        raise ValueError(f"API error: {data['rc_str']}")
    # Assuming rsp is a list of dicts with 'ifid' keys
    ifid_list = [interface["ifid"] for interface in data["rsp"]]
    return ifid_list


# Find the local top talkers: ok but it is the pro version
@mcp.tool(name="fetch_ntopng_top_local_talkers", description="Retrieve the top 10 local talkers for a specified interface.")
def get_top_local_talkers(ifid: int) -> Dict[str, Any]:
    """
    Get the top 10 local talkers for a specified interface.

    Args:
        ifid (int): Interface ID.

    Returns:
        Dict[str, Any]: JSON response with top local talkers data.

    Raises:
        requests.RequestException: If the request fails.
    """
    url = f"{BASE_URL}/lua/pro/rest/v2/get/interface/top/local/talkers.lua"
    params = {"ifid": ifid}
    response = requests.get(url, headers=HEADERS, params=params, verify=True)
    response.raise_for_status()
    return response.json()

# Find the remote top talkers: ok but it is the pro version
@mcp.tool(name="fetch_ntopng_top_remote_talkers", description="Retrieve the top 10 remote talkers for a specified interface.")
def get_top_remote_talkers(ifid: int) -> Dict[str, Any]:
    """
    Get the top 10 remote talkers for a specified interface.

    Args:
        ifid (int): Interface ID.

    Returns:
        Dict[str, Any]: JSON response with top remote talkers data.

    Raises:
        requests.RequestException: If the request fails.
    """
    url = f"{BASE_URL}/lua/pro/rest/v2/get/interface/top/remote/talkers.lua"
    params = {"ifid": ifid}
    response = requests.get(url, headers=HEADERS, params=params, verify=True)
    response.raise_for_status()
    return response.json()

# Get stats on alert categories. not ok: both pro and non-pro paths return empty objects
@mcp.tool(name="get_ntopng_all_alert_stats", description="Retrieve statistics for all alerts.")
def get_all_alert_stats(ifid: int, epoch_begin: int, epoch_end: int) -> Dict[str, Any]:
    """
    Get all alert statistics.

    Args:
        ifid (int): Interface identifier.
        epoch_begin (int): Start time (epoch).
        epoch_end (int): End time (epoch).

    Returns:
        Dict[str, Any]: JSON response with alert stats.

    Raises:
        requests.RequestException: If the request fails.
    """
    url = f"{BASE_URL}/lua/pro/rest/v2/get/all/alert/top.lua"
    params = {"ifid": ifid, "epoch_begin": epoch_begin, "epoch_end": epoch_end}
    response = requests.get(url, headers=HEADERS, params=params, verify=True)
    response.raise_for_status()
    return response.json()
