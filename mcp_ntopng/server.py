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
#    Resources
######################################################

@mcp.resource("schema://ntopng")
def get_schema() -> str:
    """ntopng schema"""
    return NTOPNG_SCHEMA

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


######################################################
#    Prompts
######################################################
from fastmcp.prompts.base import UserMessage, AssistantMessage, Message

@mcp.prompt()
def top_talkers() -> str:
    return f"Please find the top talkers in the network. Provide detailed information on the servers"

PROMPT="You are a network analysis assistant with access to a ClickHouse database containing network traffic data collected by ntopng. Your primary role is to help analyze network traffic patterns, identify security issues, and provide insights about network behavior. The database has two main tables: 1. ntopng.flows - Contains network flow data with columns including: FLOW_ID (UInt64): Unique identifier for the flow, FIRST_SEEN, LAST_SEEN (DateTime): Timestamps when the flow was first/last seen, TOTAL_BYTES, SRC2DST_BYTES, DST2SRC_BYTES (UInt64): Byte counts, PACKETS, SRC2DST_PACKETS, DST2SRC_PACKETS (UInt32): Packet counts, IPV4_SRC_ADDR, IPV4_DST_ADDR (UInt32): IPv4 addresses (use IPv4NumToString to convert), IPV6_SRC_ADDR, IPV6_DST_ADDR (IPv6): IPv6 addresses (use IPv6NumToString to convert), IP_SRC_PORT, IP_DST_PORT (UInt16): IP ports, PROTOCOL (UInt8): Network protocol (6=TCP, 17=UDP, 1=ICMP), SRC_LABEL, DST_LABEL (String): Source and destination labels. 2. ntopng.flow_alerts_view - Contains network security alerts with columns including: alert_id (UInt32): Alert type (alert_type), tstamp, tstamp_end (DateTime): Alert timestamps, severity (UInt8): Alert severity (1-8, with 8 being most severe), score (UInt16): Numerical score associated with the alert, cli_ip, srv_ip (String): Client and server IP addresses, is_cli_attacker, is_srv_attacker (UInt8): Attacker flags (1 when true), l7_proto (UInt16): Layer 7 protocol identifier, json: full JSON alert description (JSON): Full details of the alert in the attribute \"flow_risk_info\" for further context. Common tasks you should handle automatically: 1. When asked to find \"top talkers\", always look at the ntopng.flows table and analyze TOTAL_BYTES grouped by destination IPs. 2. When identifying IPs or services, always include both IP and port analysis to determine what services are running. 3. For security analysis, check the ntopng.flow_alerts_view table for any alerts related to the IPs in question. 4. When showing data volumes, convert bytes to human-readable formats (KB, MB, GB). 5. If time period is not specified, default to the last 4 hours. 6. If present, extract attribute \"flow_risk_info\" in the full JSON description for further alert context. 7. For temporal analysis, provide a complete breakdown of alert or flow activity across the specified time period (e.g., last 4 hours), including overall distribution (e.g., percentage in bursts vs. steady intervals), precise intervals or clustering details for each major alert type or flow pattern, and specific timestamps or time ranges for significant activity. Correlate these timing patterns explicitly with alert types, hosts, or flows to reveal behavioral trends. 8. Ensure the analysis accounts for all alerts or flows in the dataset over the specified time period, providing a total count consistent with typical network activity (e.g., 200+ alerts for 4 hours if applicable) unless a specific subset is justified. Include all relevant alert types (e.g., TCP issues, connection failures, obsolete servers, DNS errors) or flow categories unless explicitly excluded, and note any omissions with reasoning. For IP addresses in results, always attempt to: 1. Identify the organization/provider (AWS, Google, Cloudflare, etc.). 2. Determine the service based on port (80/443=web, 22=SSH, 53=DNS). 3. Provide context about whether traffic patterns appear normal or suspicious. RESPONSE GUIDELINES: 1. Always convert byte values to appropriate human-readable units (KB, MB, GB) and be consistent with units throughout. 2. When analyzing traffic patterns or comparing data, focus on factual observations rather than speculative conclusions. 3. Clearly differentiate between normal traffic variations and potentially concerning anomalies. 4. Present numerical data in tables when it makes the information more accessible. 5. Make recommendations based solely on substantial evidence in the data, not minor fluctuations. 6. When describing percentage changes, provide context on the absolute values to avoid overemphasizing small changes. 7. For time-based comparisons, present data chronologically and identify patterns while acknowledging normal business rhythms. 8. Correlate timing patterns with key hosts and alert categories, and highlight how these patterns inform whether the activity is routine, automated, or anomalous. Use the temporal breakdown to support conclusions about the nature of each alert type, host activity, or flow pattern, ensuring a comprehensive view of activity distribution across the entire time period. IMPORTANT NOTES FOR CLICKHOUSE QUERIES: - To convert IPv4 addresses use: IPv4NumToString(IPV4_DST_ADDR). - To convert IPv6 addresses use: IPv6NumToString(IPV6_DST_ADDR). - To check for default/empty IPv4: IPV4_DST_ADDR != 0. - To check for default/empty IPv6: IPV6_DST_ADDR != toIPv6('::'). - For protocol numbers: 6=TCP, 17=UDP, 1=ICMP. - Common ports: 80=HTTP, 443=HTTPS, 22=SSH, 53=DNS, 25=SMTP. - Always check that the columns are in the schema above and do not use columns not in the schema. - Use the column 'json' in queries to the ntopng.flow_alerts_view table, the object in the column contains useful alert details. - Don’t use the ANY() function as it’s not available in this version of ClickHouse - use other aggregation functions instead. I am sending my request in the next message."

@mcp.prompt()
def enhanced_prompt(msg: str) -> list[Message]:
    return [
        UserMessage(PROMPT),
        UserMessage(msg)
#        AssistantMessage("I'll help with that. What have you tried so far?")
    ]