# mcp-ntopng
An MCP server for network monitoring software ntopng

>Lite version 

## Development

### Setup

1. **Prerequisites**:
   - Python 3.13 or higher.
   - `uv` package manager ([installation](https://docs.astral.sh/uv/)).

2. **Clone the Repository**:
```bash
git clone https://github.com/marcoeg/mcp-nvd
cd mcp-nvd
```

3. **Set Environment Variables**:
   - Create a `.env` file in the project root with the mandatory environmental variables:
        ```
    NTOPNG_HOST=localhost
    NTOPNG_PORT=9000
    NTOPNG_USER=default
    NTOPNG_PASSWORD=
     ```

4. **Install Dependencies**:
```bash
uv sync
uv pip install -e .
```

### Run with the MCP Inspector
```bash
cd /path/to/the/repo
source .env

CLIENT_PORT=8077 SERVER_PORT=8078 npx @modelcontextprotocol/inspector uv \
    --directory /path/to/repo/mcp-ntopng run mcp-ntopng
 ```

Then open the browser to the URL indicated by the MCP Inspector, typically `http://localhost:8077?proxyPort=8078`

After connecting, list the Tools. Select `query_ntopng_database` and enter in the query a SELECT
statement like `SELECT * FROM host_alerts_view LIMIT 2`.

> Switch freely between `stdio` and `sse` transport types in the inspector. To use `sse` you need to run the server as explained below.


#### Run the Server:
```bash
cd /path/to/the/repo
source .env

uv run mcp-ntopng --transport sse --port 9090
```
- Runs with SSE transport on port `9090` by default.

>In the terminal running the server there is a full log of the activities. Very useful for debugging.
