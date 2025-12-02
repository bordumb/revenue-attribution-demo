"""
Real PostgreSQL MCP Server.
Connects to a live database and exposes schema information via MCP.

Requires: pip install asyncpg mcp
"""

import asyncio
import os
import logging
import asyncpg
from mcp.server.stdio import stdio_server
from mcp.server import Server
from mcp.types import Tool, TextContent, ImageContent, EmbeddedResource

# Configure logging to file (since stdout is used for MCP protocol)
logging.basicConfig(level=logging.INFO, filename="mcp_server.log")
logger = logging.getLogger(__name__)

# Initialize MCP Server
app = Server("postgres-live-catalog")

async def get_db_connection():
    """Create connection using environment variables."""
    # Defaults for the docker-compose setup
    return await asyncpg.connect(
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "password"),
        database=os.getenv("POSTGRES_DB", "analytics"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432")
    )

@app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="get_table_schema",
            description="Retrieve schema and column metadata from the live PostgreSQL database",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"}
                },
                "required": ["table_name"]
            }
        )
    ]

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent | ImageContent | EmbeddedResource]:
    if name == "get_table_schema":
        table_name = arguments.get("table_name")
        return await fetch_table_schema(table_name)
    
    raise ValueError(f"Unknown tool: {name}")

async def fetch_table_schema(table_name: str) -> list[TextContent]:
    """Query information_schema to build a DDL-like description."""
    conn = None
    try:
        conn = await get_db_connection()
        
        # 1. Get Columns
        rows = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = $1
            ORDER BY ordinal_position
        """, table_name)
        
        if not rows:
            return [TextContent(type="text", text=f"Table '{table_name}' not found in database.")]

        # 2. Format as DDL
        lines = [f"CREATE TABLE {table_name} ("]
        for row in rows:
            nullable = "NULL" if row['is_nullable'] == 'YES' else "NOT NULL"
            lines.append(f"    {row['column_name']} {row['data_type']} {nullable},")
        
        # 3. Get Primary Keys (Optional bonus context)
        pk_rows = await conn.fetch("""
            SELECT c.column_name
            FROM information_schema.table_constraints tc 
            JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name 
            AND tc.table_schema = ccu.table_schema
            JOIN information_schema.columns c ON c.table_name = tc.table_name AND c.column_name = ccu.column_name
            WHERE constraint_type = 'PRIMARY KEY' AND tc.table_name = $1
        """, table_name)
        
        if pk_rows:
            pks = ", ".join([r['column_name'] for r in pk_rows])
            lines.append(f"    PRIMARY KEY ({pks})")
            
        lines.append(");")
        
        schema_text = "\n".join(lines)
        logger.info(f"Retrieved schema for {table_name}")
        return [TextContent(type="text", text=schema_text)]

    except Exception as e:
        logger.error(f"DB Error: {e}")
        return [TextContent(type="text", text=f"Database error: {str(e)}")]
    
    finally:
        if conn:
            await conn.close()

async def main():
    logger.info("Starting Postgres MCP Server...")
    # Run the server over Stdio
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())

if __name__ == "__main__":
    asyncio.run(main())
