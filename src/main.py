import argparse
import sys  
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# Ensure src is in path so we can import from src.service
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def main():
    """
    Main entry point for the mcp-server-kafka script.
    It runs the MCP server with a specific transport protocol.
    """

    # Parse the command-line arguments to determine the transport protocol.
    parser = argparse.ArgumentParser(description="kafka-mcp-server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="stdio",
    )
    args = parser.parse_args()

    # Import is done here to make sure environment variables are loaded
    # only after we make the changes.
    from server import mcp_server
    # checking kafka docker is running or not
    # if not running then start the kafka docker

    TODO
    
    # run the mcp servers
    mcp_server.run(transport=args.transport)

if __name__ == "__main__":
    main()