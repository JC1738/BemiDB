#!/bin/bash

# DuckLake Server Management Script
# Usage: ./ducklake-server.sh {start|stop|restart|logs|status|test}

set -e

# Configuration
SERVER_PORT=15432
LOG_FILE_TRACKER="/tmp/bemidb-ducklake.logfile"
PID_FILE="/tmp/bemidb-ducklake.pid"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Load environment from .env.ducklake
load_env() {
    if [ -f .env.ducklake ]; then
        echo -e "${BLUE}Loading environment from .env.ducklake${NC}"
        set -a
        source .env.ducklake
        set +a

        # Use port from .env.ducklake if set, otherwise use default
        export BEMIDB_PORT="${BEMIDB_PORT:-$SERVER_PORT}"
        export BEMIDB_LOG_LEVEL="${BEMIDB_LOG_LEVEL:-DEBUG}"

        # Update SERVER_PORT variable to match actual port being used
        SERVER_PORT=$BEMIDB_PORT
    else
        echo -e "${RED}ERROR: .env.ducklake not found${NC}"
        exit 1
    fi
}

# Check if server is running
is_running() {
    if [ -f "$PID_FILE" ]; then
        local pid=$(cat "$PID_FILE")
        if ps -p "$pid" > /dev/null 2>&1; then
            return 0
        fi
    fi

    # Also check by port
    if lsof -ti:$SERVER_PORT > /dev/null 2>&1; then
        return 0
    fi

    return 1
}

# Get current log file path
get_log_file() {
    if [ -f "$LOG_FILE_TRACKER" ]; then
        cat "$LOG_FILE_TRACKER"
    else
        echo ""
    fi
}

# Start server
start_server() {
    if is_running; then
        echo -e "${YELLOW}Server is already running on port $SERVER_PORT${NC}"
        return 0
    fi

    load_env

    # Generate unique log file with timestamp
    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local LOG_FILE="/tmp/bemidb-ducklake-${timestamp}.log"
    echo "$LOG_FILE" > "$LOG_FILE_TRACKER"

    echo -e "${GREEN}Starting BemiDB server on port $SERVER_PORT...${NC}"
    echo -e "${BLUE}Log file: $LOG_FILE${NC}"

    # Start server in background
    nohup ./bin/server > "$LOG_FILE" 2>&1 &
    local pid=$!
    echo $pid > "$PID_FILE"

    # Wait for server to start
    echo -n "Waiting for server to start"
    for i in {1..15}; do
        if grep -q "BemiDB: Listening" "$LOG_FILE" 2>/dev/null; then
            echo -e "\n${GREEN}✓ Server started successfully (PID: $pid)${NC}"

            # Show DuckLake initialization status
            if grep -q "DuckLake: Initialized" "$LOG_FILE"; then
                local table_count=$(grep "DuckLake: Loaded" "$LOG_FILE" | tail -1 | grep -oP '\d+(?= tables)')
                echo -e "${GREEN}✓ DuckLake initialized ($table_count tables loaded)${NC}"
            fi

            return 0
        fi
        echo -n "."
        sleep 1
    done

    echo -e "\n${RED}✗ Server failed to start. Check logs:${NC}"
    tail -20 "$LOG_FILE"
    return 1
}

# Stop server
stop_server() {
    echo -e "${YELLOW}Stopping BemiDB server...${NC}"

    local stopped=0

    # Kill by PID file
    if [ -f "$PID_FILE" ]; then
        local pid=$(cat "$PID_FILE")
        if ps -p "$pid" > /dev/null 2>&1; then
            kill $pid 2>/dev/null || kill -9 $pid 2>/dev/null
            echo -e "${GREEN}✓ Killed server (PID: $pid)${NC}"
            stopped=1
        fi
        rm -f "$PID_FILE"
    fi

    # Kill by port
    if lsof -ti:$SERVER_PORT > /dev/null 2>&1; then
        lsof -ti:$SERVER_PORT | xargs kill -9 2>/dev/null
        echo -e "${GREEN}✓ Killed process on port $SERVER_PORT${NC}"
        stopped=1
    fi

    if [ $stopped -eq 0 ]; then
        echo -e "${BLUE}Server was not running${NC}"
    fi
}

# Show status
show_status() {
    if is_running; then
        local pid=$(cat "$PID_FILE" 2>/dev/null || lsof -ti:$SERVER_PORT)
        local log_file=$(get_log_file)
        echo -e "${GREEN}✓ Server is running (PID: $pid, Port: $SERVER_PORT)${NC}"

        if [ -n "$log_file" ]; then
            echo -e "${BLUE}Log file: $log_file${NC}"
        fi

        # Show last few log lines
        if [ -n "$log_file" ] && [ -f "$log_file" ]; then
            echo -e "\n${BLUE}Recent logs:${NC}"
            tail -10 "$log_file" | grep -E "(INFO|ERROR|WARN|DEBUG)" || tail -10 "$log_file"
        fi
    else
        echo -e "${RED}✗ Server is not running${NC}"
    fi
}

# Show logs
show_logs() {
    local log_file=$(get_log_file)

    if [ -z "$log_file" ] || [ ! -f "$log_file" ]; then
        echo -e "${YELLOW}No log file found. Server may not have been started.${NC}"
        return 1
    fi

    echo -e "${BLUE}Log file: $log_file${NC}"

    if [ "$1" = "-f" ]; then
        echo -e "${BLUE}Following logs (Ctrl+C to exit)...${NC}"
        tail -f "$log_file"
    else
        echo -e "${BLUE}Last 50 lines of logs:${NC}"
        tail -50 "$log_file"
    fi
}

# Rebuild server
rebuild_server() {
    echo -e "${BLUE}Rebuilding server...${NC}"
    cd src/server
    go build -o ../../bin/server .
    cd ../..
    echo -e "${GREEN}✓ Server rebuilt${NC}"
}

# Run test query
test_query() {
    if ! is_running; then
        echo -e "${RED}Server is not running. Start it first with: $0 start${NC}"
        return 1
    fi

    load_env

    echo -e "${BLUE}Testing connection...${NC}"
    echo ""

    # Get database name from environment or use default
    local dbname="${BEMIDB_DATABASE:-bemidb}"

    echo -e "${YELLOW}Running simple test query...${NC}"
    psql "host=localhost port=$SERVER_PORT user=postgres dbname=$dbname" -c "SELECT 1 AS test;"
}

# Main command handler
case "${1:-}" in
    start)
        start_server
        ;;
    stop)
        stop_server
        ;;
    restart)
        stop_server
        sleep 1
        start_server
        ;;
    rebuild)
        rebuild_server
        ;;
    rebuild-restart)
        rebuild_server
        stop_server
        sleep 1
        start_server
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs "${2:-}"
        ;;
    test)
        test_query
        ;;
    *)
        echo "DuckLake Server Management Script"
        echo ""
        echo "Usage: $0 {command}"
        echo ""
        echo "Commands:"
        echo "  start            - Start the server"
        echo "  stop             - Stop the server"
        echo "  restart          - Restart the server"
        echo "  rebuild          - Rebuild the server binary"
        echo "  rebuild-restart  - Rebuild and restart the server"
        echo "  status           - Show server status"
        echo "  logs             - Show recent logs"
        echo "  logs -f          - Follow logs in real-time"
        echo "  test             - Test server connection"
        echo ""
        echo "Examples:"
        echo "  $0 start          # Start server"
        echo "  $0 logs -f        # Watch logs"
        echo "  $0 restart        # Restart server"
        echo "  $0 test           # Test connection"
        exit 1
        ;;
esac
