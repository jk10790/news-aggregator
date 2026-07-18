#!/bin/bash

# Get the absolute path of the directory where this script is located
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

echo "Launching iTerm2 tabs for the News Aggregator..."

osascript <<EOF
tell application "iTerm"
    activate
    
    -- Create a brand new window
    set newWindow to (create window with default profile)
    
    -- Tab 1: API Server
    tell current session of newWindow
        write text "cd '$PROJECT_DIR' && source .venv/bin/activate && clear && echo '🚀 Starting API Server...' && python api/main.py"
        set name to "API Server"
    end tell
    
    -- Tab 2: Triage Consumer
    tell newWindow
        create tab with default profile
        tell current session
            write text "cd '$PROJECT_DIR' && source .venv/bin/activate && clear && echo '🧠 Starting Triage Consumer...' && python ingestion/consumer_triage.py"
            set name to "Triage Consumer"
        end tell
    end tell

    -- Tab 3: Storage Consumer
    tell newWindow
        create tab with default profile
        tell current session
            write text "cd '$PROJECT_DIR' && source .venv/bin/activate && clear && echo '💾 Starting Storage Consumer...' && python storage/consumer_storage.py"
            set name to "Storage Consumer"
        end tell
    end tell
    
    -- Tab 4: Empty prompt for Producer / Debugging
    tell newWindow
        create tab with default profile
        tell current session
            write text "cd '$PROJECT_DIR' && source .venv/bin/activate && clear && echo '✅ Environment Ready.' && echo '---------------------------------------------------' && echo 'Examples for running the Producer:' && echo '' && echo '1. Run once immediately:' && echo '   python ingestion/producer.py' && echo '' && echo '2. Run with an hourly cadence (every 3600 seconds):' && echo '   watch -n 3600 python ingestion/producer.py' && echo '' && echo '3. Run in a loop (every 10 minutes):' && echo '   while true; do python ingestion/producer.py; sleep 600; done' && echo '---------------------------------------------------'"
            set name to "Terminal"
        end tell
    end tell

end tell
EOF
