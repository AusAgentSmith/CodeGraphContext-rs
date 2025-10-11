import os
from datetime import datetime

# Toggle this to True to enable debug logging
debug_mode = False  # Set to True for dev/test, False for production

def debug_log(message):
    """Write debug message to a file if debug_mode is enabled"""
    if not debug_mode:
        return
    debug_file = os.path.expanduser("~/mcp_debug.log")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(debug_file, "a") as f:
        f.write(f"[{timestamp}] {message}\n")
        f.flush()
