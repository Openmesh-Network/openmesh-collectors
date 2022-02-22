from datetime import datetime
import sys

def slog(msg):
    log("server", msg)

def log(s, msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {s}: {msg}", flush=True)