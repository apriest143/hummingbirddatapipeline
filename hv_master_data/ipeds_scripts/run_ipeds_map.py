#!/usr/bin/env python3
"""
IPEDS Map Viewer
================
Just run this file and the map will open in your browser.

Usage:
    python run_ipeds_map.py
    
To stop: Press Ctrl+C in the terminal
"""

import http.server
import socketserver
import webbrowser
import os
import threading
import time

# Configuration
PORT = 8000
MAP_FILE = "ipeds_institution_map.html"
CSV_FILE = "IPEDS_analyzed_2022_2024.csv"

def find_files():
    """Find the HTML and CSV files"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Check if files are in the same directory as this script
    html_path = os.path.join(script_dir, MAP_FILE)
    csv_path = os.path.join(script_dir, CSV_FILE)
    
    if os.path.exists(html_path) and os.path.exists(csv_path):
        return script_dir
    
    # Check common subdirectories
    possible_dirs = [
        script_dir,
        os.path.join(script_dir, "hv_master_data", "data", "IPEDS"),
        os.path.join(script_dir, "data", "IPEDS"),
        os.path.join(script_dir, "IPEDS"),
    ]
    
    for dir_path in possible_dirs:
        html_path = os.path.join(dir_path, MAP_FILE)
        csv_path = os.path.join(dir_path, CSV_FILE)
        if os.path.exists(html_path) and os.path.exists(csv_path):
            return dir_path
    
    return None

def open_browser(port):
    """Open browser after a short delay"""
    time.sleep(1.5)
    webbrowser.open(f"http://localhost:{port}/{MAP_FILE}")

def main():
    print("=" * 50)
    print("🗺️  IPEDS Institution Map Viewer")
    print("=" * 50)
    
    # Find files
    working_dir = find_files()
    
    if working_dir is None:
        print("\n❌ ERROR: Could not find required files!")
        print(f"\nMake sure these files are in the same folder as this script:")
        print(f"  - {MAP_FILE}")
        print(f"  - {CSV_FILE}")
        print(f"\nCurrent script location: {os.path.dirname(os.path.abspath(__file__))}")
        input("\nPress Enter to exit...")
        return
    
    print(f"\n✅ Found files in: {working_dir}")
    print(f"   - {MAP_FILE}")
    print(f"   - {CSV_FILE}")
    
    # Change to the directory with the files
    os.chdir(working_dir)
    
    # Start server
    handler = http.server.SimpleHTTPRequestHandler
    
    # Try to find an available port
    port = PORT
    for attempt in range(10):
        try:
            with socketserver.TCPServer(("", port), handler) as httpd:
                print(f"\n🌐 Server running at: http://localhost:{port}")
                print(f"📄 Opening: http://localhost:{port}/{MAP_FILE}")
                print(f"\n⏹️  Press Ctrl+C to stop the server\n")
                
                # Open browser in background
                threading.Thread(target=open_browser, args=(port,), daemon=True).start()
                
                # Serve forever
                httpd.serve_forever()
        except OSError as e:
            if "Address already in use" in str(e) or "Only one usage" in str(e):
                port += 1
                print(f"Port {port-1} in use, trying {port}...")
            else:
                raise
        except KeyboardInterrupt:
            print("\n\n👋 Server stopped. Goodbye!")
            break

if __name__ == "__main__":
    main()