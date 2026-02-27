#!/usr/bin/env python3
"""
Auto-Clicker for VS Code Terminal + Browser Workflow
=====================================================
Tailored for the acreage scraper workflow:
1. Browser opens and takes full screen
2. Need to move browser aside and click VS Code terminal
3. Press Enter every 40 seconds

This version:
- Gives you time to arrange windows initially
- Clicks on VS Code terminal to focus it
- Presses Enter
- Repeats every 40 seconds

Requirements:
    pip install pyautogui

Usage:
    python auto_clicker.py --find-position    # First: find where to click
    python auto_clicker.py --click X Y        # Then: run with those coordinates
"""

import argparse
import time
import sys

try:
    import pyautogui
except ImportError:
    print("=" * 50)
    print("ERROR: pyautogui not installed")
    print("=" * 50)
    print("\nRun this command to install it:")
    print("    pip install pyautogui")
    print()
    sys.exit(1)


def find_terminal_position():
    """Helper to find where your VS Code terminal is."""
    print("\n" + "=" * 50)
    print("  POSITION FINDER")
    print("=" * 50)
    print("""
INSTRUCTIONS:
1. Arrange your windows how you want them:
   - Browser on the LEFT
   - VS Code on the RIGHT (with terminal visible)
   
2. Move your mouse to the VS Code TERMINAL area
   (where you would click to type)

3. Once your mouse is in position, press Ctrl+C

The coordinates will be displayed.
""")
    print("-" * 50)
    
    try:
        while True:
            x, y = pyautogui.position()
            print(f"\r  Mouse position: ({x}, {y})    ", end="", flush=True)
            time.sleep(0.1)
    except KeyboardInterrupt:
        x, y = pyautogui.position()
        print(f"\n\n" + "=" * 50)
        print(f"  SAVED POSITION: ({x}, {y})")
        print("=" * 50)
        print(f"""
Now run the auto-clicker with:

    python auto_clicker.py --click {x} {y}

This will:
- Wait 30 seconds for you to start the scraper
- Click at ({x}, {y}) to focus VS Code terminal  
- Press Enter
- Repeat every 40 seconds
""")


def countdown(seconds: int, message: str = "Starting in"):
    """Display countdown."""
    print(f"\n{message} {seconds} seconds...")
    print("(Press Ctrl+C to cancel)\n")
    for i in range(seconds, 0, -1):
        print(f"  {i}...", end=" ", flush=True)
        if i % 10 == 0 and i != seconds:
            print()  # New line every 10 seconds
        time.sleep(1)
    print("\nGO!\n")


def run_automation(click_x: int, click_y: int, interval: float):
    """
    Main automation loop.
    Clicks at (click_x, click_y) then presses Enter, every [interval] seconds.
    """
    pyautogui.FAILSAFE = True  # Move mouse to corner to abort
    pyautogui.PAUSE = 0.1
    
    action_count = 0
    
    print("=" * 50)
    print("  AUTO-CLICKER RUNNING")
    print("=" * 50)
    print(f"  Click position: ({click_x}, {click_y})")
    print(f"  Interval: {interval} seconds")
    print()
    print("  To STOP: Move mouse to top-left corner")
    print("           or press Ctrl+C")
    print("=" * 50)
    print()
    
    try:
        while True:
            # Wait for the interval
            time.sleep(interval)
            
            action_count += 1
            timestamp = time.strftime("%H:%M:%S")
            
            # Click to focus VS Code terminal
            pyautogui.click(click_x, click_y)
            time.sleep(0.3)  # Brief pause after click
            
            # Press Enter
            pyautogui.press('enter')
            
            print(f"[{timestamp}] #{action_count}: Clicked ({click_x}, {click_y}) + Enter")
                
    except KeyboardInterrupt:
        print(f"\n\nStopped by user after {action_count} actions.")
    except pyautogui.FailSafeException:
        print(f"\n\nStopped (mouse moved to corner) after {action_count} actions.")


def main():
    parser = argparse.ArgumentParser(
        description="Auto-clicker for VS Code terminal workflow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
FIRST TIME SETUP:
    1. Run: python auto_clicker.py --find-position
    2. Arrange your windows (browser left, VS Code right)
    3. Move mouse to VS Code terminal, press Ctrl+C
    4. Note the coordinates it gives you

RUNNING:
    python auto_clicker.py --click 1200 800

    (Use the coordinates from step 3)
        """
    )
    
    parser.add_argument('--click', '-c', type=int, nargs=2, metavar=('X', 'Y'),
                        help='Screen coordinates to click (VS Code terminal location)')
    parser.add_argument('--interval', '-i', type=float, default=40.0,
                        help='Seconds between actions (default: 40)')
    parser.add_argument('--delay', '-d', type=int, default=30,
                        help='Initial delay before starting (default: 30 seconds)')
    parser.add_argument('--find-position', '-f', action='store_true',
                        help='Helper to find click coordinates')
    
    args = parser.parse_args()
    
    # Mode 1: Find position helper
    if args.find_position:
        find_terminal_position()
        return
    
    # Mode 2: Run automation (requires click position)
    if not args.click:
        print("\n" + "=" * 50)
        print("  SETUP REQUIRED")
        print("=" * 50)
        print("""
You need to tell me where to click!

STEP 1 - Find your VS Code terminal position:
    python auto_clicker.py --find-position

STEP 2 - Run with those coordinates:
    python auto_clicker.py --click X Y
    
Example:
    python auto_clicker.py --click 1200 800
""")
        return
    
    click_x, click_y = args.click
    
    print("\n" + "=" * 50)
    print("  ACREAGE SCRAPER AUTO-CLICKER")
    print("=" * 50)
    print(f"""
Settings:
  - Click position: ({click_x}, {click_y})
  - Interval: {args.interval} seconds
  - Initial delay: {args.delay} seconds

INSTRUCTIONS:
  1. Start your scraper in VS Code terminal NOW
  2. When browser opens, drag it to the LEFT side
  3. Make sure VS Code terminal is visible on the RIGHT
  4. The auto-clicker will take over after the countdown
""")
    
    # Initial delay to let user start scraper and arrange windows
    countdown(args.delay, "Auto-clicking begins in")
    
    # Run the automation
    run_automation(click_x, click_y, args.interval)


if __name__ == "__main__":
    main()