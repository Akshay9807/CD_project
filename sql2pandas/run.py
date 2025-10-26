#!/usr/bin/env python3
"""
Launch script for SQL2Pandas Compiler
"""

import subprocess
import sys
import os

def main():
    """Launch the Streamlit application"""
    try:
        # Change to the script directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(script_dir)
        
        print("Starting SQL2Pandas Compiler...")
        print("This will open your web browser automatically.")
        print("If it doesn't, navigate to: http://localhost:8501")
        print("\nPress Ctrl+C to stop the server.\n")
        
        # Run streamlit
        subprocess.run([sys.executable, "-m", "streamlit", "run", "main.py"], check=True)
        
    except KeyboardInterrupt:
        print("\n\nShutting down SQL2Pandas Compiler. Goodbye!")
    except subprocess.CalledProcessError as e:
        print(f"Error starting Streamlit: {e}")
        print("Make sure you have installed the requirements:")
        print("pip install -r requirements.txt")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()