import subprocess
import sys
import os

def run_command(command):
    subprocess.run(command, shell=True, check=True)

def main():
    # Update pip
    run_command("python -m pip install --upgrade pip")

    # Create a virtual environment
    venv_dir = "venv"
    run_command(f"python -m venv {venv_dir}")

    # Determine activation command based on OS
    if sys.platform == "win32":
        activate_cmd = os.path.join(venv_dir, "Scripts", "activate")
    else:
        activate_cmd = os.path.join(venv_dir, "bin", "activate")

    # Provide user with activation instructions
    print(f"Virtual environment created. To activate, run:\n{activate_cmd}")

if __name__ == "__main__":
    main()
