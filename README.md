# chash — README

## Prerequisites
- Windows 10 (2004+) or Windows 11.
- Windows Subsystem for Linux (WSL) and Ubuntu installed.

## Install Ubuntu (Microsoft Store)
1. Open Microsoft Store.
2. Search for "Ubuntu" and click Install.
    - Or from an elevated PowerShell: `wsl --install -d Ubuntu` then restart if prompted.
3. Launch Ubuntu from Start and complete initial user setup.

## Build and run in WSL
1. Open the Ubuntu (WSL) terminal.
2. Change to the project `chash` folder on the D: drive (use quotes or escape spaces). Example:
    - cd "/mnt/d/Operating Systems/PA2/chash"
    - or: cd /mnt/d/Operating Systems/PA2/chash
3. Build:
    - make
4. Ensure the binary is executable (if needed):
    - chmod +x chash
5. Run the program and capture output:
    - ./chash commands.txt > output.txt

A `logs` folder will be created automatically when the program runs.

## Notes / Troubleshooting
- Use LF line endings for `commands.txt` (not CRLF) to avoid parsing issues.
- If WSL cannot access the D: drive, enable drive mounting in your WSL settings or confirm path under `/mnt/d`.
- If `make` is missing, install build tools: `sudo apt update && sudo apt install build-essential`.
