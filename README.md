# BittorrentClient

A simple BitTorrent client written in Python 3.11+ using asyncio.  
This is a learning project for understanding the BitTorrent protocol and async programming in Python.  
It can download torrents and has basic features.

---

## Requirements

- Python 3.11 or newer
- Install dependencies:
  ```bash
  pip install requests
  ```

---

## How to Use

1. **Clone the repo:**
   ```bash
   git clone https://github.com/rey152/BittorrentClient.git
   cd BittorrentClient/ClientComponents
   ```

2. **Run the client:**

   - **Download a torrent:**
     ```bash
     python cli.py download yourfile.torrent
     ```

   - **Create a torrent file:**
     ```bash
     python cli.py create /path/to/your/files
     ```

   - **Get info about a torrent:**
     ```bash
     python cli.py info yourfile.torrent
     ```

---

## Notes

- This is mostly for testing, learning, and fun.
- Advanced BitTorrent features may not be supported.
- For troubleshooting, use the `-v` (verbose) flag.

---

Enjoy!
