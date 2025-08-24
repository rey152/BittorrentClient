"""
Torrent file parser for BitTorrent client.
This module takes care of reading .torrent files and pulling out all the important details we need to download and share files.
"""

import hashlib
import os
from datetime import datetime
from urllib.parse import quote

from bencode import decode, encode


class TorrentError(Exception):
    """Any general problem with parsing or handling a torrent file."""
    pass


class Torrent:
    """Represents a parsed torrent file and all its metadata."""
    
    def __init__(self, torrent_data):
        """
        Load a torrent file and grab its metadata.
        
        Args:
            torrent_data: Raw bytes of torrent file or a path to the file.
        """
        if isinstance(torrent_data, str):
            # User passed a file path
            with open(torrent_data, 'rb') as f:
                torrent_data = f.read()
        
        self.raw_data = torrent_data
        self.meta_info = decode(torrent_data)
        
        if not isinstance(self.meta_info, dict):
            raise TorrentError("Invalid torrent file: root should be a dictionary")
        
        if 'info' not in self.meta_info:
            raise TorrentError("Torrent file missing 'info' dictionary")
        
        self.info = self.meta_info['info']
        self._calculate_info_hash()
        self._parse_metadata()
    
    def _calculate_info_hash(self):
        """Get the SHA1 hash for the info dictionary (used in peer/tracker communication)."""
        info_encoded = encode(self.info)
        self.info_hash = hashlib.sha1(info_encoded).digest()
        self.info_hash_hex = self.info_hash.hex()
    
    def _parse_metadata(self):
        """Read all the metadata fields from the torrent."""
        self.name = self.info.get('name', b'').decode('utf-8', errors='replace')
        self.piece_length = self.info.get('piece length', 0)
        
        if 'pieces' not in self.info:
            raise TorrentError("Torrent missing piece hashes")
        
        # Split the big piece hash string into actual 20-byte hashes
        pieces_data = self.info['pieces']
        if len(pieces_data) % 20 != 0:
            raise TorrentError("Piece hash section is the wrong size")
        
        self.piece_hashes = []
        for i in range(0, len(pieces_data), 20):
            self.piece_hashes.append(pieces_data[i:i+20])
        
        self.num_pieces = len(self.piece_hashes)
        
        # Single-file or multi-file torrent?
        if 'length' in self.info:
            self.is_multi_file = False
            self.total_size = self.info['length']
            self.files = [{
                'path': [self.name],
                'length': self.total_size,
                'offset': 0
            }]
        else:
            # Handle multi-file torrents
            self.is_multi_file = True
            self.files = []
            offset = 0
            
            if 'files' not in self.info:
                raise TorrentError("Multi-file torrent missing 'files' list")
            
            for file_info in self.info['files']:
                if 'length' not in file_info or 'path' not in file_info:
                    raise TorrentError("Bad file entry in torrent")
                
                path_parts = []
                for part in file_info['path']:
                    if isinstance(part, bytes):
                        path_parts.append(part.decode('utf-8', errors='replace'))
                    else:
                        path_parts.append(str(part))
                
                self.files.append({
                    'path': [self.name] + path_parts,
                    'length': file_info['length'],
                    'offset': offset
                })
                offset += file_info['length']
            
            self.total_size = offset
        
        # Tracker info
        self.announce = self.meta_info.get('announce', b'').decode('utf-8', errors='replace')
        self.announce_list = []
        
        if 'announce-list' in self.meta_info:
            for tier in self.meta_info['announce-list']:
                tier_list = []
                for tracker in tier:
                    if isinstance(tracker, bytes):
                        tier_list.append(tracker.decode('utf-8', errors='replace'))
                    else:
                        tier_list.append(str(tracker))
                if tier_list:
                    self.announce_list.append(tier_list)
        elif self.announce:
            self.announce_list = [[self.announce]]
        
        # Optional metadata
        self.comment = self._decode_optional_string('comment')
        self.created_by = self._decode_optional_string('created by')
        self.creation_date = None
        
        if 'creation date' in self.meta_info:
            try:
                self.creation_date = datetime.fromtimestamp(self.meta_info['creation date'])
            except (ValueError, TypeError):
                pass
        
        # Private flag
        self.private = bool(self.info.get('private', 0))
        
        # Figure out size of the last piece
        self.last_piece_size = self.total_size % self.piece_length
        if self.last_piece_size == 0:
            self.last_piece_size = self.piece_length
    
    def _decode_optional_string(self, key):
        """Try to decode a string field from the torrent, if present."""
        value = self.meta_info.get(key, b'')
        if isinstance(value, bytes):
            return value.decode('utf-8', errors='replace')
        return str(value) if value else ''
    
    def get_piece_size(self, piece_index):
        """Return the size of a particular piece."""
        if piece_index < 0 or piece_index >= self.num_pieces:
            raise ValueError(f"Invalid piece index: {piece_index}")
        
        if piece_index == self.num_pieces - 1:
            return self.last_piece_size
        return self.piece_length
    
    def get_file_piece_range(self, file_index):
        """Figure out which pieces contain the given file."""
        if file_index < 0 or file_index >= len(self.files):
            raise ValueError(f"Invalid file index: {file_index}")
        
        file_info = self.files[file_index]
        start_byte = file_info['offset']
        end_byte = start_byte + file_info['length'] - 1
        
        start_piece = start_byte // self.piece_length
        end_piece = end_byte // self.piece_length
        
        return start_piece, end_piece
    
    def get_piece_file_ranges(self, piece_index):
        """List the files and byte ranges that a given piece covers."""
        if piece_index < 0 or piece_index >= self.num_pieces:
            raise ValueError(f"Invalid piece index: {piece_index}")
        
        piece_start = piece_index * self.piece_length
        piece_size = self.get_piece_size(piece_index)
        piece_end = piece_start + piece_size - 1
        
        file_ranges = []
        
        for i, file_info in enumerate(self.files):
            file_start = file_info['offset']
            file_end = file_start + file_info['length'] - 1
            
            # Does this file touch our piece?
            if file_start <= piece_end and file_end >= piece_start:
                # Calculate where they overlap
                overlap_start = max(piece_start, file_start)
                overlap_end = min(piece_end, file_end)
                
                # Figure out offsets relative to file and piece
                file_offset = overlap_start - file_start
                length = overlap_end - overlap_start + 1
                
                file_ranges.append({
                    'file_index': i,
                    'file_offset': file_offset,
                    'piece_offset': overlap_start - piece_start,
                    'length': length
                })
        
        return file_ranges
    
    def get_magnet_link(self):
        """Build a magnet link for this torrent."""
        params = []
        params.append(f"xt=urn:btih:{self.info_hash_hex}")
        
        if self.name:
            params.append(f"dn={quote(self.name)}")
        
        for tier in self.announce_list:
            for tracker in tier:
                params.append(f"tr={quote(tracker)}")
        
        if 'url-list' in self.meta_info:
            for url in self.meta_info['url-list']:
                if isinstance(url, bytes):
                    url = url.decode('utf-8', errors='replace')
                params.append(f"ws={quote(url)}")
        
        return "magnet:?" + "&".join(params)
    
    def __str__(self):
        """Show a summary of the torrent."""
        files_str = f"{len(self.files)} files" if self.is_multi_file else "single file"
        return (f"Torrent: {self.name}\n"
                f"Info Hash: {self.info_hash_hex}\n"
                f"Total Size: {self.total_size:,} bytes\n"
                f"Pieces: {self.num_pieces} x {self.piece_length:,} bytes\n"
                f"Files: {files_str}\n"
                f"Private: {self.private}")
    
    def save(self, path):
        """Write the torrent file to disk."""
        with open(path, 'wb') as f:
            f.write(self.raw_data)


def create_torrent(path, piece_length=524288, private=False, trackers=None, comment=None):
    """
    Make a .torrent file from a folder or file.
    
    Args:
        path: Path to the file or directory
        piece_length: Each piece size in bytes (default 512KB)
        private: Set to True for a private torrent
        trackers: List of tracker URLs (optionally list of lists for tiers)
        comment: Optional comment for the torrent
        
    Returns:
        Torrent object with all metadata ready
    """
    if not os.path.exists(path):
        raise ValueError(f"Path does not exist: {path}")
    
    files = []
    total_size = 0
    
    if os.path.isfile(path):
        # Just one file
        name = os.path.basename(path)
        size = os.path.getsize(path)
        files.append((path, [], size))
        total_size = size
    else:
        # It's a directory
        name = os.path.basename(os.path.abspath(path))
        for root, dirs, filenames in os.walk(path):
            dirs.sort()
            filenames.sort()
            
            for filename in filenames:
                filepath = os.path.join(root, filename)
                relpath = os.path.relpath(filepath, path)
                path_parts = relpath.split(os.sep)
                
                size = os.path.getsize(filepath)
                files.append((filepath, path_parts, size))
                total_size += size
    
    if not files:
        raise ValueError("No files found to create torrent")
    
    # Hash up all the file data into pieces
    num_pieces = (total_size + piece_length - 1) // piece_length
    pieces = b''
    
    piece_buffer = b''
    for filepath, _, _ in files:
        with open(filepath, 'rb') as f:
            while True:
                needed = piece_length - len(piece_buffer)
                data = f.read(needed)
                if not data:
                    break
                
                piece_buffer += data
                
                if len(piece_buffer) == piece_length:
                    pieces += hashlib.sha1(piece_buffer).digest()
                    piece_buffer = b''
    
    if piece_buffer:
        pieces += hashlib.sha1(piece_buffer).digest()
    
    # Build info dictionary for the torrent
    info = {
        'name': name.encode('utf-8'),
        'piece length': piece_length,
        'pieces': pieces
    }
    
    if private:
        info['private'] = 1
    
    if len(files) == 1:
        info['length'] = files[0][2]
    else:
        info['files'] = []
        for _, path_parts, size in files:
            info['files'].append({
                'path': [part.encode('utf-8') for part in path_parts],
                'length': size
            })
    
    meta_info = {'info': info}
    
    if trackers:
        if isinstance(trackers[0], list):
            meta_info['announce-list'] = [[t.encode('utf-8') for t in tier] for tier in trackers]
            if trackers[0]:
                meta_info['announce'] = trackers[0][0].encode('utf-8')
        else:
            meta_info['announce'] = trackers[0].encode('utf-8')
            meta_info['announce-list'] = [[t.encode('utf-8') for t in trackers]]
    
    if comment:
        meta_info['comment'] = comment.encode('utf-8')
    
    meta_info['created by'] = b'SimpleBittorrentClient/1.0'
    meta_info['creation date'] = int(datetime.now().timestamp())
    
    torrent_data = encode(meta_info)
    return Torrent(torrent_data)


# Quick test/demo
if __name__ == "__main__":
    test_info = {
        'name': b'test.txt',
        'piece length': 16384,
        'length': 1024,
        'pieces': hashlib.sha1(b'test').digest()
    }
    
    test_torrent = {
        'info': test_info,
        'announce': b'http://tracker.example.com/announce',
        'creation date': int(datetime.now().timestamp()),
        'comment': b'Test torrent',
        'created by': b'SimpleBittorrentClient/1.0'
    }
    
    from bencode import encode
    torrent_data = encode(test_torrent)
    torrent = Torrent(torrent_data)
    print(torrent)
    print(f"\nMagnet link: {torrent.get_magnet_link()}")
