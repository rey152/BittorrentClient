"""
Piece manager for BitTorrent client.
Handles piece selection, verification, storage, and block management.
"""

import os
import hashlib
import asyncio
import logging
from collections import defaultdict
from enum import Enum
import time
import random

logger = logging.getLogger(__name__)


class PieceState(Enum):
    """State of a piece in the download process."""
    MISSING = 0
    PARTIAL = 1
    COMPLETE = 2
    VERIFIED = 3


class Block:
    """Represents a block within a piece."""
    
    def __init__(self, piece_index, offset, length):
        self.piece_index = piece_index
        self.offset = offset
        self.length = length
        self.data = None
        self.requested = False
        self.requested_time = None
        self.received = False
    
    def __repr__(self):
        return f"Block(piece={self.piece_index}, offset={self.offset}, length={self.length})"


class Piece:
    """Represents a piece in the torrent."""
    
    def __init__(self, index, length, hash_value):
        self.index = index
        self.length = length
        self.hash = hash_value
        self.blocks = []
        self.state = PieceState.MISSING
        self.data = bytearray(length)
        self.blocks_received = 0
        self.last_seen = {}  # {peer_key: timestamp}
        
    def add_block(self, offset, data):
        """Add block data to the piece."""
        end = offset + len(data)
        if end > self.length:
            raise ValueError(f"Block exceeds piece boundary: {end} > {self.length}")
        
        self.data[offset:end] = data
        self.blocks_received += 1
        
    def is_complete(self):
        """Check if all blocks have been received."""
        total_blocks = sum(1 for b in self.blocks if b.received)
        return total_blocks == len(self.blocks)
    
    def verify(self):
        """Verify piece hash."""
        piece_hash = hashlib.sha1(self.data).digest()
        return piece_hash == self.hash
    
    def reset(self):
        """Reset piece to initial state."""
        self.state = PieceState.MISSING
        self.data = bytearray(self.length)
        self.blocks_received = 0
        for block in self.blocks:
            block.requested = False
            block.received = False
            block.data = None


class PieceManager:
    """Manages piece downloading and storage."""
    
    BLOCK_SIZE = 16384  # 16 KB standard block size
    REQUEST_TIMEOUT = 30  # Seconds before re-requesting a block
    
    def __init__(self, torrent, download_dir):
        """
        Initialize piece manager.
        
        Args:
            torrent: Torrent object
            download_dir: Directory to save downloaded files
        """
        self.torrent = torrent
        self.download_dir = download_dir
        
        # Initialize pieces
        self.pieces = {}
        self._initialize_pieces()
        
        # Piece availability tracking
        self.piece_availability = defaultdict(int)  # piece_index: peer_count
        self.peer_pieces = defaultdict(set)  # peer_key: set of piece indices
        
        # Download state
        self.total_downloaded = 0
        self.total_uploaded = 0
        self.bitfield = self._create_bitfield()
        
        # File handles
        self.file_handles = {}
        self._prepare_files()
        
        # Statistics
        self.start_time = time.time()
        
    def _initialize_pieces(self):
        """Initialize all pieces with their blocks."""
        for i in range(self.torrent.num_pieces):
            piece_length = self.torrent.get_piece_size(i)
            piece_hash = self.torrent.piece_hashes[i]
            
            piece = Piece(i, piece_length, piece_hash)
            
            # Create blocks for this piece
            offset = 0
            while offset < piece_length:
                block_length = min(self.BLOCK_SIZE, piece_length - offset)
                block = Block(i, offset, block_length)
                piece.blocks.append(block)
                offset += block_length
            
            self.pieces[i] = piece
    
    def _create_bitfield(self):
        """Create bitfield for pieces we have."""
        num_bytes = (self.torrent.num_pieces + 7) // 8
        bitfield = bytearray(num_bytes)
        
        # Check existing files for complete pieces
        for i in range(self.torrent.num_pieces):
            if self._check_piece_on_disk(i):
                self._set_piece_complete(i)
                byte_index = i // 8
                bit_index = i % 8
                bitfield[byte_index] |= (1 << (7 - bit_index))
        
        return bitfield
    
    def _prepare_files(self):
        """Prepare files for writing."""
        os.makedirs(self.download_dir, exist_ok=True)
        
        for file_info in self.torrent.files:
            file_path = os.path.join(self.download_dir, *file_info['path'])
            file_dir = os.path.dirname(file_path)
            
            # Create directory if needed
            if file_dir:
                os.makedirs(file_dir, exist_ok=True)
            
            # Create/open file
            if not os.path.exists(file_path):
                # Pre-allocate file space
                with open(file_path, 'wb') as f:
                    f.seek(file_info['length'] - 1)
                    f.write(b'\0')
    
    def _check_piece_on_disk(self, piece_index):
        """Check if a piece exists and is valid on disk."""
        piece = self.pieces[piece_index]
        
        try:
            # Read piece data from file(s)
            piece_data = self._read_piece_from_disk(piece_index)
            if len(piece_data) != piece.length:
                return False
            
            # Verify hash
            piece_hash = hashlib.sha1(piece_data).digest()
            if piece_hash == piece.hash:
                piece.data = bytearray(piece_data)
                piece.state = PieceState.VERIFIED
                return True
                
        except Exception as e:
            logger.debug(f"Error checking piece {piece_index} on disk: {e}")
        
        return False
    
    def _read_piece_from_disk(self, piece_index):
        """Read a piece from disk."""
        piece = self.pieces[piece_index]
        piece_data = bytearray()
        
        # Get file ranges for this piece
        file_ranges = self.torrent.get_piece_file_ranges(piece_index)
        
        for file_range in file_ranges:
            file_info = self.torrent.files[file_range['file_index']]
            file_path = os.path.join(self.download_dir, *file_info['path'])
            
            with open(file_path, 'rb') as f:
                f.seek(file_range['file_offset'])
                data = f.read(file_range['length'])
                piece_data.extend(data)
        
        return bytes(piece_data)
    
    def _write_piece_to_disk(self, piece_index):
        """Write a verified piece to disk."""
        piece = self.pieces[piece_index]
        
        # Get file ranges for this piece
        file_ranges = self.torrent.get_piece_file_ranges(piece_index)
        
        for file_range in file_ranges:
            file_info = self.torrent.files[file_range['file_index']]
            file_path = os.path.join(self.download_dir, *file_info['path'])
            
            # Extract data for this file
            start = file_range['piece_offset']
            end = start + file_range['length']
            data = piece.data[start:end]
            
            # Write to file
            with open(file_path, 'r+b') as f:
                f.seek(file_range['file_offset'])
                f.write(data)
        
        logger.info(f"Wrote piece {piece_index} to disk")
    
    def _set_piece_complete(self, piece_index):
        """Mark a piece as complete in the bitfield."""
        byte_index = piece_index // 8
        bit_index = piece_index % 8
        self.bitfield[byte_index] |= (1 << (7 - bit_index))
    
    def update_peer_pieces(self, peer_key, bitfield=None, have_index=None):
        """Update piece availability from peer."""
        if bitfield is not None:
            # Process bitfield
            available_pieces = set()
            for i in range(min(len(bitfield) * 8, self.torrent.num_pieces)):
                byte_index = i // 8
                bit_index = i % 8
                if bitfield[byte_index] & (1 << (7 - bit_index)):
                    available_pieces.add(i)
            
            # Update availability counts
            old_pieces = self.peer_pieces.get(peer_key, set())
            for piece in old_pieces - available_pieces:
                self.piece_availability[piece] -= 1
            for piece in available_pieces - old_pieces:
                self.piece_availability[piece] += 1
            
            self.peer_pieces[peer_key] = available_pieces
            
        elif have_index is not None:
            # Process single have message
            if have_index < self.torrent.num_pieces:
                if have_index not in self.peer_pieces[peer_key]:
                    self.peer_pieces[peer_key].add(have_index)
                    self.piece_availability[have_index] += 1
                    self.pieces[have_index].last_seen[peer_key] = time.time()
    
    def remove_peer(self, peer_key):
        """Remove a peer and update availability."""
        if peer_key in self.peer_pieces:
            for piece_index in self.peer_pieces[peer_key]:
                self.piece_availability[piece_index] -= 1
            del self.peer_pieces[peer_key]
    
    def get_next_block(self, peer_key, peer_pieces):
        """
        Get the next block to request from a peer.
        
        Args:
            peer_key: (ip, port) tuple identifying the peer
            peer_pieces: Set of piece indices the peer has
            
        Returns:
            Block object or None if no suitable block found
        """
        current_time = time.time()
        
        # Priority 1: Continue partially downloaded pieces
        incomplete_pieces = [
            p for p in self.pieces.values()
            if p.state == PieceState.PARTIAL and p.index in peer_pieces
        ]
        
        for piece in incomplete_pieces:
            for block in piece.blocks:
                if not block.received and not block.requested:
                    block.requested = True
                    block.requested_time = current_time
                    return block
                elif (block.requested and not block.received and 
                      current_time - block.requested_time > self.REQUEST_TIMEOUT):
                    # Re-request timed out block
                    block.requested_time = current_time
                    return block
        
        # Priority 2: Rarest pieces first
        missing_pieces = [
            (self.piece_availability.get(i, 0), i)
            for i in range(self.torrent.num_pieces)
            if self.pieces[i].state == PieceState.MISSING and i in peer_pieces
        ]
        
        if missing_pieces:
            # Sort by availability (rarest first), then randomize among equally rare
            missing_pieces.sort(key=lambda x: (x[0], random.random()))
            
            for _, piece_index in missing_pieces:
                piece = self.pieces[piece_index]
                piece.state = PieceState.PARTIAL
                
                # Request first block
                if piece.blocks:
                    block = piece.blocks[0]
                    block.requested = True
                    block.requested_time = current_time
                    return block
        
        # Priority 3: Endgame mode - request unreceived blocks from multiple peers
        if self.get_progress() > 0.95:  # 95% complete
            for piece in self.pieces.values():
                if piece.state == PieceState.PARTIAL and piece.index in peer_pieces:
                    for block in piece.blocks:
                        if not block.received:
                            return block
        
        return None
    
    async def add_block(self, piece_index, offset, data):
        """
        Add a received block to a piece.
        
        Args:
            piece_index: Index of the piece
            offset: Offset within the piece
            data: Block data
            
        Returns:
            True if piece is now complete and verified
        """
        if piece_index >= self.torrent.num_pieces:
            logger.error(f"Invalid piece index: {piece_index}")
            return False
        
        piece = self.pieces[piece_index]
        
        # Find the corresponding block
        block = None
        for b in piece.blocks:
            if b.offset == offset and b.length == len(data):
                block = b
                break
        
        if not block:
            logger.error(f"No matching block for piece {piece_index}, offset {offset}")
            return False
        
        if block.received:
            logger.debug(f"Duplicate block received: piece {piece_index}, offset {offset}")
            return False
        
        # Store block data
        block.data = data
        block.received = True
        piece.add_block(offset, data)
        
        self.total_downloaded += len(data)
        
        # Check if piece is complete
        if piece.is_complete():
            if piece.verify():
                piece.state = PieceState.VERIFIED
                self._write_piece_to_disk(piece_index)
                self._set_piece_complete(piece_index)
                logger.info(f"Piece {piece_index} completed and verified")
                return True
            else:
                # Verification failed, reset piece
                logger.error(f"Piece {piece_index} verification failed")
                piece.reset()
                return False
        
        return False
    
    def get_block_data(self, piece_index, offset, length):
        """Get block data for uploading to peers."""
        if piece_index >= self.torrent.num_pieces:
            return None
        
        piece = self.pieces[piece_index]
        if piece.state != PieceState.VERIFIED:
            return None
        
        if offset + length > piece.length:
            return None
        
        # Read from disk if piece data not in memory
        if not piece.data:
            piece.data = bytearray(self._read_piece_from_disk(piece_index))
        
        return bytes(piece.data[offset:offset + length])
    
    def get_progress(self):
        """Get download progress as a fraction."""
        verified_pieces = sum(1 for p in self.pieces.values() 
                            if p.state == PieceState.VERIFIED)
        return verified_pieces / self.torrent.num_pieces
    
    def get_download_speed(self):
        """Get current download speed in bytes per second."""
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            return self.total_downloaded / elapsed
        return 0
    
    def get_stats(self):
        """Get download statistics."""
        verified_pieces = sum(1 for p in self.pieces.values() 
                            if p.state == PieceState.VERIFIED)
        partial_pieces = sum(1 for p in self.pieces.values() 
                           if p.state == PieceState.PARTIAL)
        
        return {
            'progress': self.get_progress(),
            'downloaded': self.total_downloaded,
            'uploaded': self.total_uploaded,
            'pieces_complete': verified_pieces,
            'pieces_partial': partial_pieces,
            'pieces_total': self.torrent.num_pieces,
            'download_speed': self.get_download_speed(),
        }
    
    def is_complete(self):
        """Check if download is complete."""
        return all(p.state == PieceState.VERIFIED for p in self.pieces.values())
    
    def get_needed_pieces(self):
        """Get list of piece indices we still need."""
        return [i for i, p in self.pieces.items() 
                if p.state != PieceState.VERIFIED]


# Testing
if __name__ == "__main__":
    # This would require a real torrent object
    print("PieceManager module loaded")
