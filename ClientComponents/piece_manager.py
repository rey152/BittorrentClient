"""
Piece manager for BitTorrent client.
Handles which pieces to download, checks their integrity, and manages reading/writing pieces to disk.
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
    """Describes where each piece is in the download process."""
    MISSING = 0
    PARTIAL = 1
    COMPLETE = 2
    VERIFIED = 3


class Block:
    """Represents one block (chunk) in a piece."""
    
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
    """Represents a single piece in the torrent."""
    
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
        """Add a block of data to this piece."""
        end = offset + len(data)
        if end > self.length:
            raise ValueError(f"Block is too large for piece: {end} > {self.length}")
        
        self.data[offset:end] = data
        self.blocks_received += 1
        
    def is_complete(self):
        """Check if all blocks have been received for this piece."""
        total_blocks = sum(1 for b in self.blocks if b.received)
        return total_blocks == len(self.blocks)
    
    def verify(self):
        """Make sure the piece's data matches its hash."""
        piece_hash = hashlib.sha1(self.data).digest()
        return piece_hash == self.hash
    
    def reset(self):
        """Reset the piece back to its initial state."""
        self.state = PieceState.MISSING
        self.data = bytearray(self.length)
        self.blocks_received = 0
        for block in self.blocks:
            block.requested = False
            block.received = False
            block.data = None


class PieceManager:
    """Handles all the logic for which pieces to download and where to store them."""
    
    BLOCK_SIZE = 16384  # 16 KB per block, standard for BitTorrent
    REQUEST_TIMEOUT = 30  # Seconds before a block is considered timed out
    
    def __init__(self, torrent, download_dir):
        """
        Set up the piece manager for a given torrent and download directory.
        
        Args:
            torrent: Torrent object
            download_dir: Where to save downloaded files
        """
        self.torrent = torrent
        self.download_dir = download_dir
        
        self.pieces = {}
        self._initialize_pieces()
        
        # Track which pieces peers have
        self.piece_availability = defaultdict(int)  # piece_index: number of peers
        self.peer_pieces = defaultdict(set)  # peer_key: set of pieces
        
        self.total_downloaded = 0
        self.total_uploaded = 0
        self.bitfield = self._create_bitfield()
        
        self.file_handles = {}
        self._prepare_files()
        
        self.start_time = time.time()
        
    def _initialize_pieces(self):
        """Set up all pieces and their blocks."""
        for i in range(self.torrent.num_pieces):
            piece_length = self.torrent.get_piece_size(i)
            piece_hash = self.torrent.piece_hashes[i]
            
            piece = Piece(i, piece_length, piece_hash)
            
            # Break piece into blocks
            offset = 0
            while offset < piece_length:
                block_length = min(self.BLOCK_SIZE, piece_length - offset)
                block = Block(i, offset, block_length)
                piece.blocks.append(block)
                offset += block_length
            
            self.pieces[i] = piece
    
    def _create_bitfield(self):
        """Make a bitfield for all pieces we've got."""
        num_bytes = (self.torrent.num_pieces + 7) // 8
        bitfield = bytearray(num_bytes)
        
        # Check if any pieces already exist and are valid on disk
        for i in range(self.torrent.num_pieces):
            if self._check_piece_on_disk(i):
                self._set_piece_complete(i)
                byte_index = i // 8
                bit_index = i % 8
                bitfield[byte_index] |= (1 << (7 - bit_index))
        
        return bitfield
    
    def _prepare_files(self):
        """Make sure folders/files exist for writing pieces."""
        os.makedirs(self.download_dir, exist_ok=True)
        
        for file_info in self.torrent.files:
            file_path = os.path.join(self.download_dir, *file_info['path'])
            file_dir = os.path.dirname(file_path)
            
            if file_dir:
                os.makedirs(file_dir, exist_ok=True)
            
            if not os.path.exists(file_path):
                # Pre-allocate full file size
                with open(file_path, 'wb') as f:
                    f.seek(file_info['length'] - 1)
                    f.write(b'\0')
    
    def _check_piece_on_disk(self, piece_index):
        """See if a piece is already present and correct on disk."""
        piece = self.pieces[piece_index]
        
        try:
            piece_data = self._read_piece_from_disk(piece_index)
            if len(piece_data) != piece.length:
                return False
            
            piece_hash = hashlib.sha1(piece_data).digest()
            if piece_hash == piece.hash:
                piece.data = bytearray(piece_data)
                piece.state = PieceState.VERIFIED
                return True
                
        except Exception as e:
            logger.debug(f"Error checking piece {piece_index} on disk: {e}")
        
        return False
    
    def _read_piece_from_disk(self, piece_index):
        """Load a piece from disk."""
        piece = self.pieces[piece_index]
        piece_data = bytearray()
        
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
        """Write a verified piece back to its spot on disk."""
        piece = self.pieces[piece_index]
        file_ranges = self.torrent.get_piece_file_ranges(piece_index)
        
        for file_range in file_ranges:
            file_info = self.torrent.files[file_range['file_index']]
            file_path = os.path.join(self.download_dir, *file_info['path'])
            
            start = file_range['piece_offset']
            end = start + file_range['length']
            data = piece.data[start:end]
            
            with open(file_path, 'r+b') as f:
                f.seek(file_range['file_offset'])
                f.write(data)
        
        logger.info(f"Wrote piece {piece_index} to disk")
    
    def _set_piece_complete(self, piece_index):
        """Mark a piece as done in our bitfield."""
        byte_index = piece_index // 8
        bit_index = piece_index % 8
        self.bitfield[byte_index] |= (1 << (7 - bit_index))
    
    def update_peer_pieces(self, peer_key, bitfield=None, have_index=None):
        """Track which pieces a peer has, based on bitfield or HAVE messages."""
        if bitfield is not None:
            available_pieces = set()
            for i in range(min(len(bitfield) * 8, self.torrent.num_pieces)):
                byte_index = i // 8
                bit_index = i % 8
                if bitfield[byte_index] & (1 << (7 - bit_index)):
                    available_pieces.add(i)
            
            old_pieces = self.peer_pieces.get(peer_key, set())
            for piece in old_pieces - available_pieces:
                self.piece_availability[piece] -= 1
            for piece in available_pieces - old_pieces:
                self.piece_availability[piece] += 1
            
            self.peer_pieces[peer_key] = available_pieces
            
        elif have_index is not None:
            if have_index < self.torrent.num_pieces:
                if have_index not in self.peer_pieces[peer_key]:
                    self.peer_pieces[peer_key].add(have_index)
                    self.piece_availability[have_index] += 1
                    self.pieces[have_index].last_seen[peer_key] = time.time()
    
    def remove_peer(self, peer_key):
        """Remove a peer's pieces from our tracking."""
        if peer_key in self.peer_pieces:
            for piece_index in self.peer_pieces[peer_key]:
                self.piece_availability[piece_index] -= 1
            del self.peer_pieces[peer_key]
    
    def get_next_block(self, peer_key, peer_pieces):
        """
        Figure out which block to ask for next from a peer.
        
        Args:
            peer_key: Tuple identifying the peer
            peer_pieces: Set of pieces the peer has
            
        Returns:
            Block object if there's something to request, otherwise None
        """
        current_time = time.time()
        
        # First priority: keep working on any partial pieces
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
                    block.requested_time = current_time
                    return block
        
        # Second: rarest pieces first
        missing_pieces = [
            (self.piece_availability.get(i, 0), i)
            for i in range(self.torrent.num_pieces)
            if self.pieces[i].state == PieceState.MISSING and i in peer_pieces
        ]
        
        if missing_pieces:
            missing_pieces.sort(key=lambda x: (x[0], random.random()))
            
            for _, piece_index in missing_pieces:
                piece = self.pieces[piece_index]
                piece.state = PieceState.PARTIAL
                if piece.blocks:
                    block = piece.blocks[0]
                    block.requested = True
                    block.requested_time = current_time
                    return block
        
        # Third: endgame mode—request unreceived blocks from multiple peers
        if self.get_progress() > 0.95:  # 95% complete
            for piece in self.pieces.values():
                if piece.state == PieceState.PARTIAL and piece.index in peer_pieces:
                    for block in piece.blocks:
                        if not block.received:
                            return block
        
        return None
    
    async def add_block(self, piece_index, offset, data):
        """
        Add a block we've just received.
        
        Args:
            piece_index: Which piece
            offset: Offset within the piece
            data: Block data
            
        Returns:
            True if the piece is now complete and verified, False otherwise
        """
        if piece_index >= self.torrent.num_pieces:
            logger.error(f"Invalid piece index: {piece_index}")
            return False
        
        piece = self.pieces[piece_index]
        
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
        
        block.data = data
        block.received = True
        piece.add_block(offset, data)
        
        self.total_downloaded += len(data)
        
        if piece.is_complete():
            if piece.verify():
                piece.state = PieceState.VERIFIED
                self._write_piece_to_disk(piece_index)
                self._set_piece_complete(piece_index)
                logger.info(f"Piece {piece_index} completed and verified")
                return True
            else:
                logger.error(f"Piece {piece_index} verification failed")
                piece.reset()
                return False
        
        return False
    
    def get_block_data(self, piece_index, offset, length):
        """Get block data for uploading to another peer."""
        if piece_index >= self.torrent.num_pieces:
            return None
        
        piece = self.pieces[piece_index]
        if piece.state != PieceState.VERIFIED:
            return None
        
        if offset + length > piece.length:
            return None
        
        if not piece.data:
            piece.data = bytearray(self._read_piece_from_disk(piece_index))
        
        return bytes(piece.data[offset:offset + length])
    
    def get_progress(self):
        """How much of the torrent have we finished?"""
        verified_pieces = sum(1 for p in self.pieces.values() 
                            if p.state == PieceState.VERIFIED)
        return verified_pieces / self.torrent.num_pieces
    
    def get_download_speed(self):
        """Current download speed in bytes/sec."""
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            return self.total_downloaded / elapsed
        return 0
    
    def get_stats(self):
        """Stats for this download."""
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
        """Are all pieces verified and present?"""
        return all(p.state == PieceState.VERIFIED for p in self.pieces.values())
    
    def get_needed_pieces(self):
        """List of pieces still missing or incomplete."""
        return [i for i, p in self.pieces.items() 
                if p.state != PieceState.VERIFIED]


# Test stub
if __name__ == "__main__":
    print("PieceManager module loaded")
