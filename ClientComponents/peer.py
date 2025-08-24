"""
Peer protocol handler for BitTorrent client.
Implements the BitTorrent peer wire protocol for communication with peers.
"""

import asyncio
import struct
import time
import logging
from enum import IntEnum
from collections import deque
import hashlib

logger = logging.getLogger(__name__)


class MessageType(IntEnum):
    """BitTorrent peer message types."""
    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7
    CANCEL = 8
    PORT = 9  # DHT extension
    EXTENDED = 20  # Extension protocol


class PeerError(Exception):
    """Base exception for peer-related errors."""
    pass


class PeerConnection:
    """Handles communication with a single peer."""
    
    PROTOCOL_STRING = b'BitTorrent protocol'
    HANDSHAKE_LENGTH = 68  # 1 + 19 + 8 + 20 + 20
    
    def __init__(self, info_hash, peer_id, ip, port):
        """
        Initialize peer connection.
        
        Args:
            info_hash: 20-byte torrent info hash
            peer_id: Our 20-byte peer ID
            ip: Peer IP address
            port: Peer port
        """
        self.info_hash = info_hash
        self.our_peer_id = peer_id
        self.ip = ip
        self.port = port
        
        # Connection state
        self.reader = None
        self.writer = None
        self.connected = False
        self.handshake_complete = False
        
        # Peer info
        self.peer_id = None
        self.extensions = bytes(8)  # 8 reserved bytes from handshake
        
        # Peer state
        self.am_choking = True
        self.am_interested = False
        self.peer_choking = True
        self.peer_interested = False
        
        # Piece availability
        self.bitfield = None
        self.num_pieces = 0
        
        # Request queue
        self.pending_requests = deque()  # (piece_index, begin, length)
        self.active_requests = set()  # Currently being downloaded
        
        # Statistics
        self.downloaded = 0
        self.uploaded = 0
        self.last_message_time = 0
        
        # Callbacks
        self.on_bitfield = None
        self.on_have = None
        self.on_piece = None
        self.on_request = None
        self.on_cancel = None
    
    async def connect(self, timeout=10):
        """Establish connection to peer."""
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.ip, self.port),
                timeout=timeout
            )
            self.connected = True
            logger.info(f"Connected to peer {self.ip}:{self.port}")
            return True
        except (asyncio.TimeoutError, ConnectionError) as e:
            logger.error(f"Failed to connect to {self.ip}:{self.port}: {e}")
            return False
    
    async def handshake(self):
        """Perform BitTorrent handshake."""
        if not self.connected:
            raise PeerError("Not connected")
        
        # Build handshake message
        handshake = (
            bytes([len(self.PROTOCOL_STRING)]) +
            self.PROTOCOL_STRING +
            bytes(8) +  # Reserved bytes (all zero for now)
            self.info_hash +
            self.our_peer_id
        )
        
        # Send handshake
        self.writer.write(handshake)
        await self.writer.drain()
        
        # Receive handshake
        try:
            response = await asyncio.wait_for(
                self.reader.readexactly(self.HANDSHAKE_LENGTH),
                timeout=10
            )
        except asyncio.TimeoutError:
            raise PeerError("Handshake timeout")
        except asyncio.IncompleteReadError:
            raise PeerError("Incomplete handshake")
        
        # Parse handshake
        pstr_len = response[0]
        if pstr_len != len(self.PROTOCOL_STRING):
            raise PeerError("Invalid protocol string length")
        
        pstr = response[1:20]
        if pstr != self.PROTOCOL_STRING:
            raise PeerError("Invalid protocol string")
        
        self.extensions = response[20:28]
        peer_info_hash = response[28:48]
        self.peer_id = response[48:68]
        
        # Verify info hash
        if peer_info_hash != self.info_hash:
            raise PeerError("Info hash mismatch")
        
        self.handshake_complete = True
        self.last_message_time = time.time()
        logger.info(f"Completed handshake with {self.ip}:{self.port}")
        
        return True
    
    async def send_message(self, message_type, payload=b''):
        """Send a message to the peer."""
        if not self.connected:
            raise PeerError("Not connected")
        
        # Build message: <length><type><payload>
        message = struct.pack('!IB', len(payload) + 1, message_type) + payload
        
        self.writer.write(message)
        await self.writer.drain()
        
        logger.debug(f"Sent {MessageType(message_type).name} to {self.ip}:{self.port}")
    
    async def send_keepalive(self):
        """Send keepalive message (0-length message)."""
        if not self.connected:
            return
        
        self.writer.write(struct.pack('!I', 0))
        await self.writer.drain()
    
    async def send_choke(self):
        """Tell peer we're choking them."""
        await self.send_message(MessageType.CHOKE)
        self.am_choking = True
    
    async def send_unchoke(self):
        """Tell peer we're not choking them."""
        await self.send_message(MessageType.UNCHOKE)
        self.am_choking = False
    
    async def send_interested(self):
        """Tell peer we're interested."""
        await self.send_message(MessageType.INTERESTED)
        self.am_interested = True
    
    async def send_not_interested(self):
        """Tell peer we're not interested."""
        await self.send_message(MessageType.NOT_INTERESTED)
        self.am_interested = False
    
    async def send_have(self, piece_index):
        """Tell peer we have a piece."""
        payload = struct.pack('!I', piece_index)
        await self.send_message(MessageType.HAVE, payload)
    
    async def send_bitfield(self, bitfield):
        """Send our bitfield to peer."""
        await self.send_message(MessageType.BITFIELD, bitfield)
    
    async def send_request(self, piece_index, begin, length):
        """Request a block from peer."""
        if self.peer_choking:
            raise PeerError("Peer is choking us")
        
        payload = struct.pack('!III', piece_index, begin, length)
        await self.send_message(MessageType.REQUEST, payload)
        
        self.active_requests.add((piece_index, begin, length))
    
    async def send_piece(self, piece_index, begin, data):
        """Send a piece block to peer."""
        if self.am_choking:
            logger.warning("Sending piece while choking peer")
        
        payload = struct.pack('!II', piece_index, begin) + data
        await self.send_message(MessageType.PIECE, payload)
        
        self.uploaded += len(data)
    
    async def send_cancel(self, piece_index, begin, length):
        """Cancel a block request."""
        payload = struct.pack('!III', piece_index, begin, length)
        await self.send_message(MessageType.CANCEL, payload)
        
        self.active_requests.discard((piece_index, begin, length))
    
    async def receive_message(self):
        """Receive and parse a message from peer."""
        try:
            # Read message length
            length_data = await self.reader.readexactly(4)
            length = struct.unpack('!I', length_data)[0]
            
            # Handle keepalive
            if length == 0:
                self.last_message_time = time.time()
                return MessageType.CHOKE, None  # Dummy type for keepalive
            
            # Read message type and payload
            message_data = await self.reader.readexactly(length)
            message_type = message_data[0]
            payload = message_data[1:] if length > 1 else b''
            
            self.last_message_time = time.time()
            
            # Process message
            await self._handle_message(message_type, payload)
            
            return message_type, payload
            
        except asyncio.IncompleteReadError:
            raise PeerError("Connection closed by peer")
        except struct.error:
            raise PeerError("Invalid message format")
    
    async def _handle_message(self, message_type, payload):
        """Handle received message based on type."""
        logger.debug(f"Received {MessageType(message_type).name if message_type < 21 else 'UNKNOWN'} from {self.ip}:{self.port}")
        
        if message_type == MessageType.CHOKE:
            self.peer_choking = True
            # Cancel all active requests
            self.active_requests.clear()
            
        elif message_type == MessageType.UNCHOKE:
            self.peer_choking = False
            
        elif message_type == MessageType.INTERESTED:
            self.peer_interested = True
            
        elif message_type == MessageType.NOT_INTERESTED:
            self.peer_interested = False
            
        elif message_type == MessageType.HAVE:
            if len(payload) != 4:
                raise PeerError("Invalid HAVE message")
            piece_index = struct.unpack('!I', payload)[0]
            
            if self.bitfield and piece_index < len(self.bitfield) * 8:
                # Update bitfield
                byte_index = piece_index // 8
                bit_index = piece_index % 8
                self.bitfield[byte_index] |= (1 << (7 - bit_index))
            
            if self.on_have:
                await self.on_have(self, piece_index)
            
        elif message_type == MessageType.BITFIELD:
            self.bitfield = bytearray(payload)
            if self.on_bitfield:
                await self.on_bitfield(self, self.bitfield)
            
        elif message_type == MessageType.REQUEST:
            if len(payload) != 12:
                raise PeerError("Invalid REQUEST message")
            piece_index, begin, length = struct.unpack('!III', payload)
            
            if self.on_request:
                await self.on_request(self, piece_index, begin, length)
            
        elif message_type == MessageType.PIECE:
            if len(payload) < 9:
                raise PeerError("Invalid PIECE message")
            piece_index, begin = struct.unpack('!II', payload[:8])
            block_data = payload[8:]
            
            self.downloaded += len(block_data)
            self.active_requests.discard((piece_index, begin, len(block_data)))
            
            if self.on_piece:
                await self.on_piece(self, piece_index, begin, block_data)
            
        elif message_type == MessageType.CANCEL:
            if len(payload) != 12:
                raise PeerError("Invalid CANCEL message")
            piece_index, begin, length = struct.unpack('!III', payload)
            
            if self.on_cancel:
                await self.on_cancel(self, piece_index, begin, length)
        
        elif message_type == MessageType.PORT:
            # DHT port announcement
            if len(payload) != 2:
                raise PeerError("Invalid PORT message")
            dht_port = struct.unpack('!H', payload)[0]
            logger.info(f"Peer {self.ip} announced DHT port: {dht_port}")
    
    def has_piece(self, piece_index):
        """Check if peer has a specific piece."""
        if not self.bitfield or piece_index >= len(self.bitfield) * 8:
            return False
        
        byte_index = piece_index // 8
        bit_index = piece_index % 8
        
        return bool(self.bitfield[byte_index] & (1 << (7 - bit_index)))
    
    def get_available_pieces(self):
        """Get list of piece indices that peer has."""
        if not self.bitfield:
            return []
        
        pieces = []
        for i in range(len(self.bitfield) * 8):
            if self.has_piece(i):
                pieces.append(i)
        
        return pieces
    
    async def close(self):
        """Close the peer connection."""
        self.connected = False
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
        
        logger.info(f"Closed connection to {self.ip}:{self.port}")
    
    async def run(self):
        """Main loop for handling peer messages."""
        try:
            while self.connected:
                # Check for timeout (2 minutes of inactivity)
                if time.time() - self.last_message_time > 120:
                    logger.warning(f"Peer {self.ip}:{self.port} timed out")
                    break
                
                # Send keepalive if needed (every 60 seconds)
                if time.time() - self.last_message_time > 60:
                    await self.send_keepalive()
                
                # Receive next message
                try:
                    await asyncio.wait_for(self.receive_message(), timeout=0.1)
                except asyncio.TimeoutError:
                    # No message received, continue
                    continue
                
        except PeerError as e:
            logger.error(f"Peer error for {self.ip}:{self.port}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error for {self.ip}:{self.port}: {e}")
        finally:
            await self.close()


class PeerManager:
    """Manages multiple peer connections."""
    
    def __init__(self, torrent, peer_id, max_peers=50):
        """
        Initialize peer manager.
        
        Args:
            torrent: Torrent object
            peer_id: Our peer ID
            max_peers: Maximum number of simultaneous connections
        """
        self.torrent = torrent
        self.peer_id = peer_id
        self.max_peers = max_peers
        
        self.peers = {}  # {(ip, port): PeerConnection}
        self.available_peers = deque()  # Peers to connect to
        
        # Statistics
        self.total_downloaded = 0
        self.total_uploaded = 0
    
    def add_peers(self, peer_list):
        """Add peers to the available peers queue."""
        for ip, port in peer_list:
            peer_key = (ip, port)
            if peer_key not in self.peers and peer_key not in self.available_peers:
                self.available_peers.append(peer_key)
        
        logger.info(f"Added {len(peer_list)} peers to queue")
    
    async def connect_to_peer(self, ip, port):
        """Connect to a specific peer."""
        peer = PeerConnection(self.torrent.info_hash, self.peer_id, ip, port)
        
        # Set up callbacks
        peer.on_piece = self._on_piece_received
        peer.on_have = self._on_have_received
        peer.on_bitfield = self._on_bitfield_received
        peer.on_request = self._on_request_received
        
        # Connect and handshake
        if await peer.connect():
            try:
                await peer.handshake()
                self.peers[(ip, port)] = peer
                
                # Start peer message loop
                asyncio.create_task(self._run_peer(peer))
                
                return peer
            except PeerError as e:
                logger.error(f"Handshake failed with {ip}:{port}: {e}")
                await peer.close()
        
        return None
    
    async def _run_peer(self, peer):
        """Run a peer connection."""
        try:
            await peer.run()
        finally:
            # Remove from active peers
            peer_key = (peer.ip, peer.port)
            if peer_key in self.peers:
                del self.peers[peer_key]
            
            # Update statistics
            self.total_downloaded += peer.downloaded
            self.total_uploaded += peer.uploaded
    
    async def maintain_connections(self):
        """Maintain the desired number of peer connections."""
        while len(self.peers) < self.max_peers and self.available_peers:
            ip, port = self.available_peers.popleft()
            await self.connect_to_peer(ip, port)
    
    async def _on_piece_received(self, peer, piece_index, begin, data):
        """Handle received piece data."""
        # This should be implemented by the piece manager
        logger.info(f"Received block: piece {piece_index}, offset {begin}, size {len(data)}")
    
    async def _on_have_received(self, peer, piece_index):
        """Handle HAVE message."""
        logger.debug(f"Peer {peer.ip} has piece {piece_index}")
    
    async def _on_bitfield_received(self, peer, bitfield):
        """Handle BITFIELD message."""
        pieces = peer.get_available_pieces()
        logger.info(f"Peer {peer.ip} has {len(pieces)} pieces")
    
    async def _on_request_received(self, peer, piece_index, begin, length):
        """Handle piece request from peer."""
        # This should be implemented by the piece manager
        logger.debug(f"Peer {peer.ip} requested: piece {piece_index}, offset {begin}, length {length}")
    
    async def broadcast_have(self, piece_index):
        """Broadcast HAVE message to all connected peers."""
        tasks = []
        for peer in self.peers.values():
            if peer.connected and peer.handshake_complete:
                tasks.append(peer.send_have(piece_index))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def close_all(self):
        """Close all peer connections."""
        tasks = []
        for peer in list(self.peers.values()):
            tasks.append(peer.close())
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        self.peers.clear()


# Testing
if __name__ == "__main__":
    import random
    
    # Set up logging
    logging.basicConfig(level=logging.DEBUG)
    
    # Create test data
    info_hash = hashlib.sha1(b'test_torrent').digest()
    peer_id = b'-SB0100-' + bytes(random.randint(0, 255) for _ in range(12))
    
    async def test_peer():
        # This would normally connect to a real peer
        peer = PeerConnection(info_hash, peer_id, "127.0.0.1", 6881)
        
        # Simulate some operations
        print(f"Created peer connection to {peer.ip}:{peer.port}")
        print(f"Info hash: {info_hash.hex()}")
        print(f"Peer ID: {peer_id}")
    
    # Run test
    asyncio.run(test_peer())
