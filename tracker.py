"""
Tracker communication module for BitTorrent client.
Handles both HTTP and UDP tracker protocols.
"""

import random
import socket
import struct
import time
from urllib.parse import urlencode, urlparse
import requests
from enum import IntEnum

from bencode import decode, encode


class TrackerError(Exception):
    """Base exception for tracker-related errors."""
    pass


class TrackerEvent(IntEnum):
    """Tracker announce events."""
    NONE = 0
    COMPLETED = 1
    STARTED = 2
    STOPPED = 3


class TrackerResponse:
    """Represents a response from a tracker."""
    
    def __init__(self, peers, interval=1800, min_interval=None, 
                 tracker_id=None, complete=None, incomplete=None):
        self.peers = peers  # List of (ip, port) tuples
        self.interval = interval  # Seconds until next announce
        self.min_interval = min_interval or interval
        self.tracker_id = tracker_id
        self.complete = complete  # Number of seeders
        self.incomplete = incomplete  # Number of leechers


class Tracker:
    """Base class for tracker communication."""
    
    def __init__(self, announce_url, torrent_info_hash, peer_id):
        self.announce_url = announce_url
        self.info_hash = torrent_info_hash
        self.peer_id = peer_id
        self.parsed_url = urlparse(announce_url)
        
    def announce(self, uploaded, downloaded, left, event=TrackerEvent.NONE, 
                 port=6881, num_want=50):
        """Announce to the tracker. Must be implemented by subclasses."""
        raise NotImplementedError


class HTTPTracker(Tracker):
    """HTTP/HTTPS tracker implementation."""
    
    def __init__(self, announce_url, torrent_info_hash, peer_id):
        super().__init__(announce_url, torrent_info_hash, peer_id)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SimpleBittorrentClient/1.0'
        })
    
    def announce(self, uploaded, downloaded, left, event=TrackerEvent.NONE,
                 port=6881, num_want=50, compact=True):
        """Send announce request to HTTP tracker."""
        params = {
            'info_hash': self.info_hash,
            'peer_id': self.peer_id,
            'uploaded': uploaded,
            'downloaded': downloaded,
            'left': left,
            'port': port,
            'compact': 1 if compact else 0,
            'num_want': num_want,
        }
        
        if event != TrackerEvent.NONE:
            params['event'] = event.name.lower()
        
        # Build URL with raw info_hash and peer_id (they're already encoded)
        query_parts = []
        for key, value in params.items():
            if key in ('info_hash', 'peer_id'):
                # These are raw bytes, need special handling
                if isinstance(value, bytes):
                    # URL encode each byte
                    encoded = ''.join(f'%{b:02x}' for b in value)
                    query_parts.append(f'{key}={encoded}')
            else:
                query_parts.append(f'{key}={value}')
        
        url = f"{self.announce_url}?{'&'.join(query_parts)}"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse bencode response
            data = decode(response.content)
            
            if b'failure reason' in data:
                raise TrackerError(data[b'failure reason'].decode('utf-8', errors='replace'))
            
            # Extract peer list
            peers = self._parse_peers(data.get(b'peers', b''), compact)
            
            # Build response object
            return TrackerResponse(
                peers=peers,
                interval=data.get(b'interval', 1800),
                min_interval=data.get(b'min interval'),
                tracker_id=data.get(b'tracker id'),
                complete=data.get(b'complete'),
                incomplete=data.get(b'incomplete')
            )
            
        except requests.RequestException as e:
            raise TrackerError(f"HTTP request failed: {e}")
        except Exception as e:
            raise TrackerError(f"Failed to parse tracker response: {e}")
    
    def _parse_peers(self, peer_data, compact):
        """Parse peer list from tracker response."""
        peers = []
        
        if compact and isinstance(peer_data, bytes):
            # Compact format: 6 bytes per peer (4 for IP, 2 for port)
            for i in range(0, len(peer_data), 6):
                if i + 6 <= len(peer_data):
                    ip_bytes = peer_data[i:i+4]
                    port_bytes = peer_data[i+4:i+6]
                    
                    ip = '.'.join(str(b) for b in ip_bytes)
                    port = struct.unpack('!H', port_bytes)[0]
                    
                    peers.append((ip, port))
        elif isinstance(peer_data, list):
            # Non-compact format: list of dictionaries
            for peer in peer_data:
                if isinstance(peer, dict) and b'ip' in peer and b'port' in peer:
                    ip = peer[b'ip'].decode('utf-8', errors='replace')
                    port = peer[b'port']
                    peers.append((ip, port))
        
        return peers


class UDPTracker(Tracker):
    """UDP tracker implementation (BEP 15)."""
    
    # UDP tracker actions
    ACTION_CONNECT = 0
    ACTION_ANNOUNCE = 1
    ACTION_SCRAPE = 2
    ACTION_ERROR = 3
    
    # Magic constant for connection
    MAGIC_CONNECTION_ID = 0x41727101980
    
    def __init__(self, announce_url, torrent_info_hash, peer_id):
        super().__init__(announce_url, torrent_info_hash, peer_id)
        self.connection_id = None
        self.connection_expiry = 0
        self.socket = None
        
    def _create_socket(self):
        """Create and configure UDP socket."""
        if self.socket:
            self.socket.close()
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(15)
    
    def _send_receive(self, data, host, port):
        """Send data and receive response."""
        if not self.socket:
            self._create_socket()
        
        self.socket.sendto(data, (host, port))
        
        try:
            response, addr = self.socket.recvfrom(2048)
            return response
        except socket.timeout:
            raise TrackerError("UDP tracker timeout")
    
    def _connect(self):
        """Establish connection with UDP tracker."""
        # Check if we have a valid connection
        if self.connection_id and time.time() < self.connection_expiry:
            return
        
        transaction_id = random.randint(0, 2**32 - 1)
        
        # Build connect request
        request = struct.pack('!QII',
            self.MAGIC_CONNECTION_ID,  # connection_id
            self.ACTION_CONNECT,       # action
            transaction_id            # transaction_id
        )
        
        host = self.parsed_url.hostname
        port = self.parsed_url.port or 80
        
        response = self._send_receive(request, host, port)
        
        if len(response) < 16:
            raise TrackerError("Invalid connect response")
        
        action, resp_transaction_id, connection_id = struct.unpack('!IIQ', response)
        
        if action != self.ACTION_CONNECT:
            raise TrackerError(f"Unexpected action in connect response: {action}")
        
        if resp_transaction_id != transaction_id:
            raise TrackerError("Transaction ID mismatch")
        
        self.connection_id = connection_id
        self.connection_expiry = time.time() + 60  # Connection valid for 1 minute
    
    def announce(self, uploaded, downloaded, left, event=TrackerEvent.NONE,
                 port=6881, num_want=50, key=None):
        """Send announce request to UDP tracker."""
        # Ensure we're connected
        self._connect()
        
        transaction_id = random.randint(0, 2**32 - 1)
        
        # Generate a key if not provided
        if key is None:
            key = random.randint(0, 2**32 - 1)
        
        # Build announce request
        request = struct.pack('!QII20s20sQQQIIIIH',
            self.connection_id,     # connection_id
            self.ACTION_ANNOUNCE,   # action
            transaction_id,         # transaction_id
            self.info_hash,         # info_hash
            self.peer_id,           # peer_id
            downloaded,             # downloaded
            left,                   # left
            uploaded,               # uploaded
            event,                  # event
            0,                      # ip (0 = default)
            key,                    # key
            num_want,               # num_want
            port                    # port
        )
        
        host = self.parsed_url.hostname
        port_num = self.parsed_url.port or 80
        
        response = self._send_receive(request, host, port_num)
        
        if len(response) < 20:
            raise TrackerError("Invalid announce response")
        
        # Parse response header
        action, resp_transaction_id, interval, leechers, seeders = struct.unpack('!IIIII', response[:20])
        
        if action == self.ACTION_ERROR:
            # Error response
            error_msg = response[8:].decode('utf-8', errors='replace')
            raise TrackerError(f"Tracker error: {error_msg}")
        
        if action != self.ACTION_ANNOUNCE:
            raise TrackerError(f"Unexpected action in announce response: {action}")
        
        if resp_transaction_id != transaction_id:
            raise TrackerError("Transaction ID mismatch")
        
        # Parse peer list (compact format)
        peers = []
        peer_data = response[20:]
        
        for i in range(0, len(peer_data), 6):
            if i + 6 <= len(peer_data):
                ip_bytes = peer_data[i:i+4]
                port_bytes = peer_data[i+4:i+6]
                
                ip = '.'.join(str(b) for b in ip_bytes)
                peer_port = struct.unpack('!H', port_bytes)[0]
                
                peers.append((ip, peer_port))
        
        return TrackerResponse(
            peers=peers,
            interval=interval,
            complete=seeders,
            incomplete=leechers
        )
    
    def close(self):
        """Close the UDP socket."""
        if self.socket:
            self.socket.close()
            self.socket = None


def create_tracker(announce_url, info_hash, peer_id):
    """
    Factory function to create appropriate tracker instance.
    
    Args:
        announce_url: Tracker announce URL
        info_hash: 20-byte torrent info hash
        peer_id: 20-byte peer ID
        
    Returns:
        Tracker instance (HTTPTracker or UDPTracker)
    """
    parsed = urlparse(announce_url)
    
    if parsed.scheme in ('http', 'https'):
        return HTTPTracker(announce_url, info_hash, peer_id)
    elif parsed.scheme == 'udp':
        return UDPTracker(announce_url, info_hash, peer_id)
    else:
        raise ValueError(f"Unsupported tracker protocol: {parsed.scheme}")


class TrackerManager:
    """Manages multiple trackers for a torrent."""
    
    def __init__(self, torrent, peer_id):
        """
        Initialize tracker manager.
        
        Args:
            torrent: Torrent object
            peer_id: 20-byte peer ID
        """
        self.torrent = torrent
        self.peer_id = peer_id
        self.trackers = []
        
        # Initialize trackers from announce list
        for tier in torrent.announce_list:
            tier_trackers = []
            for announce_url in tier:
                try:
                    tracker = create_tracker(announce_url, torrent.info_hash, peer_id)
                    tier_trackers.append(tracker)
                except Exception as e:
                    print(f"Failed to create tracker for {announce_url}: {e}")
            
            if tier_trackers:
                self.trackers.append(tier_trackers)
    
    def announce(self, uploaded, downloaded, left, event=TrackerEvent.NONE,
                 port=6881, num_want=50):
        """
        Announce to trackers, trying each tier until success.
        
        Returns:
            TrackerResponse or None if all trackers fail
        """
        for tier in self.trackers:
            # Randomize tracker order within tier
            tier_copy = tier.copy()
            random.shuffle(tier_copy)
            
            for tracker in tier_copy:
                try:
                    response = tracker.announce(
                        uploaded, downloaded, left, event, port, num_want
                    )
                    return response
                except Exception as e:
                    print(f"Tracker {tracker.announce_url} failed: {e}")
                    continue
            
            # If we got here, all trackers in this tier failed
            # Continue to next tier
        
        return None
    
    def close(self):
        """Close all tracker connections."""
        for tier in self.trackers:
            for tracker in tier:
                if isinstance(tracker, UDPTracker):
                    tracker.close()


# Testing
if __name__ == "__main__":
    # Generate test peer ID
    peer_id = b'-SB0100-' + bytes(random.randint(0, 255) for _ in range(12))
    
    # Test bencode parsing
    test_response = encode({
        b'interval': 1800,
        b'peers': b''.join([
            bytes([127, 0, 0, 1]) + struct.pack('!H', 6881),
            bytes([192, 168, 1, 1]) + struct.pack('!H', 6882),
        ])
    })
    
    # Test compact peer parsing
    http_tracker = HTTPTracker('http://example.com/announce', b'test_info_hash' * 20, peer_id)
    decoded = decode(test_response)
    peers = http_tracker._parse_peers(decoded[b'peers'], compact=True)
    print(f"Parsed peers: {peers}")
