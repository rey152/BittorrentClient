"""
DHT (Distributed Hash Table) for BitTorrent.
Implements the Kademlia-style MainLine DHT protocol (BEP 5).
"""

import asyncio
import socket
import struct
import time
import random
import hashlib
import logging
from collections import deque, defaultdict
from ipaddress import ip_address

from bencode import encode, decode

logger = logging.getLogger(__name__)


def generate_node_id():
    """Just creates a random 20-byte node ID for this DHT instance."""
    return bytes(random.randint(0, 255) for _ in range(20))


def distance(id1, id2):
    """Returns XOR distance between two node IDs."""
    return int.from_bytes(id1, 'big') ^ int.from_bytes(id2, 'big')


class DHTNode:
    """Represents one entry/node in the DHT network."""
    
    def __init__(self, node_id, ip, port):
        self.id = node_id
        self.ip = ip
        self.port = port
        self.last_seen = time.time()
        self.failed_queries = 0
    
    @property
    def addr(self):
        return (self.ip, self.port)
    
    def compact_info(self):
        """Returns the 'compact' node info (26 bytes: 20-byte ID + IP + port)."""
        try:
            ip_bytes = socket.inet_aton(self.ip)
            port_bytes = struct.pack('!H', self.port)
            return self.id + ip_bytes + port_bytes
        except:
            return None
    
    @classmethod
    def from_compact_info(cls, data):
        """Builds a node from compact info (used in DHT messages)."""
        if len(data) != 26:
            raise ValueError("Compact node info should be 26 bytes")
        
        node_id = data[:20]
        ip = socket.inet_ntoa(data[20:24])
        port = struct.unpack('!H', data[24:26])[0]
        
        return cls(node_id, ip, port)


class RoutingTable:
    """Kademlia routing table with buckets for nodes at varying distances."""
    
    def __init__(self, node_id, k=8):
        self.node_id = node_id
        self.k = k  # Max nodes per bucket
        self.buckets = [deque(maxlen=k) for _ in range(160)]  # One per bit
    
    def get_bucket_index(self, node_id):
        """Find the bucket index for a given node ID."""
        if node_id == self.node_id:
            return -1
        
        distance_int = distance(self.node_id, node_id)
        if distance_int == 0:
            return -1
        
        # Use the most significant bit as the bucket
        return distance_int.bit_length() - 1
    
    def add_node(self, node):
        """Add a node to the right bucket, remove old/bad if needed."""
        if node.id == self.node_id:
            return
        
        bucket_idx = self.get_bucket_index(node.id)
        if bucket_idx < 0:
            return
        
        bucket = self.buckets[bucket_idx]
        
        # If the node is already here, move it to the end (most recently seen)
        for i, existing_node in enumerate(bucket):
            if existing_node.id == node.id:
                del bucket[i]
                bucket.append(node)
                return
        
        # Add new node or replace an old one if the bucket is full
        if len(bucket) < self.k:
            bucket.append(node)
        else:
            oldest = bucket[0]
            if oldest.failed_queries >= 3:
                bucket.popleft()
                bucket.append(node)
    
    def remove_node(self, node_id):
        """Remove a node completely from the table."""
        bucket_idx = self.get_bucket_index(node_id)
        if bucket_idx < 0:
            return
        
        bucket = self.buckets[bucket_idx]
        for i, node in enumerate(bucket):
            if node.id == node_id:
                del bucket[i]
                return
    
    def find_closest_nodes(self, target_id, count=8):
        """Returns up to 'count' nodes closest to target_id."""
        nodes = []
        for bucket in self.buckets:
            nodes.extend(bucket)
        nodes.sort(key=lambda n: distance(n.id, target_id))
        return nodes[:count]
    
    def get_all_nodes(self):
        """Returns a flat list of all nodes in all buckets."""
        nodes = []
        for bucket in self.buckets:
            nodes.extend(bucket)
        return nodes


class DHT:
    """Main DHT implementation for peer discovery and decentralized storage."""
    
    def __init__(self, node_id=None, port=6881):
        self.node_id = node_id or generate_node_id()
        self.port = port
        self.routing_table = RoutingTable(self.node_id)
        self.queries = {}  # transaction_id: (query_type, callback)
        self.transaction_counter = 0
        self.announced = {}  # info_hash: announce_time
        self.peers = defaultdict(set)  # info_hash: set of (ip, port)
        self.socket = None
        self.transport = None
        self.protocol = None
        self.bootstrap_nodes = [
            ('router.bittorrent.com', 6881),
            ('dht.transmissionbt.com', 6881),
            ('router.utorrent.com', 6881),
        ]
    
    async def start(self):
        """Start up the DHT node and bootstrap into the network."""
        loop = asyncio.get_event_loop()
        self.transport, self.protocol = await loop.create_datagram_endpoint(
            lambda: DHTProtocol(self),
            local_addr=('0.0.0.0', self.port)
        )
        logger.info(f"DHT started on port {self.port} with node ID: {self.node_id.hex()}")
        await self.bootstrap()
        asyncio.create_task(self.maintenance_loop())
    
    async def stop(self):
        """Stop the DHT node and close the socket."""
        if self.transport:
            self.transport.close()
    
    async def bootstrap(self):
        """Ping the well-known bootstrap nodes to get started."""
        for host, port in self.bootstrap_nodes:
            try:
                info = await asyncio.get_event_loop().getaddrinfo(
                    host, port, family=socket.AF_INET, type=socket.SOCK_DGRAM
                )
                if info:
                    ip = info[0][4][0]
                    await self.ping(ip, port)
            except Exception as e:
                logger.debug(f"Couldn't contact bootstrap node {host}:{port}: {e}")
        await self.find_node(self.node_id)
    
    async def maintenance_loop(self):
        """Background loop for periodic DHT housekeeping."""
        while self.transport and not self.transport.is_closing():
            try:
                # Bucket refresh
                for bucket in self.routing_table.buckets:
                    if bucket and time.time() - bucket[-1].last_seen > 900:  # 15 min
                        random_id = generate_node_id()
                        await self.find_node(random_id)
                # Re-announce torrents
                current_time = time.time()
                for info_hash, announce_time in list(self.announced.items()):
                    if current_time - announce_time > 1800:  # 30 min
                        await self.announce_peer(info_hash, self.port)
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"DHT maintenance error: {e}")
    
    def make_query(self, query_type, args):
        """Create a DHT query with a new transaction ID."""
        self.transaction_counter = (self.transaction_counter + 1) % 65536
        tid = struct.pack('!H', self.transaction_counter)
        query = {
            b't': tid,
            b'y': b'q',
            b'q': query_type.encode(),
            b'a': args
        }
        return tid, encode(query)
    
    async def send_query(self, addr, query_type, args, callback=None):
        """Send a query to a node and optionally store a callback."""
        tid, message = self.make_query(query_type, args)
        if callback:
            self.queries[tid] = (query_type, callback)
        self.transport.sendto(message, addr)
        asyncio.create_task(self._cleanup_query(tid, 5))
    
    async def _cleanup_query(self, tid, delay):
        """Remove query from pending after timeout."""
        await asyncio.sleep(delay)
        self.queries.pop(tid, None)
    
    def send_response(self, addr, tid, response):
        """Send a response to a query from another node."""
        message = {
            b't': tid,
            b'y': b'r',
            b'r': response
        }
        self.transport.sendto(encode(message), addr)
    
    def send_error(self, addr, tid, error_code, error_msg):
        """Send an error response to a query."""
        message = {
            b't': tid,
            b'y': b'e',
            b'e': [error_code, error_msg.encode()]
        }
        self.transport.sendto(encode(message), addr)
    
    async def ping(self, ip, port):
        """Send a ping to another node."""
        args = {b'id': self.node_id}
        await self.send_query((ip, port), 'ping', args)
    
    async def find_node(self, target_id):
        """Find nodes near a given target ID."""
        args = {
            b'id': self.node_id,
            b'target': target_id
        }
        closest = self.routing_table.find_closest_nodes(target_id)
        for node in closest[:3]:
            await self.send_query(node.addr, 'find_node', args)
    
    async def get_peers(self, info_hash):
        """Look up which peers have a given torrent."""
        args = {
            b'id': self.node_id,
            b'info_hash': info_hash
        }
        if info_hash not in self.peers:
            self.peers[info_hash] = set()
        closest = self.routing_table.find_closest_nodes(info_hash)
        for node in closest[:8]:
            await self.send_query(node.addr, 'get_peers', args,
                                  lambda r: self._handle_get_peers_response(info_hash, r))
        await self.find_node(info_hash)
    
    def _handle_get_peers_response(self, info_hash, response):
        """Callback for get_peers responses."""
        if b'values' in response:
            for peer_data in response[b'values']:
                if len(peer_data) == 6:
                    ip = socket.inet_ntoa(peer_data[:4])
                    port = struct.unpack('!H', peer_data[4:6])[0]
                    self.peers[info_hash].add((ip, port))
                    logger.debug(f"Discovered peer for {info_hash.hex()}: {ip}:{port}")
    
    async def announce_peer(self, info_hash, port, implied_port=True):
        """Tell the DHT we're sharing a particular torrent."""
        self.announced[info_hash] = time.time()
        token = b''  # Normally obtained from get_peers, but we skip here
        args = {
            b'id': self.node_id,
            b'info_hash': info_hash,
            b'port': port,
            b'token': token
        }
        if implied_port:
            args[b'implied_port'] = 1
        closest = self.routing_table.find_closest_nodes(info_hash)
        for node in closest[:8]:
            await self.send_query(node.addr, 'announce_peer', args)
    
    def handle_query(self, addr, tid, query_type, args):
        """Handle incoming DHT query from another node."""
        if b'id' in args:
            node = DHTNode(args[b'id'], addr[0], addr[1])
            self.routing_table.add_node(node)
        if query_type == b'ping':
            self.handle_ping(addr, tid, args)
        elif query_type == b'find_node':
            self.handle_find_node(addr, tid, args)
        elif query_type == b'get_peers':
            self.handle_get_peers(addr, tid, args)
        elif query_type == b'announce_peer':
            self.handle_announce_peer(addr, tid, args)
        else:
            self.send_error(addr, tid, 204, "Unknown method")
    
    def handle_ping(self, addr, tid, args):
        """Respond to a ping query."""
        response = {b'id': self.node_id}
        self.send_response(addr, tid, response)
    
    def handle_find_node(self, addr, tid, args):
        """Respond to a find_node query."""
        if b'target' not in args:
            self.send_error(addr, tid, 203, "Missing target")
            return
        target = args[b'target']
        nodes = self.routing_table.find_closest_nodes(target, count=8)
        compact_nodes = b''
        for node in nodes:
            info = node.compact_info()
            if info:
                compact_nodes += info
        response = {
            b'id': self.node_id,
            b'nodes': compact_nodes
        }
        self.send_response(addr, tid, response)
    
    def handle_get_peers(self, addr, tid, args):
        """Respond to a get_peers query."""
        if b'info_hash' not in args:
            self.send_error(addr, tid, 203, "Missing info_hash")
            return
        info_hash = args[b'info_hash']
        token = hashlib.sha1(addr[0].encode() + str(self.port).encode()).digest()[:4]
        response = {
            b'id': self.node_id,
            b'token': token
        }
        if info_hash in self.peers and self.peers[info_hash]:
            values = []
            for ip, port in list(self.peers[info_hash])[:50]:
                try:
                    peer_data = socket.inet_aton(ip) + struct.pack('!H', port)
                    values.append(peer_data)
                except:
                    pass
            if values:
                response[b'values'] = values
        else:
            nodes = self.routing_table.find_closest_nodes(info_hash, count=8)
            compact_nodes = b''
            for node in nodes:
                info = node.compact_info()
                if info:
                    compact_nodes += info
            response[b'nodes'] = compact_nodes
        self.send_response(addr, tid, response)
    
    def handle_announce_peer(self, addr, tid, args):
        """Respond to an announce_peer query."""
        if b'info_hash' not in args or b'port' not in args:
            self.send_error(addr, tid, 203, "Missing arguments")
            return
        # Normally would verify token here
        info_hash = args[b'info_hash']
        if args.get(b'implied_port', 0):
            port = addr[1]
        else:
            port = args[b'port']
        self.peers[info_hash].add((addr[0], port))
        logger.info(f"Peer announced for {info_hash.hex()}: {addr[0]}:{port}")
        response = {b'id': self.node_id}
        self.send_response(addr, tid, response)
    
    def handle_response(self, addr, tid, response):
        """Process incoming DHT response messages."""
        if b'id' in response:
            node = DHTNode(response[b'id'], addr[0], addr[1])
            self.routing_table.add_node(node)
        if b'nodes' in response:
            self.process_compact_nodes(response[b'nodes'])
        if tid in self.queries:
            query_type, callback = self.queries.pop(tid)
            if callback:
                callback(response)
    
    def process_compact_nodes(self, compact_nodes):
        """Unpack compact node info and add new nodes to our routing table."""
        for i in range(0, len(compact_nodes), 26):
            if i + 26 <= len(compact_nodes):
                try:
                    node = DHTNode.from_compact_info(compact_nodes[i:i+26])
                    self.routing_table.add_node(node)
                except Exception as e:
                    logger.debug(f"Failed to parse compact node: {e}")


class DHTProtocol(asyncio.DatagramProtocol):
    """Handles incoming/outgoing UDP messages for DHT."""
    
    def __init__(self, dht):
        self.dht = dht
    
    def connection_made(self, transport):
        self.transport = transport
    
    def datagram_received(self, data, addr):
        """Process an incoming datagram from the network."""
        try:
            message = decode(data)
            if not isinstance(message, dict) or b't' not in message:
                return
            tid = message[b't']
            msg_type = message.get(b'y', b'')
            if msg_type == b'q':
                query_type = message.get(b'q', b'')
                args = message.get(b'a', {})
                self.dht.handle_query(addr, tid, query_type, args)
            elif msg_type == b'r':
                response = message.get(b'r', {})
                self.dht.handle_response(addr, tid, response)
            elif msg_type == b'e':
                error = message.get(b'e', [0, b''])
                logger.debug(f"DHT error from {addr}: {error}")
        except Exception as e:
            logger.debug(f"Failed to process DHT message from {addr}: {e}")


# Self-test (for development, not production)
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.DEBUG)
    async def test_dht():
        dht = DHT(port=6882)
        await dht.start()
        test_hash = hashlib.sha1(b'test_torrent').digest()
        print(f"DHT Node ID: {dht.node_id.hex()}")
        print(f"Looking for peers for: {test_hash.hex()}")
        await dht.get_peers(test_hash)
        await asyncio.sleep(30)
        print(f"\nRouting table: {len(dht.routing_table.get_all_nodes())} nodes")
        print(f"Peers found: {len(dht.peers.get(test_hash, []))}")
        await dht.stop()
    asyncio.run(test_dht())
