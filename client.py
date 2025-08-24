"""
Main BitTorrent client implementation.
Coordinates all components to download and upload torrents.
"""

import asyncio
import logging
import random
import time
import os
from pathlib import Path

from torrent import Torrent
from tracker import TrackerManager, TrackerEvent
from peer import PeerManager
from piece_manager import PieceManager
from dht import DHT

logger = logging.getLogger(__name__)


def generate_peer_id():
    """Generate a unique peer ID."""
    # Format: -<client_id><version>-<random_bytes>
    # SB = Simple Bittorrent, 0100 = version 1.0.0
    client_prefix = b'-SB0100-'
    random_bytes = bytes(random.randint(0, 255) for _ in range(12))
    return client_prefix + random_bytes


class TorrentClient:
    """Main BitTorrent client class."""
    
    def __init__(self, download_dir="downloads", port=6881, dht_port=None):
        """
        Initialize the BitTorrent client.
        
        Args:
            download_dir: Directory to save downloaded files
            port: Port for peer connections
            dht_port: Port for DHT (uses port+1 if not specified)
        """
        self.download_dir = Path(download_dir)
        self.port = port
        self.dht_port = dht_port or (port + 1)
        self.peer_id = generate_peer_id()
        
        # Active downloads
        self.downloads = {}  # info_hash: TorrentDownload
        
        # DHT
        self.dht = None
        
        # Statistics
        self.total_downloaded = 0
        self.total_uploaded = 0
        
        logger.info(f"BitTorrent client initialized with peer ID: {self.peer_id}")
    
    async def start(self):
        """Start the BitTorrent client."""
        # Create download directory
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        # Start DHT
        self.dht = DHT(port=self.dht_port)
        await self.dht.start()
        
        logger.info("BitTorrent client started")
    
    async def stop(self):
        """Stop the BitTorrent client."""
        # Stop all downloads
        tasks = []
        for download in list(self.downloads.values()):
            tasks.append(download.stop())
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Stop DHT
        if self.dht:
            await self.dht.stop()
        
        logger.info("BitTorrent client stopped")
    
    async def add_torrent(self, torrent_path, download_dir=None):
        """
        Add a torrent for downloading.
        
        Args:
            torrent_path: Path to .torrent file or magnet link
            download_dir: Override download directory for this torrent
            
        Returns:
            TorrentDownload instance
        """
        # Parse torrent
        torrent = Torrent(torrent_path)
        
        # Check if already downloading
        if torrent.info_hash in self.downloads:
            logger.warning(f"Torrent already downloading: {torrent.name}")
            return self.downloads[torrent.info_hash]
        
        # Create download
        download_dir = download_dir or self.download_dir
        download = TorrentDownload(
            torrent, self.peer_id, download_dir, 
            self.port, self.dht
        )
        
        self.downloads[torrent.info_hash] = download
        
        # Start download
        await download.start()
        
        logger.info(f"Added torrent: {torrent.name}")
        return download
    
    def remove_torrent(self, info_hash, delete_files=False):
        """Remove a torrent from the client."""
        if info_hash in self.downloads:
            download = self.downloads[info_hash]
            asyncio.create_task(download.stop())
            del self.downloads[info_hash]
            
            if delete_files:
                # TODO: Delete downloaded files
                pass
    
    def get_downloads(self):
        """Get all active downloads."""
        return list(self.downloads.values())
    
    def get_stats(self):
        """Get client statistics."""
        active_downloads = len(self.downloads)
        downloading = sum(1 for d in self.downloads.values() if not d.is_complete())
        seeding = active_downloads - downloading
        
        return {
            'peer_id': self.peer_id.decode('latin1'),
            'active_downloads': active_downloads,
            'downloading': downloading,
            'seeding': seeding,
            'total_downloaded': self.total_downloaded,
            'total_uploaded': self.total_uploaded,
            'dht_nodes': len(self.dht.routing_table.get_all_nodes()) if self.dht else 0
        }


class TorrentDownload:
    """Manages downloading/uploading a single torrent."""
    
    def __init__(self, torrent, peer_id, download_dir, port, dht):
        self.torrent = torrent
        self.peer_id = peer_id
        self.download_dir = download_dir
        self.port = port
        self.dht = dht
        
        # Components
        self.tracker_manager = TrackerManager(torrent, peer_id)
        self.peer_manager = PeerManager(torrent, peer_id)
        self.piece_manager = PieceManager(torrent, download_dir)
        
        # State
        self.running = False
        self.start_time = None
        self.complete_time = None
        
        # Tasks
        self.tasks = []
    
    async def start(self):
        """Start downloading the torrent."""
        if self.running:
            return
        
        self.running = True
        self.start_time = time.time()
        
        # Start initial announce
        asyncio.create_task(self._announce_loop())
        
        # Start DHT announce
        if self.dht:
            await self.dht.announce_peer(self.torrent.info_hash, self.port)
            asyncio.create_task(self._dht_peer_search())
        
        # Start peer connections
        asyncio.create_task(self._peer_loop())
        
        # Start piece downloading
        asyncio.create_task(self._download_loop())
        
        logger.info(f"Started downloading: {self.torrent.name}")
    
    async def stop(self):
        """Stop downloading the torrent."""
        if not self.running:
            return
        
        self.running = False
        
        # Send stopped event to trackers
        await self._announce(TrackerEvent.STOPPED)
        
        # Close all peer connections
        await self.peer_manager.close_all()
        
        # Close tracker connections
        self.tracker_manager.close()
        
        logger.info(f"Stopped downloading: {self.torrent.name}")
    
    async def _announce_loop(self):
        """Periodically announce to trackers."""
        first_announce = True
        
        while self.running:
            try:
                # Determine event
                if first_announce:
                    event = TrackerEvent.STARTED
                    first_announce = False
                elif self.is_complete() and not self.complete_time:
                    event = TrackerEvent.COMPLETED
                    self.complete_time = time.time()
                else:
                    event = TrackerEvent.NONE
                
                # Announce
                interval = await self._announce(event)
                
                # Wait for next announce
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Announce error: {e}")
                await asyncio.sleep(60)  # Retry after 1 minute
    
    async def _announce(self, event=TrackerEvent.NONE):
        """Announce to trackers and get peers."""
        stats = self.piece_manager.get_stats()
        
        response = await self.tracker_manager.announce(
            uploaded=stats['uploaded'],
            downloaded=stats['downloaded'],
            left=self.torrent.total_size - stats['downloaded'],
            event=event,
            port=self.port
        )
        
        if response and response.peers:
            self.peer_manager.add_peers(response.peers)
            logger.info(f"Got {len(response.peers)} peers from tracker")
            return response.interval
        
        return 1800  # Default 30 minutes
    
    async def _dht_peer_search(self):
        """Periodically search for peers using DHT."""
        while self.running:
            try:
                # Search for peers
                await self.dht.get_peers(self.torrent.info_hash)
                
                # Wait a bit to collect responses
                await asyncio.sleep(10)
                
                # Add found peers
                dht_peers = self.dht.peers.get(self.torrent.info_hash, set())
                if dht_peers:
                    self.peer_manager.add_peers(list(dht_peers))
                    logger.info(f"Got {len(dht_peers)} peers from DHT")
                
                # Re-announce periodically
                await self.dht.announce_peer(self.torrent.info_hash, self.port)
                
                # Wait before next search
                await asyncio.sleep(300)  # 5 minutes
                
            except Exception as e:
                logger.error(f"DHT peer search error: {e}")
                await asyncio.sleep(60)
    
    async def _peer_loop(self):
        """Maintain peer connections."""
        while self.running:
            try:
                # Maintain connections
                await self.peer_manager.maintain_connections()
                
                # Update piece manager callbacks
                for peer in self.peer_manager.peers.values():
                    if not peer.on_piece:
                        peer.on_piece = self._on_piece_received
                        peer.on_have = self._on_have_received
                        peer.on_bitfield = self._on_bitfield_received
                        peer.on_request = self._on_request_received
                
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Peer loop error: {e}")
                await asyncio.sleep(5)
    
    async def _download_loop(self):
        """Main download loop."""
        while self.running and not self.is_complete():
            try:
                # Request pieces from all unchoked peers
                for peer_key, peer in list(self.peer_manager.peers.items()):
                    if (peer.connected and peer.handshake_complete and 
                        not peer.peer_choking and peer.am_interested):
                        
                        # Get available pieces from peer
                        peer_pieces = set(peer.get_available_pieces())
                        
                        # Request up to 10 blocks from this peer
                        for _ in range(10):
                            block = self.piece_manager.get_next_block(peer_key, peer_pieces)
                            if block:
                                try:
                                    await peer.send_request(
                                        block.piece_index,
                                        block.offset,
                                        block.length
                                    )
                                except Exception as e:
                                    logger.debug(f"Failed to request block from {peer.ip}: {e}")
                                    break
                            else:
                                break
                
                # Send bitfield to new peers
                for peer in self.peer_manager.peers.values():
                    if peer.connected and peer.handshake_complete and not hasattr(peer, '_bitfield_sent'):
                        await peer.send_bitfield(self.piece_manager.bitfield)
                        peer._bitfield_sent = True
                
                # Show progress
                stats = self.piece_manager.get_stats()
                progress = stats['progress'] * 100
                speed = stats['download_speed'] / 1024 / 1024  # MB/s
                logger.info(f"Progress: {progress:.1f}% | Speed: {speed:.2f} MB/s | "
                          f"Peers: {len(self.peer_manager.peers)}")
                
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Download loop error: {e}")
                await asyncio.sleep(5)
        
        if self.is_complete():
            logger.info(f"Download complete: {self.torrent.name}")
    
    async def _on_piece_received(self, peer, piece_index, offset, data):
        """Handle received piece data."""
        # Add block to piece manager
        piece_complete = await self.piece_manager.add_block(piece_index, offset, data)
        
        if piece_complete:
            # Broadcast HAVE to all peers
            await self.peer_manager.broadcast_have(piece_index)
    
    async def _on_have_received(self, peer, piece_index):
        """Handle HAVE message from peer."""
        peer_key = (peer.ip, peer.port)
        self.piece_manager.update_peer_pieces(peer_key, have_index=piece_index)
        
        # Update interest
        needed_pieces = set(self.piece_manager.get_needed_pieces())
        peer_pieces = set(peer.get_available_pieces())
        
        if needed_pieces & peer_pieces:
            if not peer.am_interested:
                await peer.send_interested()
        else:
            if peer.am_interested:
                await peer.send_not_interested()
    
    async def _on_bitfield_received(self, peer, bitfield):
        """Handle BITFIELD message from peer."""
        peer_key = (peer.ip, peer.port)
        self.piece_manager.update_peer_pieces(peer_key, bitfield=bitfield)
        
        # Update interest
        needed_pieces = set(self.piece_manager.get_needed_pieces())
        peer_pieces = set(peer.get_available_pieces())
        
        if needed_pieces & peer_pieces:
            await peer.send_interested()
        else:
            await peer.send_not_interested()
    
    async def _on_request_received(self, peer, piece_index, offset, length):
        """Handle piece request from peer."""
        # Check if we're choking the peer
        if peer.am_choking:
            return
        
        # Get block data
        data = self.piece_manager.get_block_data(piece_index, offset, length)
        if data:
            await peer.send_piece(piece_index, offset, data)
            self.piece_manager.total_uploaded += len(data)
    
    def is_complete(self):
        """Check if download is complete."""
        return self.piece_manager.is_complete()
    
    def get_stats(self):
        """Get download statistics."""
        stats = self.piece_manager.get_stats()
        stats.update({
            'name': self.torrent.name,
            'size': self.torrent.total_size,
            'info_hash': self.torrent.info_hash_hex,
            'peers': len(self.peer_manager.peers),
            'seeders': sum(1 for p in self.peer_manager.peers.values() 
                         if p.connected and len(p.get_available_pieces()) == self.torrent.num_pieces),
            'leechers': len(self.peer_manager.peers) - stats.get('seeders', 0),
        })
        
        if self.start_time:
            elapsed = time.time() - self.start_time
            if elapsed > 0:
                stats['average_speed'] = stats['downloaded'] / elapsed
        
        return stats


# Testing
if __name__ == "__main__":
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    async def test_client():
        client = TorrentClient()
        await client.start()
        
        # Test with a torrent file
        if len(sys.argv) > 1:
            torrent_path = sys.argv[1]
            download = await client.add_torrent(torrent_path)
            
            # Monitor progress
            while not download.is_complete():
                stats = download.get_stats()
                print(f"\rProgress: {stats['progress']*100:.1f}%", end='', flush=True)
                await asyncio.sleep(5)
            
            print("\nDownload complete!")
        
        await client.stop()
    
    asyncio.run(test_client())
