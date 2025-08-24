"""
Main BitTorrent client.
This module brings together all the key components to handle torrent downloads and uploads.
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
    """Create a unique peer ID for this client instance."""
    # Format: -<client_id><version>-<random_bytes>
    # SB = Simple Bittorrent, 0100 = version 1.0.0
    client_prefix = b'-SB0100-'
    random_bytes = bytes(random.randint(0, 255) for _ in range(12))
    return client_prefix + random_bytes


class TorrentClient:
    """Handles overall BitTorrent operations for this client."""
    
    def __init__(self, download_dir="downloads", port=6881, dht_port=None):
        """
        Set up the BitTorrent client and its main properties.
        
        Args:
            download_dir: Where downloaded files will be saved.
            port: Port to use for peer connections.
            dht_port: Port for DHT (defaults to port+1 if not set).
        """
        self.download_dir = Path(download_dir)
        self.port = port
        self.dht_port = dht_port or (port + 1)
        self.peer_id = generate_peer_id()
        
        # Keeps track of active downloads
        self.downloads = {}  # info_hash: TorrentDownload
        
        # DHT (for peer discovery)
        self.dht = None
        
        # Stats
        self.total_downloaded = 0
        self.total_uploaded = 0
        
        logger.info(f"BitTorrent client initialized with peer ID: {self.peer_id}")
    
    async def start(self):
        """Start up the BitTorrent client, including DHT and directories."""
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        # Fire up DHT for decentralized peer finding
        self.dht = DHT(port=self.dht_port)
        await self.dht.start()
        
        logger.info("BitTorrent client started")
    
    async def stop(self):
        """Shut down the client and clean up running tasks."""
        # Stop all active downloads
        tasks = []
        for download in list(self.downloads.values()):
            tasks.append(download.stop())
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Stop DHT if running
        if self.dht:
            await self.dht.stop()
        
        logger.info("BitTorrent client stopped")
    
    async def add_torrent(self, torrent_path, download_dir=None):
        """
        Add a torrent to begin downloading.
        
        Args:
            torrent_path: Path to a .torrent file or a magnet link.
            download_dir: Optional override for the save directory.
            
        Returns:
            TorrentDownload instance.
        """
        # Parse the .torrent file and get its metadata
        torrent = Torrent(torrent_path)
        
        # Avoid duplicate downloads
        if torrent.info_hash in self.downloads:
            logger.warning(f"Torrent already downloading: {torrent.name}")
            return self.downloads[torrent.info_hash]
        
        # Kick off download
        download_dir = download_dir or self.download_dir
        download = TorrentDownload(
            torrent, self.peer_id, download_dir, 
            self.port, self.dht
        )
        
        self.downloads[torrent.info_hash] = download
        
        # Start download process
        await download.start()
        
        logger.info(f"Added torrent: {torrent.name}")
        return download
    
    def remove_torrent(self, info_hash, delete_files=False):
        """Stop and remove a torrent from this client."""
        if info_hash in self.downloads:
            download = self.downloads[info_hash]
            asyncio.create_task(download.stop())
            del self.downloads[info_hash]
            
            if delete_files:
                # Optionally remove downloaded files (not implemented yet)
                pass
    
    def get_downloads(self):
        """List all currently active downloads."""
        return list(self.downloads.values())
    
    def get_stats(self):
        """Get summary statistics for this client."""
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
    """Handles downloading and uploading for a single torrent."""
    
    def __init__(self, torrent, peer_id, download_dir, port, dht):
        self.torrent = torrent
        self.peer_id = peer_id
        self.download_dir = download_dir
        self.port = port
        self.dht = dht
        
        # Torrent components
        self.tracker_manager = TrackerManager(torrent, peer_id)
        self.peer_manager = PeerManager(torrent, peer_id)
        self.piece_manager = PieceManager(torrent, download_dir)
        
        # State info
        self.running = False
        self.start_time = None
        self.complete_time = None
        
        # Async tasks
        self.tasks = []
    
    async def start(self):
        """Start downloading this torrent and set up necessary loops."""
        if self.running:
            return
        
        self.running = True
        self.start_time = time.time()
        
        # Start tracker announces
        asyncio.create_task(self._announce_loop())
        
        # Announce on DHT and search for peers
        if self.dht:
            await self.dht.announce_peer(self.torrent.info_hash, self.port)
            asyncio.create_task(self._dht_peer_search())
        
        # Start peer connections
        asyncio.create_task(self._peer_loop())
        
        # Start piece download loop
        asyncio.create_task(self._download_loop())
        
        logger.info(f"Started downloading: {self.torrent.name}")
    
    async def stop(self):
        """Stop downloading this torrent and clean up resources."""
        if not self.running:
            return
        
        self.running = False
        
        # Let trackers know we've stopped
        await self._announce(TrackerEvent.STOPPED)
        
        # Close all peer connections
        await self.peer_manager.close_all()
        
        # Close tracker connections
        self.tracker_manager.close()
        
        logger.info(f"Stopped downloading: {self.torrent.name}")
    
    async def _announce_loop(self):
        """Send periodic updates to trackers."""
        first_announce = True
        
        while self.running:
            try:
                # Figure out which event we're reporting
                if first_announce:
                    event = TrackerEvent.STARTED
                    first_announce = False
                elif self.is_complete() and not self.complete_time:
                    event = TrackerEvent.COMPLETED
                    self.complete_time = time.time()
                else:
                    event = TrackerEvent.NONE
                
                # Perform the announce
                interval = await self._announce(event)
                
                # Wait until the next scheduled announce
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Announce error: {e}")
                await asyncio.sleep(60)  # If there's a problem, try again in a minute
    
    async def _announce(self, event=TrackerEvent.NONE):
        """Send an announce to all trackers and process peer info."""
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
        
        return 1800  # Default is 30 minutes
    
    async def _dht_peer_search(self):
        """Periodically look for peers using DHT."""
        while self.running:
            try:
                # Ask DHT for more peers
                await self.dht.get_peers(self.torrent.info_hash)
                
                # Give the network a moment to respond
                await asyncio.sleep(10)
                
                # Add any new peers found
                dht_peers = self.dht.peers.get(self.torrent.info_hash, set())
                if dht_peers:
                    self.peer_manager.add_peers(list(dht_peers))
                    logger.info(f"Got {len(dht_peers)} peers from DHT")
                
                # Re-announce periodically
                await self.dht.announce_peer(self.torrent.info_hash, self.port)
                
                # Wait before next search
                await asyncio.sleep(300)  # Every 5 minutes
                
            except Exception as e:
                logger.error(f"DHT peer search error: {e}")
                await asyncio.sleep(60)
    
    async def _peer_loop(self):
        """Keep peer connections healthy and up to date."""
        while self.running:
            try:
                # Maintain and refresh connections
                await self.peer_manager.maintain_connections()
                
                # Set up callbacks for new peers
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
        """Main loop for downloading pieces and tracking progress."""
        while self.running and not self.is_complete():
            try:
                # Request blocks from unchoked peers
                for peer_key, peer in list(self.peer_manager.peers.items()):
                    if (peer.connected and peer.handshake_complete and 
                        not peer.peer_choking and peer.am_interested):
                        
                        # Figure out which pieces we can get from this peer
                        peer_pieces = set(peer.get_available_pieces())
                        
                        # Ask for up to 10 blocks at a time
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
                
                # Send bitfield to any newly connected peers
                for peer in self.peer_manager.peers.values():
                    if peer.connected and peer.handshake_complete and not hasattr(peer, '_bitfield_sent'):
                        await peer.send_bitfield(self.piece_manager.bitfield)
                        peer._bitfield_sent = True
                
                # Log current progress
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
        """Handle a piece block received from a peer."""
        # Pass block to the piece manager
        piece_complete = await self.piece_manager.add_block(piece_index, offset, data)
        
        if piece_complete:
            # Let all peers know we've got a new piece
            await self.peer_manager.broadcast_have(piece_index)
    
    async def _on_have_received(self, peer, piece_index):
        """Process HAVE message from a peer, update our interest status."""
        peer_key = (peer.ip, peer.port)
        self.piece_manager.update_peer_pieces(peer_key, have_index=piece_index)
        
        # Decide if we're interested in this peer
        needed_pieces = set(self.piece_manager.get_needed_pieces())
        peer_pieces = set(peer.get_available_pieces())
        
        if needed_pieces & peer_pieces:
            if not peer.am_interested:
                await peer.send_interested()
        else:
            if peer.am_interested:
                await peer.send_not_interested()
    
    async def _on_bitfield_received(self, peer, bitfield):
        """Process BITFIELD message from a peer, update our interest status."""
        peer_key = (peer.ip, peer.port)
        self.piece_manager.update_peer_pieces(peer_key, bitfield=bitfield)
        
        needed_pieces = set(self.piece_manager.get_needed_pieces())
        peer_pieces = set(peer.get_available_pieces())
        
        if needed_pieces & peer_pieces:
            await peer.send_interested()
        else:
            await peer.send_not_interested()
    
    async def _on_request_received(self, peer, piece_index, offset, length):
        """Handle a block request from a peer (if we're not choking them)."""
        if peer.am_choking:
            return
        
        # Try to fetch the requested data and send it
        data = self.piece_manager.get_block_data(piece_index, offset, length)
        if data:
            await peer.send_piece(piece_index, offset, data)
            self.piece_manager.total_uploaded += len(data)
    
    def is_complete(self):
        """Returns True if all pieces are fully downloaded and verified."""
        return self.piece_manager.is_complete()
    
    def get_stats(self):
        """Get statistics about this specific torrent download."""
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
        
        # Try downloading a torrent if one is specified
        if len(sys.argv) > 1:
            torrent_path = sys.argv[1]
            download = await client.add_torrent(torrent_path)
            
            # Print progress info until finished
            while not download.is_complete():
                stats = download.get_stats()
                print(f"\rProgress: {stats['progress']*100:.1f}%", end='', flush=True)
                await asyncio.sleep(5)
            
            print("\nDownload complete!")
        
        await client.stop()
    
    asyncio.run(test_client())
