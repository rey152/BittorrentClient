#!/usr/bin/env python3
"""
Command-line interface for the BitTorrent client.
"""

import asyncio
import argparse
import logging
import sys
import os
from pathlib import Path
import json

from client import TorrentClient
from torrent import create_torrent


def setup_logging(verbose=False):
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    
    # Configure root logger
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Quiet some noisy loggers
    if not verbose:
        logging.getLogger('asyncio').setLevel(logging.WARNING)
        logging.getLogger('dht').setLevel(logging.WARNING)


def format_size(size):
    """Format size in bytes to human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"


def format_speed(speed):
    """Format speed in bytes/sec to human readable format."""
    return format_size(speed) + "/s"


async def download_torrent(args):
    """Download a torrent file."""
    client = TorrentClient(
        download_dir=args.download_dir,
        port=args.port
    )
    
    try:
        await client.start()
        
        # Add torrent
        print(f"Loading torrent: {args.torrent}")
        download = await client.add_torrent(args.torrent)
        
        print(f"Torrent name: {download.torrent.name}")
        print(f"Total size: {format_size(download.torrent.total_size)}")
        print(f"Pieces: {download.torrent.num_pieces}")
        print("")
        
        # Monitor progress
        last_progress = -1
        while not download.is_complete():
            stats = download.get_stats()
            progress = int(stats['progress'] * 100)
            
            # Update display
            if progress != last_progress or args.verbose:
                speed = stats.get('download_speed', 0)
                peers = stats.get('peers', 0)
                seeders = stats.get('seeders', 0)
                leechers = stats.get('leechers', 0)
                
                print(f"\rProgress: {progress}% | "
                      f"Speed: {format_speed(speed)} | "
                      f"Peers: {peers} ({seeders}S/{leechers}L) | "
                      f"Downloaded: {format_size(stats['downloaded'])}",
                      end='', flush=True)
                
                last_progress = progress
            
            await asyncio.sleep(1)
        
        print(f"\n\nDownload complete!")
        print(f"Files saved to: {args.download_dir}")
        
        # Continue seeding if requested
        if args.seed:
            print("\nSeeding... Press Ctrl+C to stop")
            try:
                while True:
                    stats = download.get_stats()
                    print(f"\rUploaded: {format_size(stats['uploaded'])} | "
                          f"Peers: {stats['peers']}",
                          end='', flush=True)
                    await asyncio.sleep(5)
            except KeyboardInterrupt:
                print("\nStopping...")
        
    except KeyboardInterrupt:
        print("\n\nDownload cancelled by user")
    except Exception as e:
        print(f"\nError: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
    finally:
        await client.stop()


async def create_torrent_file(args):
    """Create a new torrent file."""
    print(f"Creating torrent for: {args.path}")
    
    # Parse trackers
    trackers = []
    if args.trackers:
        for tracker in args.trackers:
            if ',' in tracker:
                # Multiple trackers in one tier
                trackers.append(tracker.split(','))
            else:
                # Single tracker
                trackers.append([tracker])
    
    try:
        # Create torrent
        torrent = create_torrent(
            args.path,
            piece_length=args.piece_length * 1024,  # Convert KB to bytes
            private=args.private,
            trackers=trackers,
            comment=args.comment
        )
        
        # Save torrent file
        output_path = args.output or f"{Path(args.path).name}.torrent"
        torrent.save(output_path)
        
        print(f"\nTorrent created successfully!")
        print(f"Output file: {output_path}")
        print(f"Info hash: {torrent.info_hash_hex}")
        print(f"Total size: {format_size(torrent.total_size)}")
        print(f"Pieces: {torrent.num_pieces} x {format_size(torrent.piece_length)}")
        
        if args.show_magnet:
            print(f"\nMagnet link:")
            print(torrent.get_magnet_link())
        
    except Exception as e:
        print(f"Error creating torrent: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()


async def show_info(args):
    """Show information about a torrent file."""
    try:
        from torrent import Torrent
        
        torrent = Torrent(args.torrent)
        
        print(f"Torrent Information")
        print(f"==================")
        print(f"Name: {torrent.name}")
        print(f"Info hash: {torrent.info_hash_hex}")
        print(f"Total size: {format_size(torrent.total_size)}")
        print(f"Piece length: {format_size(torrent.piece_length)}")
        print(f"Pieces: {torrent.num_pieces}")
        print(f"Private: {torrent.private}")
        
        if torrent.comment:
            print(f"Comment: {torrent.comment}")
        if torrent.created_by:
            print(f"Created by: {torrent.created_by}")
        if torrent.creation_date:
            print(f"Creation date: {torrent.creation_date}")
        
        print(f"\nFiles ({len(torrent.files)}):")
        for i, file_info in enumerate(torrent.files):
            path = os.path.join(*file_info['path'])
            size = file_info['length']
            print(f"  {i+1}. {path} ({format_size(size)})")
        
        if torrent.announce_list:
            print(f"\nTrackers:")
            for i, tier in enumerate(torrent.announce_list):
                print(f"  Tier {i+1}:")
                for tracker in tier:
                    print(f"    - {tracker}")
        
        if args.show_pieces:
            print(f"\nPiece hashes:")
            for i, piece_hash in enumerate(torrent.piece_hashes):
                print(f"  {i}: {piece_hash.hex()}")
        
        if args.show_magnet:
            print(f"\nMagnet link:")
            print(torrent.get_magnet_link())
            
    except Exception as e:
        print(f"Error reading torrent: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='BitTorrent Client CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download a torrent
  %(prog)s download example.torrent
  
  # Create a torrent
  %(prog)s create /path/to/files -t http://tracker.example.com/announce
  
  # Show torrent info
  %(prog)s info example.torrent
        """
    )
    
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Enable verbose logging')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Download command
    download_parser = subparsers.add_parser('download', help='Download a torrent')
    download_parser.add_argument('torrent', help='Path to .torrent file')
    download_parser.add_argument('-d', '--download-dir', default='downloads',
                                help='Download directory (default: downloads)')
    download_parser.add_argument('-p', '--port', type=int, default=6881,
                                help='Port for peer connections (default: 6881)')
    download_parser.add_argument('-s', '--seed', action='store_true',
                                help='Continue seeding after download completes')
    
    # Create command
    create_parser = subparsers.add_parser('create', help='Create a torrent file')
    create_parser.add_argument('path', help='Path to file or directory')
    create_parser.add_argument('-o', '--output', help='Output torrent file path')
    create_parser.add_argument('-t', '--trackers', nargs='+',
                              help='Tracker URLs (use comma for same tier)')
    create_parser.add_argument('-c', '--comment', help='Torrent comment')
    create_parser.add_argument('-l', '--piece-length', type=int, default=512,
                              help='Piece length in KB (default: 512)')
    create_parser.add_argument('--private', action='store_true',
                              help='Create private torrent')
    create_parser.add_argument('--show-magnet', action='store_true',
                              help='Show magnet link after creation')
    
    # Info command
    info_parser = subparsers.add_parser('info', help='Show torrent information')
    info_parser.add_argument('torrent', help='Path to .torrent file')
    info_parser.add_argument('--show-pieces', action='store_true',
                            help='Show piece hashes')
    info_parser.add_argument('--show-magnet', action='store_true',
                            help='Show magnet link')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Set up logging
    setup_logging(args.verbose)
    
    # Run appropriate command
    if args.command == 'download':
        asyncio.run(download_torrent(args))
    elif args.command == 'create':
        asyncio.run(create_torrent_file(args))
    elif args.command == 'info':
        asyncio.run(show_info(args))


if __name__ == '__main__':
    main()
