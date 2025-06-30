#!/usr/bin/env python3
"""
Enhanced main entry point for high-yield Mexican Spanish corpus harvesting.
Runs the complete enhanced framework with aggressive data collection strategies.
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime
import json

# Add corpus_scraper to path
sys.path.insert(0, str(Path(__file__).parent / 'corpus_scraper'))

from corpus_scraper.high_yield_orchestrator import HighYieldOrchestrator
from corpus_scraper.config_manager import ConfigManager


def setup_enhanced_logging(log_level: str = "INFO", log_file: str = None):
    """Setup enhanced logging configuration."""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Add file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(log_format))
        logging.getLogger().addHandler(file_handler)
    
    # Reduce noise from third-party libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('prawcore').setLevel(logging.WARNING)
    logging.getLogger('playwright').setLevel(logging.WARNING)


def print_harvest_summary(results: dict):
    """Print a comprehensive harvest summary."""
    print("\n" + "="*80)
    print("🚀 HIGH-YIELD MEXICAN SPANISH CORPUS HARVEST COMPLETE")
    print("="*80)
    
    if not results.get('success', False):
        print(f"❌ HARVEST FAILED: {results.get('error', 'Unknown error')}")
        return
    
    # Basic metrics
    duration_hours = results['workflow_duration_seconds'] / 3600
    print(f"⏱️  Total Duration: {duration_hours:.1f} hours")
    print(f"🔍 URLs Discovered: {results['urls_discovered']:,}")
    print(f"➕ New URLs Added: {results['new_urls_added']:,}")
    
    # Session results
    session = results.get('session_results', {})
    if session:
        print(f"\n📊 PROCESSING SESSION RESULTS:")
        print(f"   • URLs Processed: {session.get('processed_urls', 0):,}")
        print(f"   • Successful Extractions: {session.get('successful_extractions', 0):,}")
        print(f"   • Success Rate: {session.get('success_rate', 0):.1f}%")
        print(f"   • Processing Rate: {session.get('processing_rate', 0):.1f} URLs/second")
        
        # Token metrics
        total_tokens = session.get('total_tokens_collected', 0)
        token_rate = session.get('token_rate', 0)
        print(f"\n🪙 TOKEN COLLECTION:")
        print(f"   • Total Tokens: {total_tokens:,}")
        print(f"   • Token Rate: {token_rate:,.0f} tokens/second")
        print(f"   • Target Progress: {(total_tokens / 1000000000) * 100:.2f}% of 1B tokens")
    
    # Corpus statistics
    corpus = results.get('corpus_stats', {})
    if corpus:
        print(f"\n📚 CORPUS STATISTICS:")
        print(f"   • Total Files: {corpus.get('total_files', 0):,}")
        print(f"   • Total Size: {corpus.get('total_size_gb', 0):.2f} GB")
        print(f"   • Total Tokens: {corpus.get('total_tokens', 0):,}")
        print(f"   • Unique Content Hashes: {corpus.get('unique_hashes', 0):,}")
        print(f"   • HTML Snapshots: {corpus.get('snapshots', 0):,}")
    
    # Source breakdown
    source_breakdown = session.get('source_breakdown', {})
    if source_breakdown:
        print(f"\n🎯 TOP PERFORMING SOURCES:")
        sorted_sources = sorted(
            source_breakdown.items(),
            key=lambda x: x[1].get('tokens', 0),
            reverse=True
        )
        
        for i, (source, stats) in enumerate(sorted_sources[:10], 1):
            tokens = stats.get('tokens', 0)
            success_rate = (stats.get('successful', 0) / stats.get('processed', 1)) * 100
            print(f"   {i:2d}. {source}: {tokens:,} tokens ({success_rate:.0f}% success)")
    
    # Progress toward goals
    progress = results.get('final_progress_stats', {})
    if progress and 'tokens' in progress:
        token_progress = progress['tokens'].get('progress_percentage', 0)
        print(f"\n🎯 OVERALL PROGRESS:")
        print(f"   • Target Achievement: {token_progress:.2f}% of 1 billion tokens")
        
        if token_progress > 0:
            estimated_sessions = 100 / token_progress if token_progress > 0 else float('inf')
            print(f"   • Estimated Sessions to Goal: {estimated_sessions:.0f}")
    
    print("\n" + "="*80)
    print("💾 Data saved to: data/corpus_raw/")
    print("📊 Full metrics available in logs")
    print("="*80)


def run_discovery_only(orchestrator: HighYieldOrchestrator):
    """Run URL discovery only without processing."""
    print("🔍 Running discovery-only mode...")
    
    discovered_urls = orchestrator.discover_all_content()
    total_urls = sum(len(urls) for urls in discovered_urls.values())
    
    print(f"\n📋 DISCOVERY RESULTS:")
    print(f"   • Total Sources: {len(discovered_urls)}")
    print(f"   • Total URLs: {total_urls:,}")
    
    for source_name, urls in sorted(discovered_urls.items(), key=lambda x: len(x[1]), reverse=True):
        print(f"   • {source_name}: {len(urls):,} URLs")
    
    return discovered_urls


def run_status_check(orchestrator: HighYieldOrchestrator):
    """Check current harvest status."""
    print("📊 Checking harvest status...")
    
    status = orchestrator.get_harvest_status()
    
    print(f"\n🕐 SESSION INFO:")
    print(f"   • Session ID: {status['session_id']}")
    print(f"   • Duration: {status['session_duration']/3600:.1f} hours")
    
    # Progress stats
    progress = status.get('progress_stats', {})
    if progress and 'overall' in progress:
        overall = progress['overall']
        print(f"\n📈 PROCESSING PROGRESS:")
        print(f"   • Total URLs: {overall.get('total', 0):,}")
        print(f"   • Pending: {overall.get('pending', 0):,}")
        print(f"   • Completed: {overall.get('completed', 0):,}")
        print(f"   • Failed: {overall.get('failed', 0):,}")
        print(f"   • Success Rate: {overall.get('success_rate', 0):.1f}%")
    
    # Token stats
    if 'tokens' in progress:
        tokens = progress['tokens']
        print(f"\n🪙 TOKEN COLLECTION:")
        print(f"   • Total Tokens: {tokens.get('total_tokens', 0):,}")
        print(f"   • Progress: {tokens.get('progress_percentage', 0):.2f}% of target")
    
    # Corpus stats
    corpus = status.get('corpus_stats', {})
    if corpus:
        print(f"\n📚 CORPUS STATUS:")
        print(f"   • Files: {corpus.get('total_files', 0):,}")
        print(f"   • Size: {corpus.get('total_size_gb', 0):.2f} GB")
        print(f"   • Snapshots: {corpus.get('snapshots', 0):,}")


def main():
    """Enhanced main function with comprehensive argument handling."""
    parser = argparse.ArgumentParser(
        description="High-Yield Mexican Spanish Corpus Harvester",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Run complete harvest
  %(prog)s --discovery-only                   # Discover URLs only
  %(prog)s --status                          # Check current status
  %(prog)s --target-tokens 100000000         # Target 100M tokens
  %(prog)s --duration 4                      # Run for 4 hours
  %(prog)s --config config_enhanced.yaml     # Use enhanced config
        """
    )
    
    # Configuration options
    parser.add_argument(
        '--config', 
        default='config_enhanced.yaml',
        help='Configuration file path (default: config_enhanced.yaml)'
    )
    parser.add_argument(
        '--sources',
        default='sources_enhanced.yaml', 
        help='Sources file path (default: sources_enhanced.yaml)'
    )
    
    # Operation modes
    parser.add_argument(
        '--discovery-only',
        action='store_true',
        help='Run URL discovery only, no processing'
    )
    parser.add_argument(
        '--status',
        action='store_true',
        help='Show current harvest status and exit'
    )
    
    # Session parameters
    parser.add_argument(
        '--target-tokens',
        type=int,
        default=50000000,
        help='Target token count for session (default: 50M)'
    )
    parser.add_argument(
        '--duration',
        type=float,
        default=2.0,
        help='Maximum session duration in hours (default: 2.0)'
    )
    
    # Logging options
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Log level (default: INFO)'
    )
    parser.add_argument(
        '--log-file',
        help='Log file path (optional)'
    )
    
    # Output options
    parser.add_argument(
        '--save-results',
        help='Save results to JSON file'
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Reduce output verbosity'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_enhanced_logging(args.log_level, args.log_file)
    logger = logging.getLogger(__name__)
    
    if not args.quiet:
        print("🇲🇽 HIGH-YIELD MEXICAN SPANISH CORPUS HARVESTER")
        print("=" * 50)
        print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⚙️  Config: {args.config}")
        print(f"📋 Sources: {args.sources}")
        print()
    
    try:
        # Initialize enhanced orchestrator
        logger.info("Initializing high-yield orchestrator...")
        orchestrator = HighYieldOrchestrator(args.config, args.sources)
        
        if args.status:
            # Status check mode
            run_status_check(orchestrator)
            return 0
            
        elif args.discovery_only:
            # Discovery-only mode
            discovered_urls = run_discovery_only(orchestrator)
            
            if args.save_results:
                with open(args.save_results, 'w', encoding='utf-8') as f:
                    json.dump({
                        'discovery_results': {k: v[:100] for k, v in discovered_urls.items()},  # Limit for JSON size
                        'total_urls': sum(len(urls) for urls in discovered_urls.values()),
                        'timestamp': datetime.now().isoformat()
                    }, f, indent=2, ensure_ascii=False)
                print(f"📁 Results saved to: {args.save_results}")
            
            return 0
        
        else:
            # Full harvest mode
            logger.info("Starting complete high-yield harvest...")
            
            # Run complete harvest
            results = orchestrator.run_complete_harvest()
            
            # Print summary unless quiet
            if not args.quiet:
                print_harvest_summary(results)
            
            # Save results if requested
            if args.save_results:
                with open(args.save_results, 'w', encoding='utf-8') as f:
                    json.dump(results, f, indent=2, ensure_ascii=False, default=str)
                print(f"📁 Detailed results saved to: {args.save_results}")
            
            return 0 if results.get('success', False) else 1
    
    except KeyboardInterrupt:
        logger.info("Harvest interrupted by user")
        print("\n⏹️  Harvest interrupted by user")
        return 130
    
    except Exception as e:
        logger.error(f"Harvest failed with error: {e}", exc_info=True)
        print(f"\n❌ HARVEST FAILED: {e}")
        return 1
    
    finally:
        try:
            if 'orchestrator' in locals():
                orchestrator.cleanup()
        except:
            pass


if __name__ == "__main__":
    sys.exit(main())