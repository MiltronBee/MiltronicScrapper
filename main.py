#!/usr/bin/env python3
"""
Main entry point for the Spanish Corpus Scraping Framework.
Production-grade web scraping for high-quality NLP corpus generation.
"""

import logging
import sys
import argparse
from pathlib import Path
from corpus_scraper.orchestrator import Orchestrator


def setup_logging(log_level: str = "INFO"):
    """Configure comprehensive logging for the framework."""
    # Create logs directory if it doesn't exist
    log_dir = Path("data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Configure logging format
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    # Set up file and console handlers
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.FileHandler(log_dir / "scraper.log", encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Suppress overly verbose external library logs
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("trafilatura").setLevel(logging.WARNING)


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Spanish Corpus Scraping Framework - Enterprise-grade web scraping for NLP"
    )
    parser.add_argument(
        "--config", "-c",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    parser.add_argument(
        "--sources", "-s",
        default="sources.yaml", 
        help="Path to sources file (default: sources.yaml)"
    )
    parser.add_argument(
        "--log-level", "-l",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show current status and exit"
    )
    parser.add_argument(
        "--discover-only",
        action="store_true",
        help="Only discover URLs without processing them"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 60)
        logger.info("Spanish Corpus Scraping Framework v1.0.0")
        logger.info("Enterprise-grade web scraping for NLP corpus generation")
        logger.info("=" * 60)
        
        # Initialize orchestrator
        orchestrator = Orchestrator(args.config, args.sources)
        
        if args.status:
            # Show status and exit
            status = orchestrator.get_status()
            print("\n=== FRAMEWORK STATUS ===")
            print(f"Progress: {status['progress']['overall']}")
            print(f"Corpus: {status['corpus']}")
            print(f"Configuration: {status['configuration']}")
            return 0
        
        if args.discover_only:
            # Only discover URLs
            logger.info("Running URL discovery only")
            discovered_urls = orchestrator.discover_urls()
            new_urls = orchestrator.populate_state(discovered_urls)
            logger.info(f"Discovery complete: {new_urls} new URLs added to queue")
            return 0
        
        # Run complete workflow
        logger.info("Starting complete scraping workflow")
        results = orchestrator.run()
        
        # Print summary
        print("\n" + "=" * 60)
        print("SCRAPING SESSION COMPLETE")
        print("=" * 60)
        print(f"Workflow Duration: {results['workflow_duration_seconds']:.1f} seconds")
        print(f"URLs Discovered: {results['urls_discovered']:,}")
        print(f"URLs Processed: {results['total_processed']:,}")
        print(f"Success Rate: {results['successful']/results['total_processed']*100 if results['total_processed'] > 0 else 0:.1f}%")
        if 'final_corpus_stats' in results:
            print(f"Final Corpus: {results['final_corpus_stats']['total_files']:,} documents")
            print(f"Corpus Size: {results['final_corpus_stats']['total_size_mb']:.1f} MB")
        else:
            print("Final Corpus: Statistics unavailable")
        print("=" * 60)
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        return 1
        
    except Exception as e:
        logger.error(f"Critical error: {e}", exc_info=True)
        return 1
        
    finally:
        # Cleanup
        try:
            orchestrator.cleanup()
        except:
            pass


if __name__ == "__main__":
    sys.exit(main())