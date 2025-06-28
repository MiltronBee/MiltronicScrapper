#!/usr/bin/env python3
"""
RSS Feed Tester - Test RSS feeds individually to debug issues
"""
import sys
import yaml
import logging
from corpus_scraper.rss_manager import RSSManager
from corpus_scraper.config_manager import ConfigManager

def test_rss_feeds():
    """Test all RSS feeds individually"""
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Load configuration
    try:
        config_manager = ConfigManager('config.yaml')
        config = config_manager.get_config()
    except Exception as e:
        print(f"Failed to load config: {e}")
        return
    
    # Initialize RSS manager
    rss_manager = RSSManager(config['politeness'])
    
    print("🔍 Testing RSS feeds individually...")
    print("=" * 60)
    
    working_feeds = []
    failed_feeds = []
    
    # Test each feed
    for source_name, feed_url in rss_manager.mexican_feeds.items():
        print(f"\n📡 Testing: {source_name}")
        print(f"🔗 URL: {feed_url}")
        
        try:
            entries = rss_manager.fetch_feed(feed_url, source_name)
            if entries:
                print(f"✅ SUCCESS: Found {len(entries)} entries")
                print(f"   📰 Sample title: {entries[0]['title'][:80]}...")
                working_feeds.append((source_name, feed_url, len(entries)))
            else:
                print(f"⚠️  WARNING: Feed parsed but no entries found")
                failed_feeds.append((source_name, feed_url, "No entries"))
                
        except Exception as e:
            print(f"❌ FAILED: {e}")
            failed_feeds.append((source_name, feed_url, str(e)))
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 SUMMARY")
    print("=" * 60)
    
    print(f"\n✅ Working feeds ({len(working_feeds)}):")
    for name, url, count in working_feeds:
        print(f"   • {name}: {count} entries")
    
    print(f"\n❌ Failed feeds ({len(failed_feeds)}):")
    for name, url, error in failed_feeds:
        print(f"   • {name}: {error}")
    
    # Test fresh content discovery
    if working_feeds:
        print(f"\n🔄 Testing fresh content discovery...")
        try:
            fresh_content = rss_manager.discover_fresh_content(hours_back=48)
            total_fresh = sum(len(entries) for entries in fresh_content.values())
            print(f"✅ Found {total_fresh} fresh articles from {len(fresh_content)} sources")
            
            # Show sample fresh content
            for source, entries in list(fresh_content.items())[:3]:  # Show first 3 sources
                if entries:
                    print(f"   📰 {source}: {len(entries)} articles")
                    print(f"      - {entries[0]['title'][:60]}...")
                    
        except Exception as e:
            print(f"❌ Fresh content discovery failed: {e}")
    
    rss_manager.close()

if __name__ == "__main__":
    test_rss_feeds()