# 🇪🇸 Spanish Corpus Scraping Framework

[![Production Ready](https://img.shields.io/badge/Status-Production%20Ready-brightgreen)](https://github.com/your-org/spanish-corpus-scraper)
[![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-blue)](https://python.org)
[![Enterprise Grade](https://img.shields.io/badge/Grade-Enterprise-gold)](https://your-org.com)

A **production-grade**, **modular**, and **scalable** Python framework for scraping high-quality, formal Spanish text from public websites. Designed specifically for generating clean, richly structured text corpora suitable for downstream Natural Language Processing (NLP) tasks, including fine-tuning Large Language Models (LLMs).

## 🚀 Key Features

### 🏗️ **Enterprise Architecture**
- **Modular Design**: Class-based architecture with clear separation of concerns
- **Configuration-Driven**: External YAML configuration for zero-code modifications
- **Thread-Safe**: Production-ready concurrency with thread pool execution
- **Crash-Resistant**: SQLite-based state management with atomic transactions

### 🤖 **Advanced Politeness Protocols**
- **Robots.txt Compliance**: Modern `protego` parser with full RFC 9309 support
- **Intelligent Rate Limiting**: Randomized delays with exponential backoff
- **Realistic Headers**: Full browser header profile rotation for stealth
- **Adaptive Retry Logic**: Tenacity-powered resilience against transient failures

### 🔍 **Multi-Layer Content Extraction**
- **Primary Engine**: `trafilatura` for turnkey boilerplate removal
- **Surgical Fallback**: BeautifulSoup with site-specific CSS selectors
- **HTML Sanitization**: Allowlist-based pre-processing for corpus purity
- **Dynamic Content**: Optional Playwright integration for JavaScript-heavy sites

### 🎯 **Quality Assurance Pipeline**
- **Language Detection**: High-accuracy `fasttext` Spanish validation (F1: 0.97)
- **Content Validation**: spaCy-powered word/sentence counting
- **Deduplication**: SHA-256 content hashing prevents redundant files
- **Atomic Writes**: Corruption-resistant file operations

## 📊 **Target Sources**

| Source | Description | Type |
|--------|-------------|------|
| **DOF** | Diario Oficial de la Federación | Government/Legal |
| **SCJN** | Suprema Corte de Justicia | Judicial/Legal |
| **UNAM** | Universidad Nacional Autónoma | Academic/Research |

## 🛠️ **Installation**

### Prerequisites
- Python 3.9+
- C compiler (for `lxml` compilation)
- 2GB+ available disk space

### Quick Setup
```bash
# Clone the repository
git clone https://github.com/your-org/spanish-corpus-scraper.git
cd spanish-corpus-scraper

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\\Scripts\\activate

# Install dependencies
pip install -r requirements.txt

# Download required models
python -m spacy download es_core_news_sm
playwright install  # Optional: for JavaScript sites
```

### FastText Language Model
```bash
# Download language detection model
wget https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin
```

## 🎮 **Usage**

### Basic Operation
```bash
# Run complete scraping workflow
python main.py

# Show current status
python main.py --status

# Discover URLs only (no processing)
python main.py --discover-only

# Custom configuration
python main.py --config custom_config.yaml --sources custom_sources.yaml
```

### Advanced Options
```bash
# Debug mode with verbose logging
python main.py --log-level DEBUG

# Production mode with minimal output
python main.py --log-level WARNING
```

## ⚙️ **Configuration**

### `config.yaml` - Global Settings
```yaml
politeness:
  request_delay: 2.0      # Base delay between requests
  jitter: 1.0             # Random delay variance
  timeout: 30             # Request timeout
  retry_attempts: 3       # Max retry attempts

validation:
  min_word_count: 200     # Minimum words per document
  required_language: 'es' # Target language
  lang_detect_confidence: 0.90  # Detection threshold

concurrency:
  num_threads: 4          # Worker threads
```

### `sources.yaml` - Target Websites
```yaml
sources:
  - name: 'dof'
    base_url: 'https://dof.gob.mx'
    start_urls:
      - 'https://dof.gob.mx/'
    # Optional: fallback_selector, engine override
```

## 📁 **Project Structure**

```
corpus_scraper/
├── corpus_scraper/          # Core framework modules
│   ├── orchestrator.py      # Central workflow coordinator
│   ├── config_manager.py    # YAML configuration handler
│   ├── scraper.py          # Network requests & politeness
│   ├── extractor.py        # Content extraction pipeline
│   ├── saver.py            # File persistence & deduplication
│   ├── state_manager.py    # SQLite state management
│   └── exceptions.py       # Custom exception classes
├── main.py                 # CLI entry point
├── config.yaml             # Global configuration
├── sources.yaml            # Target website definitions
├── requirements.txt        # Python dependencies
└── data/                   # Output and state data
    ├── corpus_raw/         # Final text corpus
    ├── logs/              # Application logs
    └── state/             # Checkpointing database
```

## 🔄 **Workflow**

1. **URL Discovery**: Fetch sitemaps and crawl start URLs
2. **State Management**: Track processing status in SQLite
3. **Content Extraction**: Multi-layered text extraction pipeline
4. **Quality Validation**: Language detection and content filtering
5. **Deduplication**: SHA-256 hashing prevents duplicate content
6. **Persistence**: Atomic file writes with structured naming

## 📈 **Output Format**

### File Naming Convention
```
SOURCE_YYYYMMDD_HASH.txt
```
- `SOURCE`: Website identifier (dof, scjn, unam)
- `YYYYMMDD`: Processing date
- `HASH`: Content SHA-256 hash (first 16 chars)

### Example Output
```
data/corpus_raw/
├── dof/
│   ├── dof_20241225_a1b2c3d4e5f6g7h8.txt
│   └── dof_20241225_i9j0k1l2m3n4o5p6.txt
├── scjn/
│   └── scjn_20241225_q7r8s9t0u1v2w3x4.txt
└── unam/
    └── unam_20241225_y5z6a7b8c9d0e1f2.txt
```

## 🔍 **Monitoring & Reporting**

The framework includes **executive-level Discord reporting** with:
- Real-time progress updates
- Corpus statistics and quality metrics
- Success/failure rates and processing speeds
- Critical error alerts with actionable information

## 🛡️ **Ethical Considerations**

### ✅ **Responsible Scraping**
- **Robots.txt Compliance**: Strict adherence to webmaster guidelines
- **Rate Limiting**: Respectful request patterns to avoid server overload
- **Public Data Only**: Targets publicly accessible government/academic sources
- **Research Purpose**: Designed for legitimate NLP research applications

### ⚠️ **Important Notes**
- Always verify legal compliance with target websites' Terms of Service
- This tool is intended for academic and research purposes only
- Users are responsible for ensuring ethical and legal usage
- Respect server resources and maintain appropriate request rates

## 🔧 **Technical Specifications**

- **Languages**: Python 3.9+
- **Concurrency**: ThreadPoolExecutor with configurable pool size
- **Database**: SQLite with WAL mode for optimal concurrency
- **Text Processing**: spaCy + fasttext for industrial-grade NLP
- **HTTP Client**: Requests with connection pooling and retry logic
- **Content Extraction**: trafilatura + BeautifulSoup fallback

## 📞 **Support**

For technical support, feature requests, or deployment assistance:
- 📧 **Email**: support@your-org.com
- 📋 **Issues**: [GitHub Issues](https://github.com/your-org/spanish-corpus-scraper/issues)
- 📚 **Documentation**: [Full Documentation](https://docs.your-org.com)

---

**🏆 Built for Excellence** • **Enterprise-Ready** • **Production-Tested**

*Spanish Corpus Scraping Framework - Powering the next generation of NLP research*