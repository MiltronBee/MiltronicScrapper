#!/usr/bin/env python3
import os
import glob
import chardet

def check_file_encoding(filepath):
    """Check encoding issues in a file"""
    issues = []
    
    try:
        # Check raw bytes first
        with open(filepath, 'rb') as f:
            raw_content = f.read(1000)  # First 1000 bytes
            
        # Detect encoding
        detected = chardet.detect(raw_content)
        
        # Try to read as UTF-8
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read(500)  # First 500 chars
                
            # Check for common issues
            if '�' in content:
                issues.append(f"replacement_chars: {content.count('�')}")
            
            if detected['encoding'].lower() != 'utf-8':
                issues.append(f"detected_as: {detected['encoding']} (confidence: {detected['confidence']:.2f})")
                
            # Check for suspicious byte sequences that might indicate encoding issues
            suspicious_chars = sum(1 for c in content if ord(c) > 65535 or (ord(c) > 127 and not c.isprintable() and c not in '\n\t\r'))
            if suspicious_chars > 5:
                issues.append(f"suspicious_chars: {suspicious_chars}")
                
        except UnicodeDecodeError as e:
            issues.append(f"utf8_decode_error: {str(e)[:50]}")
            
    except Exception as e:
        issues.append(f"general_error: {str(e)[:50]}")
    
    return issues

# Check a sample of files from different sources
sources_to_check = ['letras_com', 'rss_24_horas', 'rss_el_financiero', 'dof_oficial']
files_with_issues = []

for source in sources_to_check:
    source_dir = f"/root/MiltronicScrapper/data/corpus_raw/{source}"
    if os.path.exists(source_dir):
        txt_files = glob.glob(f"{source_dir}/*.txt")[:5]  # Check first 5 files
        
        for filepath in txt_files:
            issues = check_file_encoding(filepath)
            if issues:
                files_with_issues.append((os.path.basename(filepath), source, issues))

if files_with_issues:
    print("Files with encoding issues found:")
    for filename, source, issues in files_with_issues:
        print(f"\n{source}/{filename}:")
        for issue in issues:
            print(f"  - {issue}")
else:
    print("No encoding issues found in sampled files")

print(f"\nChecked {sum(len(glob.glob(f'/root/MiltronicScrapper/data/corpus_raw/{source}/*.txt')[:5]) for source in sources_to_check if os.path.exists(f'/root/MiltronicScrapper/data/corpus_raw/{source}'))} files total")