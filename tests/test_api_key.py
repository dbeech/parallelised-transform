#!/usr/bin/env python3
"""
Test API key authentication functionality.
This script tests different API key formats and encoding.
"""

import base64
import sys
from pathlib import Path

# Add parent directory to path to import main module
sys.path.append(str(Path(__file__).parent.parent))
from parallelised_transform import ElasticsearchTransformManager


def test_api_key_formats():
    """Test different API key formats."""
    print("Testing API key authentication formats...")
    
    # Test data
    test_key_id = "test-api-key-id"
    test_key_secret = "test-api-key-secret"
    
    # Format 1: id:secret (should be encoded)
    print("\n1. Testing id:secret format...")
    manager1 = ElasticsearchTransformManager(
        "http://localhost:9200", 
        api_key=f"{test_key_id}:{test_key_secret}"
    )
    auth_header = manager1.session.headers.get('Authorization')
    print(f"   Generated header: {auth_header}")
    
    # Verify it's properly base64 encoded
    if auth_header and auth_header.startswith('ApiKey '):
        encoded_part = auth_header.replace('ApiKey ', '')
        try:
            decoded = base64.b64decode(encoded_part).decode()
            print(f"   Decoded: {decoded}")
            if decoded == f"{test_key_id}:{test_key_secret}":
                print("   ✅ Correctly encoded")
            else:
                print("   ❌ Encoding mismatch")
        except Exception as e:
            print(f"   ❌ Decoding failed: {e}")
    
    # Format 2: Pre-encoded base64
    print("\n2. Testing pre-encoded base64 format...")
    pre_encoded = base64.b64encode(f"{test_key_id}:{test_key_secret}".encode()).decode()
    manager2 = ElasticsearchTransformManager(
        "http://localhost:9200",
        api_key=pre_encoded
    )
    auth_header2 = manager2.session.headers.get('Authorization')
    print(f"   Generated header: {auth_header2}")
    print(f"   Expected: ApiKey {pre_encoded}")
    if auth_header2 == f"ApiKey {pre_encoded}":
        print("   ✅ Correctly formatted")
    else:
        print("   ❌ Format mismatch")
    
    # Format 3: Already formatted with ApiKey prefix
    print("\n3. Testing pre-formatted ApiKey header...")
    formatted_key = f"ApiKey {pre_encoded}"
    manager3 = ElasticsearchTransformManager(
        "http://localhost:9200",
        api_key=formatted_key
    )
    auth_header3 = manager3.session.headers.get('Authorization')
    print(f"   Generated header: {auth_header3}")
    if auth_header3 == formatted_key:
        print("   ✅ Correctly preserved")
    else:
        print("   ❌ Format changed unexpectedly")
    
    # Test no API key
    print("\n4. Testing no API key...")
    manager4 = ElasticsearchTransformManager("http://localhost:9200")
    auth_header4 = manager4.session.headers.get('Authorization')
    print(f"   Generated header: {auth_header4}")
    if auth_header4 is None:
        print("   ✅ No auth header as expected")
    else:
        print("   ❌ Unexpected auth header present")


def test_headers():
    """Test that proper headers are set."""
    print("\nTesting HTTP headers...")
    
    manager = ElasticsearchTransformManager(
        "http://localhost:9200",
        api_key="test-id:test-secret"
    )
    
    headers = manager.session.headers
    print(f"Content-Type: {headers.get('Content-Type')}")
    print(f"Accept: {headers.get('Accept')}")
    print(f"Authorization: {headers.get('Authorization')}")
    
    expected_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    success = True
    for key, expected_value in expected_headers.items():
        if headers.get(key) == expected_value:
            print(f"   ✅ {key} correctly set")
        else:
            print(f"   ❌ {key} incorrect: got {headers.get(key)}, expected {expected_value}")
            success = False
    
    if headers.get('Authorization') and headers.get('Authorization').startswith('ApiKey '):
        print("   ✅ Authorization header correctly formatted")
    else:
        print("   ❌ Authorization header missing or malformed")
        success = False
    
    return success


if __name__ == "__main__":
    print("API Key Authentication Tests")
    print("=" * 50)
    
    try:
        test_api_key_formats()
        success = test_headers()
        
        if success:
            print("\n🎉 All API key tests passed!")
            sys.exit(0)
        else:
            print("\n💥 Some API key tests failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        sys.exit(1)
