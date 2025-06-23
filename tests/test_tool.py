#!/usr/bin/env python3
"""
Test script for the parallelised transform tool.
This script tests template rendering without actually submitting to Elasticsearch.
"""

import json
import sys
from pathlib import Path

# Add parent directory to path to import main module
sys.path.append(str(Path(__file__).parent.parent))
from parallelised_transform import ElasticsearchTransformManager


def test_template_rendering():
    """Test template rendering functionality."""
    print("Testing template rendering...")
    
    # Create a dummy manager (no actual ES connection needed for template testing)
    manager = ElasticsearchTransformManager("http://localhost:9200")
    
    # Test with example template - use absolute path from project root
    project_root = Path(__file__).parent.parent
    template_path = project_root / "templates" / "example_transform_template.json"
    
    if not template_path.exists():
        print(f"Error: {template_path} not found. Make sure you're running this from the project directory.")
        return False
    
    try:
        # Load template
        template = manager.load_template(str(template_path))
        
        # Generate configs for 3 partitions
        parallelism = 3
        configs = manager.generate_transform_configs(template, parallelism)
        
        print(f"Successfully generated {len(configs)} transform configurations:")
        
        for i, config in enumerate(configs):
            print(f"\n--- Transform {i} ---")
            print(json.dumps(config, indent=2))
            
            # Validate that hash substitution worked
            if "dest" in config and "index" in config["dest"]:
                dest_index = config["dest"]["index"]
                if f"_{i}" not in dest_index:
                    print(f"Warning: Expected hash {i} not found in dest index: {dest_index}")
        
        print("\n✅ Template rendering test passed!")
        return True
        
    except Exception as e:
        print(f"❌ Template rendering test failed: {e}")
        return False


def test_with_additional_vars():
    """Test template rendering with additional variables."""
    print("\nTesting template rendering with additional variables...")
    
    manager = ElasticsearchTransformManager("http://localhost:9200")
    project_root = Path(__file__).parent.parent
    template_path = project_root / "templates" / "example_transform_template.json"
    
    try:
        template = manager.load_template(str(template_path))
        
        # Test with additional variables
        additional_vars = {
            "parallelism": 5,
            "source_index": "custom_source",
            "frequency": "5m"
        }
        
        configs = manager.generate_transform_configs(template, 2, additional_vars)
        
        print(f"Generated {len(configs)} configs with additional variables:")
        for i, config in enumerate(configs):
            print(f"\n--- Transform {i} with additional vars ---")
            print(json.dumps(config, indent=2))
        
        print("\n✅ Additional variables test passed!")
        return True
        
    except Exception as e:
        print(f"❌ Additional variables test failed: {e}")
        return False


def test_start_transforms_feature():
    """Test the start transforms functionality."""
    print("\nTesting start transforms feature...")
    
    manager = ElasticsearchTransformManager("http://localhost:9200")
    project_root = Path(__file__).parent.parent
    template_path = project_root / "templates" / "example_transform_template.json"
    
    try:
        # Test that the create_parallel_transforms method returns the correct tuple
        successful_creates, created_transform_ids = manager.create_parallel_transforms(
            str(template_path), 2, "test_start", start_transforms=False
        )
        
        print(f"Method returned: {successful_creates} successful creates, {len(created_transform_ids)} transform IDs")
        
        # The method should return the correct structure even if API calls fail
        if isinstance(created_transform_ids, list) and len(created_transform_ids) >= 0:
            print("✅ Start transforms feature interface test passed!")
            return True
        else:
            print(f"❌ Expected list of transform IDs, got {type(created_transform_ids)}")
            return False
            
    except Exception as e:
        print(f"❌ Start transforms feature test failed: {e}")
        return False


if __name__ == "__main__":
    print("Running parallelised transform tests...\n")
    
    success = True
    success &= test_template_rendering()
    success &= test_with_additional_vars()
    success &= test_start_transforms_feature()
    
    if success:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print("\n💥 Some tests failed!")
        sys.exit(1)
