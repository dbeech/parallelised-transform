#!/usr/bin/env python3
"""
Example usage scripts for the parallelised transform tool.
These examples show different ways to use the tool with various templates and configurations.
"""

import os
import subprocess
import json
from pathlib import Path


def run_example(description, command, show_output_lines=10):
    """Run an example command and show the results."""
    print(f"\n{'='*60}")
    print(f"Example: {description}")
    print(f"Command: {command}")
    print(f"{'='*60}")
    
    try:
        # Run the command and capture output
        result = subprocess.run(command, shell=True, capture_output=True, text=True, cwd=Path.cwd())
        
        # Show first few lines of output
        output_lines = (result.stdout + result.stderr).split('\n')
        for i, line in enumerate(output_lines[:show_output_lines]):
            if line.strip():
                print(line)
        
        if len(output_lines) > show_output_lines:
            print(f"... (showing first {show_output_lines} lines)")
            
    except Exception as e:
        print(f"Error running example: {e}")


def main():
    """Run various examples of the parallelised transform tool."""
    
    # Use python from PATH (works with virtual envs and system python)
    import shutil
    python_exe = shutil.which('python3') or shutil.which('python')
    if not python_exe:
        print("Error: Python interpreter not found in PATH")
        return
    
    script_path = "parallelised_transform.py"
    
    print("Parallelised Elasticsearch Transform Tool - Examples")
    print("Note: These examples will fail at the API submission stage since Elasticsearch is not running.")
    print("However, they demonstrate the template rendering and configuration generation.")
    
    # Example 1: Basic usage with simple template
    run_example(
        "Basic usage with 5 parallel transforms",
        f"{python_exe} {script_path} --parallelism 5 --template templates/example_transform_template.json --verbose"
    )
    
    # Example 2: Advanced template with custom variables
    advanced_vars = {
        "source_index": "user_events_2024",
        "dest_prefix": "aggregated_events",
        "event_category": "user_activity",
        "event_type": "page_view",
        "group_field": "customer_id",
        "session_field": "session_token",
        "description": "User activity events processing",
        "frequency": "10m",
        "batch_id": "activity2024",
        "total_partitions": 8
    }
    
    run_example(
        "Advanced template with custom variables",
        f"{python_exe} {script_path} --parallelism 8 --template templates/advanced_transform_template.json " +
        f"--template-vars '{json.dumps(advanced_vars)}' --transform-prefix jan2024_events --verbose"
    )
    
    # Example 3: Different Elasticsearch URL and authentication
    run_example(
        "With custom ES URL and API key authentication",
        f"{python_exe} {script_path} --parallelism 3 --template templates/example_transform_template.json " +
        "--elasticsearch-url https://my-cluster.es.io:9200 --api-key my-api-key-id:my-api-key-secret " +
        "--transform-prefix prod_transform"
    )
    
    # Example 4: Using .env file for configuration
    run_example(
        "Using .env file for configuration",
        f"{python_exe} {script_path} --parallelism 4 --template templates/example_transform_template.json " +
        "--env-file .env.example --transform-prefix env_config_test"
    )
    
    # Example 5: Minimal parallelism
    run_example(
        "Minimal parallelism (2 transforms)",
        f"{python_exe} {script_path} --parallelism 2 --template templates/example_transform_template.json " +
        "--transform-prefix minimal_test"
    )
    
    print(f"\n{'='*60}")
    print("Examples completed!")
    print("To use with a real Elasticsearch cluster:")
    print("1. Start your Elasticsearch cluster")
    print("2. Update the --elasticsearch-url parameter")
    print("3. Provide authentication if required")
    print("4. Run the command - the transforms will be created!")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
