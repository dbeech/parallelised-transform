#!/usr/bin/env python3
"""
Parallelised Elasticsearch Transform Tool

This tool submits transforms to the Elasticsearch API to run them in parallel.
The transform definition is written in a Jinja template with a placeholder for 
a hash value that will be replaced before posting to the API.
"""

import argparse
import json
import sys
import logging
import os
from pathlib import Path
from typing import Dict, Any, List
import requests
from jinja2 import Template, Environment, FileSystemLoader
from urllib.parse import urljoin
from dotenv import load_dotenv


class ElasticsearchTransformManager:
    """Manages parallelised Elasticsearch transforms."""
    
    def __init__(self, elasticsearch_url: str, api_key: str = None):
        """
        Initialize the transform manager.
        
        Args:
            elasticsearch_url: Base URL for Elasticsearch cluster
            api_key: API key for Elasticsearch authentication (format: id:api_key or base64 encoded)
        """
        self.elasticsearch_url = elasticsearch_url.rstrip('/')
        self.session = requests.Session()
        
        if api_key:
            # Handle both base64 encoded and id:key format
            if ':' in api_key and not api_key.startswith('ApiKey '):
                # Convert id:key format to base64
                import base64
                encoded_key = base64.b64encode(api_key.encode()).decode()
                self.session.headers.update({
                    'Authorization': f'ApiKey {encoded_key}'
                })
            elif api_key.startswith('ApiKey '):
                # Already formatted
                self.session.headers.update({
                    'Authorization': api_key
                })
            else:
                # Assume it's already base64 encoded
                self.session.headers.update({
                    'Authorization': f'ApiKey {api_key}'
                })
            
        # Set default headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        self.logger = logging.getLogger(__name__)
    
    def load_template(self, template_path: str) -> Template:
        """
        Load Jinja template from file.
        
        Args:
            template_path: Path to the Jinja template file
            
        Returns:
            Jinja2 Template object
        """
        template_file = Path(template_path)
        if not template_file.exists():
            raise FileNotFoundError(f"Template file not found: {template_path}")
            
        env = Environment(loader=FileSystemLoader(template_file.parent))
        return env.get_template(template_file.name)
    
    def generate_transform_configs(self, template: Template, parallelism: int, 
                                 additional_vars: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Generate transform configurations by replacing placeholders in template.
        
        Args:
            template: Jinja2 template object
            parallelism: Number of parallel transforms to create
            additional_vars: Additional variables to pass to template
            
        Returns:
            List of transform configuration dictionaries
        """
        transforms = []
        base_vars = additional_vars or {}
        
        for i in range(parallelism):
            # Create template variables with hash value
            template_vars = {
                'hash': i,
                'hash_value': i,
                'partition': i,
                'partition_id': i,
                'parallelism': parallelism,
                **base_vars
            }
            
            # Render template with current hash/partition value
            rendered_json = template.render(**template_vars)
            
            try:
                transform_config = json.loads(rendered_json)
                transforms.append(transform_config)
                self.logger.debug(f"Generated transform config for partition {i}")
            except json.JSONDecodeError as e:
                self.logger.error(f"Invalid JSON generated for partition {i}: {e}")
                raise
                
        return transforms
    
    def create_transform(self, transform_id: str, transform_config: Dict[str, Any], overwrite: bool = False, stop_delay_seconds: int = 2) -> bool:
        """
        Create a single transform via Elasticsearch API.
        
        Args:
            transform_id: Unique identifier for the transform
            transform_config: Transform configuration dictionary
            overwrite: If True, delete existing transform before creating new one
            stop_delay_seconds: Seconds to wait after stopping before attempting delete
            
        Returns:
            True if successful, False otherwise
        """
        if overwrite:
            # First delete the existing transform if it exists
            self.delete_transform(transform_id, stop_delay_seconds)
        
        url = urljoin(self.elasticsearch_url, f"_transform/{transform_id}")
        
        try:
            response = self.session.put(url, json=transform_config)
            response.raise_for_status()
            
            action_verb = "Recreated" if overwrite else "Created"
            self.logger.info(f"Successfully {action_verb.lower()} transform: {transform_id}")
            return True
            
        except requests.exceptions.RequestException as e:
            action_verb = "recreate" if overwrite else "create"
            self.logger.error(f"Failed to {action_verb} transform {transform_id}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Response content: {e.response.text}")
            return False
    
    def stop_transform(self, transform_id: str) -> bool:
        """
        Stop a running transform via Elasticsearch API.
        
        Args:
            transform_id: Unique identifier for the transform
            
        Returns:
            True if successful or transform is already stopped/doesn't exist, False on other errors
        """
        url = urljoin(self.elasticsearch_url, f"_transform/{transform_id}/_stop")
        
        try:
            response = self.session.post(url)
            response.raise_for_status()
            
            # Check if the response indicates success
            if response.status_code == 200:
                response_data = response.json()
                if response_data.get('acknowledged', False):
                    self.logger.info(f"Successfully stopped transform: {transform_id}")
                    return True
                else:
                    self.logger.warning(f"Transform stop not acknowledged for: {transform_id}")
                    return False
            
            return True
            
        except requests.exceptions.RequestException as e:
            # If transform doesn't exist (404) or is already stopped, that's fine for our use case
            if hasattr(e, 'response') and e.response is not None:
                if e.response.status_code == 404:
                    self.logger.debug(f"Transform {transform_id} does not exist (404) - skipping stop")
                    return True
                elif e.response.status_code == 409:
                    # 409 typically means transform is already stopped or stopping
                    self.logger.debug(f"Transform {transform_id} is already stopped or stopping (409)")
                    return True
            
            self.logger.error(f"Failed to stop transform {transform_id}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Response content: {e.response.text}")
            return False

    def delete_transform(self, transform_id: str, stop_delay_seconds: int = 2) -> bool:
        """
        Delete a transform via Elasticsearch API.
        First stops the transform if it's running, then deletes it.
        
        Args:
            transform_id: Unique identifier for the transform
            stop_delay_seconds: Seconds to wait after stopping before attempting delete
            
        Returns:
            True if successful or transform doesn't exist, False on other errors
        """
        # First, try to stop the transform if it's running
        if not self.stop_transform(transform_id):
            self.logger.warning(f"Could not stop transform {transform_id}, attempting delete anyway")
        else:
            # Wait for the transform to fully stop before attempting delete
            import time
            self.logger.debug(f"Waiting {stop_delay_seconds} seconds for transform {transform_id} to stop completely...")
            time.sleep(stop_delay_seconds)
        
        url = urljoin(self.elasticsearch_url, f"_transform/{transform_id}")
        
        try:
            response = self.session.delete(url)
            response.raise_for_status()
            
            self.logger.info(f"Successfully deleted transform: {transform_id}")
            return True
            
        except requests.exceptions.RequestException as e:
            # If transform doesn't exist (404), that's fine for our use case
            if hasattr(e, 'response') and e.response is not None and e.response.status_code == 404:
                self.logger.debug(f"Transform {transform_id} does not exist (404) - skipping delete")
                return True
            
            self.logger.error(f"Failed to delete transform {transform_id}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Response content: {e.response.text}")
            return False
    
    def start_transform(self, transform_id: str) -> bool:
        """
        Start a single transform via Elasticsearch API.
        
        Args:
            transform_id: Unique identifier for the transform
            
        Returns:
            True if successful, False otherwise
        """
        url = urljoin(self.elasticsearch_url, f"_transform/{transform_id}/_start")
        
        try:
            response = self.session.post(url)
            response.raise_for_status()
            
            self.logger.info(f"Successfully started transform: {transform_id}")
            return True
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to start transform {transform_id}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Response content: {e.response.text}")
            return False
    
    def list_transforms_with_prefix(self, prefix: str) -> List[str]:
        """
        List all transforms that start with the given prefix.
        
        Args:
            prefix: Transform name prefix to search for
            
        Returns:
            List of transform IDs that match the prefix
        """
        url = urljoin(self.elasticsearch_url, "_transform")
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            
            data = response.json()
            transforms = data.get('transforms', [])
            
            # Filter transforms that start with our prefix
            matching_transforms = []
            for transform in transforms:
                transform_id = transform.get('id', '')
                if transform_id.startswith(prefix):
                    matching_transforms.append(transform_id)
            
            self.logger.debug(f"Found {len(matching_transforms)} transforms with prefix '{prefix}'")
            return matching_transforms
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to list transforms: {e}")
            return []

    def cleanup_excess_transforms(self, transform_name_prefix: str, current_parallelism: int, 
                                stop_delay_seconds: int = 2) -> List[str]:
        """
        Clean up transforms that exceed the current parallelism level.
        
        Args:
            transform_name_prefix: Prefix used for transform names
            current_parallelism: Current parallelism level (0 to parallelism-1)
            stop_delay_seconds: Delay between stop and delete operations
            
        Returns:
            List of transform IDs that were cleaned up
        """
        self.logger.info(f"Checking for excess transforms beyond parallelism {current_parallelism}")
        
        # Get all transforms with this prefix
        existing_transforms = self.list_transforms_with_prefix(transform_name_prefix)
        
        # Identify transforms that should be removed (index >= current_parallelism)
        excess_transforms = []
        expected_pattern = f"{transform_name_prefix}_"
        
        for transform_id in existing_transforms:
            if transform_id.startswith(expected_pattern):
                # Extract the suffix number
                suffix = transform_id[len(expected_pattern):]
                try:
                    index = int(suffix)
                    if index >= current_parallelism:
                        excess_transforms.append(transform_id)
                except ValueError:
                    # Suffix is not a number, skip
                    self.logger.debug(f"Skipping transform with non-numeric suffix: {transform_id}")
                    continue
        
        if not excess_transforms:
            self.logger.info("No excess transforms found")
            return []
        
        self.logger.info(f"Found {len(excess_transforms)} excess transforms to clean up: {excess_transforms}")
        
        # Clean up excess transforms
        cleaned_up = []
        for transform_id in excess_transforms:
            try:
                self.logger.info(f"Cleaning up excess transform: {transform_id}")
                if self.delete_transform(transform_id, stop_delay_seconds):
                    cleaned_up.append(transform_id)
                    self.logger.info(f"Successfully cleaned up excess transform: {transform_id}")
                else:
                    self.logger.warning(f"Failed to clean up excess transform: {transform_id}")
            except Exception as e:
                self.logger.error(f"Error cleaning up transform {transform_id}: {e}")
        
        self.logger.info(f"Cleaned up {len(cleaned_up)} excess transforms")
        return cleaned_up

    def create_parallel_transforms(self, template_path: str, parallelism: int,
                                 transform_name_prefix: str, additional_vars: Dict[str, Any] = None, 
                                 start_transforms: bool = False, overwrite: bool = False, 
                                 stop_delay_seconds: int = 2, cleanup_excess: bool = False) -> tuple[int, List[str]]:
        """
        Create multiple parallel transforms from template.
        
        Args:
            template_path: Path to Jinja template file
            parallelism: Number of parallel transforms to create
            transform_name_prefix: Prefix for transform names
            additional_vars: Additional variables for template rendering
            start_transforms: Whether to start transforms after creation (only if all created successfully)
            overwrite: Whether to delete and recreate existing transforms
            stop_delay_seconds: Seconds to wait after stopping before attempting delete (when overwriting)
            cleanup_excess: Whether to clean up transforms that exceed current parallelism
            
        Returns:
            Tuple of (number of successfully created transforms, list of created transform IDs)
        """
        action_verb = "Recreating" if overwrite else "Creating"
        self.logger.info(f"{action_verb} {parallelism} parallel transforms from template: {template_path}")
        
        # Clean up excess transforms if requested (do this first)
        cleaned_up_transforms = []
        if cleanup_excess:
            try:
                cleaned_up_transforms = self.cleanup_excess_transforms(
                    transform_name_prefix, parallelism, stop_delay_seconds
                )
            except Exception as e:
                self.logger.error(f"Failed to clean up excess transforms: {e}")
        
        # Load template
        template = self.load_template(template_path)
        
        # Generate transform configurations
        transform_configs = self.generate_transform_configs(template, parallelism, additional_vars)
        
        # Create/Update transforms
        successful_creates = 0
        created_transform_ids = []
        
        for i, config in enumerate(transform_configs):
            transform_id = f"{transform_name_prefix}_{i}"
            if self.create_transform(transform_id, config, overwrite, stop_delay_seconds):
                successful_creates += 1
                created_transform_ids.append(transform_id)
        
        action_verb = "recreated" if overwrite else "created"
        if cleaned_up_transforms:
            self.logger.info(f"Successfully {action_verb} {successful_creates}/{parallelism} transforms, "
                           f"cleaned up {len(cleaned_up_transforms)} excess transforms")
        else:
            self.logger.info(f"Successfully {action_verb} {successful_creates}/{parallelism} transforms")
        
        # Start transforms if requested and all were created/recreated successfully
        if start_transforms and successful_creates == parallelism:
            action_verb = "recreated" if overwrite else "created"
            self.logger.info(f"All transforms {action_verb} successfully. Starting transforms...")
            started_count = 0
            
            for transform_id in created_transform_ids:
                if self.start_transform(transform_id):
                    started_count += 1
            
            self.logger.info(f"Successfully started {started_count}/{len(created_transform_ids)} transforms")
            
            if started_count != len(created_transform_ids):
                self.logger.warning("Not all transforms were started successfully")
        
        elif start_transforms and successful_creates < parallelism:
            action_verb = "recreated" if overwrite else "created"
            self.logger.warning(f"Skipping transform start because only {successful_creates}/{parallelism} transforms were {action_verb} successfully")
        
        return successful_creates, created_transform_ids


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def load_config_from_env(env_file: str = None) -> Dict[str, str]:
    """
    Load configuration from .env file.
    
    Args:
        env_file: Path to .env file (optional, defaults to .env in current directory)
        
    Returns:
        Dictionary of configuration values
    """
    # Load .env file if it exists
    if env_file:
        load_dotenv(env_file)
    else:
        load_dotenv()  # This will look for .env in current directory
    
    config = {}
    
    # Load Elasticsearch configuration from environment variables
    if os.getenv('ELASTICSEARCH_URL'):
        config['elasticsearch_url'] = os.getenv('ELASTICSEARCH_URL')
    
    if os.getenv('ELASTICSEARCH_API_KEY'):
        config['api_key'] = os.getenv('ELASTICSEARCH_API_KEY')
    
    return config


def main():
    """Main entry point for the CLI application."""
    parser = argparse.ArgumentParser(
        description="Create parallelised Elasticsearch transforms from Jinja templates",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        '--parallelism', '-p',
        type=int,
        required=True,
        help='Number of parallel transforms to create'
    )
    
    parser.add_argument(
        '--template', '-t',
        type=str,
        required=True,
        help='Path to Jinja template file containing transform definition'
    )
    
    parser.add_argument(
        '--elasticsearch-url', '-u',
        type=str,
        default='http://localhost:9200',
        help='Elasticsearch cluster URL (can also be set via ELASTICSEARCH_URL in .env file)'
    )
    
    parser.add_argument(
        '--transform-prefix',
        type=str,
        default='parallel_transform',
        help='Prefix for transform names'
    )
    
    parser.add_argument(
        '--api-key',
        type=str,
        help='Elasticsearch API key (format: id:key or base64 encoded, can also be set via ELASTICSEARCH_API_KEY in .env file)'
    )
    
    parser.add_argument(
        '--env-file',
        type=str,
        help='Path to .env file containing configuration (defaults to .env in current directory)'
    )
    
    parser.add_argument(
        '--template-vars',
        type=str,
        help='JSON string of additional variables to pass to template'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--start',
        action='store_true',
        help='Automatically start all transforms after creation (only if all were created successfully)'
    )
    
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='Delete and recreate existing transforms instead of creating new ones'
    )
    
    parser.add_argument(
        '--stop-delay',
        type=int,
        default=2,
        help='Seconds to wait after stopping transforms before attempting delete (default: 2)'
    )
    
    parser.add_argument(
        '--cleanup-excess',
        action='store_true',
        help='Clean up transforms that exceed the current parallelism level'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)
    
    # Load configuration from .env file
    env_config = load_config_from_env(args.env_file)
    
    # Use command line args with fallback to env config, then defaults
    elasticsearch_url = args.elasticsearch_url
    if not elasticsearch_url or elasticsearch_url == 'http://localhost:9200':
        elasticsearch_url = env_config.get('elasticsearch_url', args.elasticsearch_url)
    
    api_key = args.api_key or env_config.get('api_key')
    
    # Validate arguments
    if args.parallelism <= 0:
        logger.error("Parallelism must be a positive integer")
        sys.exit(1)
    
    if args.stop_delay < 0:
        logger.error("Stop delay must be a non-negative integer")
        sys.exit(1)
    
    if not Path(args.template).exists():
        logger.error(f"Template file not found: {args.template}")
        sys.exit(1)
    
    # Parse additional template variables
    additional_vars = {}
    if args.template_vars:
        try:
            additional_vars = json.loads(args.template_vars)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in template-vars: {e}")
            sys.exit(1)
    
    try:
        # Create transform manager
        manager = ElasticsearchTransformManager(
            elasticsearch_url=elasticsearch_url,
            api_key=api_key
        )
        
        # Create parallel transforms
        successful_creates, created_transform_ids = manager.create_parallel_transforms(
            template_path=args.template,
            parallelism=args.parallelism,
            transform_name_prefix=args.transform_prefix,
            additional_vars=additional_vars,
            start_transforms=args.start,
            overwrite=args.overwrite,
            stop_delay_seconds=args.stop_delay,
            cleanup_excess=args.cleanup_excess
        )
        
        action_verb = "recreated" if args.overwrite else "created"
        if successful_creates == args.parallelism:
            if args.start:
                logger.info(f"All transforms {action_verb} and start requested!")
            else:
                logger.info(f"All transforms {action_verb} successfully!")
            sys.exit(0)
        else:
            logger.warning(f"Only {successful_creates}/{args.parallelism} transforms {action_verb} successfully")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
