# Parallelised Elasticsearch Transform Tool ✅

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Set up configuration (optional)
cp .env.example .env
# Edit .env with your Elasticsearch URL and API key

# Run with basic template
python parallelised_transform.py --parallelism 12 --template templates/example_transform_template.json

# Test template rendering (without ES connection)
python tests/test_tool.py
```

## Features

- Generate multiple transform configurations from a single Jinja template
- Submit transforms to Elasticsearch API in parallel
- Configurable parallelism level via command-line argument
- Support for Elasticsearch authentication
- Template variable substitution using Jinja2
- Comprehensive logging and error handling

## Installation

1. Clone this repository
2. (Optional) Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Basic Usage

```bash
python parallelised_transform.py --parallelism 12 --template example_transform_template.json
```

### Full Command Line Options

```bash
python parallelised_transform.py \
  --parallelism 12 \
  --template templates/my_transform_template.json \
  --elasticsearch-url http://localhost:9200 \
  --transform-prefix my_parallel_transform \
  --api-key my-api-key-id:my-api-key-secret \
  --template-vars '{"index_pattern": "logs-*", "date_field": "@timestamp"}' \
  --env-file /path/to/my.env \
  --start \
  --overwrite \
  --stop-delay 3 \
  --verbose
```

### Command Line Arguments

- `--parallelism, -p`: **Required**. Number of parallel transforms to create (e.g., 12)
- `--template, -t`: **Required**. Path to Jinja template file containing transform definition
- `--elasticsearch-url, -u`: Elasticsearch cluster URL (default: http://localhost:9200, can be set via .env file)
- `--transform-prefix`: Prefix for transform names (default: parallel_transform)
- `--api-key`: Elasticsearch API key (format: id:key or base64 encoded, can be set via .env file)
- `--template-vars`: JSON string of additional variables to pass to template
- `--env-file`: Path to .env file containing configuration (defaults to .env in current directory)
- `--start`: Automatically start all transforms after creation (only if all were created successfully)
- `--overwrite`: Delete and recreate existing transforms instead of creating new ones
- `--stop-delay`: Seconds to wait after stopping transforms before attempting delete (default: 2)
- `--verbose, -v`: Enable verbose logging

## Template Variables

The tool automatically provides the following variables to your Jinja template:

- `hash`: The partition number (0, 1, 2, ..., parallelism-1)
- `hash_value`: Alias for `hash`
- `partition`: Alias for `hash`
- `partition_id`: Alias for `hash`

You can also pass additional variables using the `--template-vars` argument.

## Authentication

The tool uses Elasticsearch API key authentication. You can provide the API key in several ways:

### 1. Command Line
```bash
python parallelised_transform.py --api-key my-api-key-id:my-api-key-secret --parallelism 5 --template templates/my_template.json
```

### 2. Environment File (.env)
Create a `.env` file in your project directory:
```bash
cp .env.example .env
```

Edit the `.env` file:
```bash
ELASTICSEARCH_URL=https://my-cluster.es.io:9243
ELASTICSEARCH_API_KEY=my-api-key-id:my-api-key-secret
```

Then run without specifying URL or API key:
```bash
python parallelised_transform.py --parallelism 5 --template templates/my_template.json
```

### 3. Custom Environment File
```bash
python parallelised_transform.py --env-file /path/to/my-config.env --parallelism 5 --template templates/my_template.json
```

### API Key Formats Supported

The tool accepts API keys in multiple formats:
- **ID:Secret format**: `my-api-key-id:my-api-key-secret` (automatically base64 encoded)
- **Base64 encoded**: `eW91ci1iYXNlNjQtZW5jb2RlZC1hcGkta2V5`
- **Already formatted**: `ApiKey eW91ci1iYXNlNjQtZW5jb2RlZC1hcGkta2V5`

## Configuration Priority

Configuration values are used in this order of priority:
1. Command line arguments
2. Environment file values (.env or custom file)
3. Default values

## Example Template

Here's an example Jinja template (`templates/example_transform_template.json`):

```json
{
  "source": {
    "index": "source_index",
    "query": {
      "bool": {
        "filter": [
          {
            "term": {
              "_routing": "{{ hash }}"
            }
          }
        ]
      }
    }
  },
  "dest": {
    "index": "dest_index_{{ hash }}"
  },
  "pivot": {
    "group_by": {
      "user_id": {
        "terms": {
          "field": "user_id"
        }
      }
    },
    "aggregations": {
      "total_amount": {
        "sum": {
          "field": "amount"
        }
      },
      "count": {
        "value_count": {
          "field": "transaction_id"
        }
      }
    }
  },
  "description": "Parallel transform for partition {{ hash }} of {{ parallelism | default(1) }}",
  "frequency": "1m",
  "sync": {
    "time": {
      "field": "event.ingested",
      "delay": "60s"
    }
  }
}
```

This template will generate transforms that:
1. Filter data based on routing value (using the `hash` variable)
2. Output to different destination indices for each partition
3. Include the partition number in the description

## Transform Management

### Creating New Transforms

By default, the tool creates new transforms using the Elasticsearch `PUT /_transform/{transform_id}` API:

```bash
# Create 3 new transforms
python3 parallelised_transform.py --parallelism 3 --template templates/netflow_agg_bytes_recv_by_site_v2.json --transform-prefix netflow_site_agg
```

### Updating Existing Transforms

Use the `--overwrite` flag to delete and recreate existing transforms with new configurations:

```bash
# Delete and recreate existing transforms
python3 parallelised_transform.py --parallelism 3 --template templates/netflow_agg_bytes_recv_by_site_v2.json --transform-prefix netflow_site_agg --overwrite
```

**Note**: This will:
1. Stop any running transforms (if they're currently running)
2. Wait for the specified delay (default 2 seconds) to ensure transforms are fully stopped
3. Delete the existing transforms 
4. Recreate them with the new configuration

Any transform state (like checkpoints) will be lost, and transforms will start fresh.

**Tip**: If you're working with transforms that take longer to stop, you can increase the delay:

```bash
# Use a longer delay for transforms that take time to stop
python3 parallelised_transform.py --parallelism 3 --template templates/my_template.json --transform-prefix my_transform --overwrite --stop-delay 5
```

### Starting Transforms Automatically

Use the `--start` flag to automatically start transforms after they are created or updated:

```bash
# Create and start transforms
python3 parallelised_transform.py --parallelism 3 --template templates/netflow_agg_bytes_recv_by_site_v2.json --transform-prefix netflow_site_agg --start

# Update and start transforms
python3 parallelised_transform.py --parallelism 3 --template templates/netflow_agg_bytes_recv_by_site_v2.json --transform-prefix netflow_site_agg --overwrite --start
```

**Note**: Transforms are only started if ALL transforms were created/recreated successfully. This ensures consistency across your parallel transform set.

### Workflow Example

A typical workflow for managing parallel transforms:

```bash
# 1. Initial creation
python3 parallelised_transform.py --parallelism 5 --template templates/my_template.json --transform-prefix my_transform --start --verbose

# 2. Later updates (e.g., after template changes)
python3 parallelised_transform.py --parallelism 5 --template templates/my_template.json --transform-prefix my_transform --overwrite --start --verbose
```

### Elasticsearch API Usage:
- **Create Mode**: `PUT /_transform/{transform_id}` (default behavior)
- **Overwrite Mode**: 
  1. `POST /_transform/{transform_id}/_stop` (stop running transform)
  2. Wait for specified delay (default 2 seconds)
  3. `DELETE /_transform/{transform_id}` (delete stopped transform)
  4. `PUT /_transform/{transform_id}` (create new transform)
- **Start**: `POST /_transform/{transform_id}/_start` (with `--start` flag)

## License

MIT License
