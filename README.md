# Parallelised Elasticsearch Transform Tool ✅

**Status: Complete and Ready to Use**

This tool allows you to create multiple parallel Elasticsearch transforms from a single Jinja template. It's useful for processing large datasets by splitting the work across multiple transforms that can run concurrently.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Set up configuration (optional)
cp .env.example .env
# Edit .env with your Elasticsearch URL and API key

# Run with basic template
python parallelised_transform.py --parallelism 12 --template example_transform_template.json

# Test template rendering (without ES connection)
python test_tool.py
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
2. Install dependencies:
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
  --template my_transform_template.json \
  --elasticsearch-url http://localhost:9200 \
  --transform-prefix my_parallel_transform \
  --api-key my-api-key-id:my-api-key-secret \
  --template-vars '{"index_pattern": "logs-*", "date_field": "@timestamp"}' \
  --env-file /path/to/my.env \
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
python parallelised_transform.py --api-key my-api-key-id:my-api-key-secret --parallelism 5 --template my_template.json
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
python parallelised_transform.py --parallelism 5 --template my_template.json
```

### 3. Custom Environment File
```bash
python parallelised_transform.py --env-file /path/to/my-config.env --parallelism 5 --template my_template.json
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

Here's an example Jinja template (`example_transform_template.json`):

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
      }
    }
  },
  "description": "Parallel transform for partition {{ hash }}",
  "frequency": "1m"
}
```

This template will generate transforms that:
1. Filter data based on routing value (using the `hash` variable)
2. Output to different destination indices for each partition
3. Include the partition number in the description

## How It Works

1. **Template Loading**: The tool loads your Jinja template file
2. **Configuration Generation**: For each partition (0 to parallelism-1), it renders the template with the current hash/partition value
3. **Transform Creation**: Each generated configuration is submitted to the Elasticsearch `_transform` API
4. **Error Handling**: The tool reports success/failure for each transform creation

## Use Cases

- **Large Dataset Processing**: Split large transforms into smaller, parallel chunks
- **Routing-based Partitioning**: Use Elasticsearch routing to distribute data processing
- **Time-based Partitioning**: Process different time ranges in parallel
- **Custom Partitioning Logic**: Use any field or combination of fields for partitioning

## Error Handling

The tool provides comprehensive error handling:
- Template file validation
- JSON syntax validation after template rendering
- Elasticsearch API error reporting
- Detailed logging with timestamps

## Examples

### Time-based Partitioning

```json
{
  "source": {
    "index": "logs-*",
    "query": {
      "range": {
        "@timestamp": {
          "gte": "{{ start_date }}",
          "lt": "{{ end_date }}"
        }
      }
    }
  },
  "dest": {
    "index": "processed_logs_{{ hash }}"
  }
}
```

Run with:
```bash
python parallelised_transform.py \
  --parallelism 7 \
  --template time_based_template.json \
  --template-vars '{"start_date": "2023-01-01", "end_date": "2023-01-08"}' \
  --api-key your-api-key-id:your-api-key-secret
```

### User ID Hash-based Partitioning

```json
{
  "source": {
    "index": "user_events",
    "query": {
      "bool": {
        "filter": [
          {
            "script": {
              "script": {
                "source": "doc['user_id'].value.hashCode() % params.total_partitions == params.current_partition",
                "params": {
                  "total_partitions": 12,
                  "current_partition": {{ hash }}
                }
              }
            }
          }
        ]
      }
    }
  }
}
```

## Contributing

Feel free to submit issues and enhancement requests!

## License

MIT License
