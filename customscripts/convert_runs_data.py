import json
import sys

def convert_runs_data(input_data):
    """
    Convert runs data to simplified format.

    Args:
        input_data: List of dictionaries containing run data

    Returns:
        List of dictionaries with accession and is_private fields
    """
    result = []
    for run in input_data:
        result.append({
            'accession': run.get('ACCESSION'),
            'is_private': bool(run.get('IS_PRIVATE'))  # Convert 1/0 to true/false
        })

    return result


if __name__ == '__main__':
    # Check if input file is provided
    if len(sys.argv) < 2:
        print("Usage: python convert_data.py <input_file.json> [output_file.json]")
        print("Example: python convert_data.py input.json output.json")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'output.json'

    try:
        # Read the input file
        print(f"Reading from {input_file}...")
        with open(input_file, 'r') as f:
            input_data = json.load(f)

        # Validate input is a list
        if not isinstance(input_data, list):
            print("Error: Input JSON must be an array")
            sys.exit(1)

        # Convert the data
        print("Converting data...")
        output_data = convert_runs_data(input_data)

        # Write to output file
        print(f"Writing to {output_file}...")
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)

        print(f"✓ Successfully converted {len(output_data)} records")
        print(f"✓ Output saved to {output_file}")

    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: '{input_file}' is not valid JSON")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)