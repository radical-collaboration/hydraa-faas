#!/usr/bin/env python3
"""
Creates and zips example agent function directories.

Usage:

    cd agent_functions
    python create_agent_functions.py
"""
import os
import shutil
from pathlib import Path


def create_and_zip_function(base_path, name, files):
    """a helper to create, populate, zip, and clean up a function directory."""
    func_dir = base_path / name

    # create the directory and write files
    os.makedirs(func_dir, exist_ok=True)
    for filename, content in files.items():
        with open(func_dir / filename, 'w') as f:
            f.write(content.strip())


def main():
    """main function to create all agent example functions."""
    # this script assumes it is inside the 'agent_functions' directory
    # and will populate this same directory with zip files.
    output_dir = Path(__file__).parent

    print(f"creating agent function zip files in: {output_dir.resolve()}")

    # clean up any existing zip files before creating new ones
    for item in output_dir.glob('*.zip'):
        if item.is_file():
            print(f"removing existing file: {item}")
            item.unlink()

    calc_main = """
import json

def handler(event, context=None):
    a = event.get('a', 0)
    b = event.get('b', 0)
    result = a + b
    return {'statusCode': 200, 'body': json.dumps({'a': a, 'b': b, 'sum': result})}
"""
    create_and_zip_function(output_dir, 'calculator', {'main.py': calc_main})

    text_main = """
import json
import inflect

p = inflect.engine()

def handler(event, context=None):
    text = event.get('text', '')
    word_count = len(text.split())
    return {'statusCode': 200, 'body': json.dumps({
        'message': f'the text has {p.number_to_words(word_count)} words.'
    })}
"""
    text_reqs = 'inflect'
    create_and_zip_function(output_dir, 'text_analyzer', {'main.py': text_main, 'requirements.txt': text_reqs})

    print("\nagent function zip files created successfully.")


if __name__ == "__main__":
    main()
