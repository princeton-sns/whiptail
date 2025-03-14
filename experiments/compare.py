import json
import argparse

def load_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def compare_keys(json1, json2, path=""):
    keys1 = set(json1.keys())
    keys2 = set(json2.keys())

    only_in_json1 = keys1 - keys2
    only_in_json2 = keys2 - keys1

    if only_in_json1:
        print(f"Keys only in first JSON at {path}: {only_in_json1}")
    if only_in_json2:
        print(f"Keys only in second JSON at {path}: {only_in_json2}")

    common_keys = keys1 & keys2
    for key in common_keys:
        if isinstance(json1[key], dict) and isinstance(json2[key], dict):
            compare_keys(json1[key], json2[key], path + f".{key}")
        elif json1[key] != json2[key]:
            print(f"Different values at {path}.{key}: {json1[key]} != {json2[key]}")

def main():
    parser = argparse.ArgumentParser(description="Compare keys of two JSON files")
    parser.add_argument("file1", help="Path to the first JSON file")
    parser.add_argument("file2", help="Path to the second JSON file")
    args = parser.parse_args()

    json1 = load_json(args.file1)
    json2 = load_json(args.file2)

    compare_keys(json1, json2)

if __name__ == "__main__":
    main()