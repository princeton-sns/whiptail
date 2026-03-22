#!/usr/bin/env python3
import argparse
import json
import sys
import concurrent.futures
from utils.remote_util import run_remote_command_sync, get_server_host, get_client_host


def get_all_hosts(config):
    hosts = []
    for server_name in config['server_names']:
        hostname = get_server_host(config, server_name)
        hosts.append((hostname, server_name))
    clients = config['clients']
    if isinstance(clients[0], list):
        clients = clients[-1]
    for client_name in clients:
        hostname = get_client_host(config, client_name)
        hosts.append((hostname, client_name))
    return hosts


def append_hosts_file(hostname, username, hosts_content):
    command = f"echo '{hosts_content}' | sudo tee -a /etc/hosts > /dev/null"
    run_remote_command_sync(command, username, hostname)
    return hostname


def main():
    parser = argparse.ArgumentParser(description='Append hosts file to all machines')
    parser.add_argument('--config', required=True, help='Path to JSON config file')
    parser.add_argument('--input', required=True, help='Path to hosts file to append')
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = json.load(f)

    with open(args.input, 'r') as f:
        hosts_content = f.read().strip()

    hosts = get_all_hosts(config)
    username = config['emulab_user']

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(append_hosts_file, hostname, username, hosts_content)
                   for hostname, _ in hosts]
        for future in concurrent.futures.as_completed(futures):
            try:
                hostname = future.result()
                print(f"Updated {hostname}")
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)


if __name__ == '__main__':
    main()
