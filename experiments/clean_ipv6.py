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


def clean_hosts_file(hostname, username):
    command = "sudo sed -i '/^fe80::/d' /etc/hosts"
    run_remote_command_sync(command, username, hostname)
    return hostname


def main():
    parser = argparse.ArgumentParser(description='Clean IPv6 entries from /etc/hosts on all machines')
    parser.add_argument('--config', required=True, help='Path to JSON config file')
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = json.load(f)

    hosts = get_all_hosts(config)
    username = config['emulab_user']

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(clean_hosts_file, hostname, username)
                   for hostname, _ in hosts]
        for future in concurrent.futures.as_completed(futures):
            try:
                hostname = future.result()
                print(f"Cleaned {hostname}")
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)


if __name__ == '__main__':
    main()
