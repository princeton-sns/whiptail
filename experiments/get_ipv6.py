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


def fetch_ipv6_and_interface(hostname, machine_name, subnet, username):
    """Fetch IPv6 address and interface name for a given subnet."""
    # Find interface with IP in the subnet using ip command with CIDR notation
    command = f"ip -4 addr show | grep 'inet ' | grep '{subnet}' | head -1 | awk '{{print $NF}}'"
    interface_result = run_remote_command_sync(command, username, hostname)
    interface = interface_result.strip() if interface_result else ""

    if not interface:
        return None, None

    # Get IPv6 link-local address from that interface
    ipv6_command = f"ip -6 addr show {interface} | grep 'inet6 fe80' | awk '{{print $2}}' | cut -d'/' -f1 | head -1"
    ipv6_result = run_remote_command_sync(ipv6_command, username, hostname)
    ipv6 = ipv6_result.strip() if ipv6_result else ""

    return ipv6, interface


def main():
    parser = argparse.ArgumentParser(description='Get IPv6 addresses from config machines')
    parser.add_argument('--config', required=True, help='Path to JSON config file')
    parser.add_argument('--subnet', required=True, help='Subnet to match (e.g., 10.10.1)')
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = json.load(f)

    hosts = get_all_hosts(config)
    username = config['emulab_user']

    results = []
    interface_map = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        future_to_host = {
            executor.submit(fetch_ipv6_and_interface, hostname, machine_name, args.subnet, username): (hostname, machine_name)
            for hostname, machine_name in hosts
        }
        for future in concurrent.futures.as_completed(future_to_host):
            hostname, machine_name = future_to_host[future]
            try:
                ipv6, interface = future.result()
                if ipv6 and interface:
                    results.append((ipv6, machine_name))
                    interface_map[machine_name] = interface
                else:
                    print(f"No IPv6/interface found for {machine_name} ({hostname})", file=sys.stderr)
            except Exception as e:
                print(f"Error fetching from {hostname}: {e}", file=sys.stderr)

    print(f"\nTotal results: {len(results)}", file=sys.stderr)

    # Write hosts.txt
    with open('experiments/hosts.txt', 'w') as f:
        for ipv6, machine_name in sorted(results, key=lambda x: x[1]):
            f.write(f"{ipv6} {machine_name}\n")
    print("Written experiments/hosts.txt", file=sys.stderr)

    # Write interface mapping to JSON file
    with open('experiments/ipv6_interfaces.json', 'w') as f:
        json.dump(interface_map, f, indent=2)
    print("Written experiments/ipv6_interfaces.json", file=sys.stderr)


if __name__ == '__main__':
    main()
