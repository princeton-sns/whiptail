#!/usr/bin/env python3
import json
import os
import socket
import subprocess
import re
import matplotlib.pyplot as plt
import sys
import argparse

DEFAULT_TIMEOUT = 3  # seconds for SSH connection timeout

def read_config(config_file):
    """Read the JSON configuration file."""
    with open(config_file, "r") as f:
        config = json.load(f)
    return config

def test_connectivity(hostname, ip):
    """
    Attempt to resolve the hostname.
    If resolution is successful, return hostname; otherwise, return ip.
    """
    try:
        socket.gethostbyname(hostname)
        return hostname
    except socket.error:
        return ip

def build_ssh_command(target, config, remote_cmd=None):
    """
    Build an SSH command list with a connection timeout option.
    If remote_cmd is provided, append it to the SSH command.
    """
    base_cmd = []
    auth_method = config["auth"].lower()
    username = config["user"]
    # Add the ConnectTimeout option to the SSH command
    timeout_option = ["-o", f"ConnectTimeout={DEFAULT_TIMEOUT}"]
    if auth_method == "publickey":
        if config.get("keypath"):
            base_cmd = ["ssh", "-i", config["keypath"]] + timeout_option + [f"{username}@{target}"]
        else:
            base_cmd = ["ssh"] + timeout_option + [f"{username}@{target}"]
    elif auth_method == "password":
        # Use sshpass for password authentication
        base_cmd = ["sshpass", "-p", config["password"], "ssh"] + timeout_option + [f"{username}@{target}"]
    else:
        raise ValueError("Unknown authentication method")
    if remote_cmd:
        base_cmd.append(remote_cmd)
    return base_cmd

def build_scp_command(source, target, config, remote_path="."):
    """
    Build an SCP command list to copy the source file to the target's remote_path.
    """
    username = config["user"]
    auth_method = config["auth"].lower()
    remote_target = f"{username}@{target}:{remote_path}"
    timeout_option = ["-o", f"ConnectTimeout={DEFAULT_TIMEOUT}"]
    if auth_method == "publickey":
        if config.get("keypath"):
            cmd = ["scp", "-i", config["keypath"]] + timeout_option + [source, remote_target]
        else:
            cmd = ["scp"] + timeout_option + [source, remote_target]
    elif auth_method == "password":
        cmd = ["sshpass", "-p", config["password"], "scp"] + timeout_option + [source, remote_target]
    else:
        raise ValueError("Unknown authentication method")
    return cmd

def copy_files_to_remote(target, config, files, remote_path="."):
    """Copy the specified files to the remote host."""
    for file in files:
        cmd = build_scp_command(file, target, config, remote_path)
        print(f"Copying {file} to {target}:{remote_path}")
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            print(f"Failed to copy {file} to {target}: {result.stderr.decode()}")

def run_remote_command(target, config, remote_cmd, timeout=DEFAULT_TIMEOUT):
    """Execute a command on the remote host with a timeout."""
    cmd = build_ssh_command(target, config, remote_cmd)
    print(f"Executing command on {target}: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        if result.returncode != 0:
            print(f"Command execution on {target} failed: {result.stderr.decode()}")
        return result
    except subprocess.TimeoutExpired:
        print(f"Connection to {target} timed out.")
        # Return a dummy result with a non-zero returncode to indicate failure
        class DummyResult:
            returncode = -1
        return DummyResult()

def retrieve_file_from_remote(target, config, remote_file, local_path):
    """Copy a file from the remote host to the local machine."""
    username = config["user"]
    auth_method = config["auth"].lower()
    remote_target = f"{username}@{target}:{remote_file}"
    timeout_option = ["-o", f"ConnectTimeout={DEFAULT_TIMEOUT}"]
    if auth_method == "publickey":
        if config.get("keypath"):
            cmd = ["scp", "-i", config["keypath"]] + timeout_option + [remote_target, local_path]
        else:
            cmd = ["scp"] + timeout_option + [remote_target, local_path]
    elif auth_method == "password":
        cmd = ["sshpass", "-p", config["password"], "scp"] + timeout_option + [remote_target, local_path]
    else:
        raise ValueError("Unknown authentication method")
    print(f"Retrieving file {remote_file} from {target} to {local_path}")
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        print(f"Failed to retrieve file: {result.stderr.decode()}")
    return result

def create_remote_ping_script(target_address, config):
    """
    Create the content of a ping test script for the remote host.
    This script will perform ping tests for all targets except the current host.
    """
    targets = []
    for host in config["hosts"]:
        t = test_connectivity(host["host"], host["ip"])
        if t != target_address:
            targets.append(t)
    # Convert ping_interval (milliseconds) to seconds
    interval_sec = config["ping_interval"] / 1000.0
    count = config["ping_times"]
    lines = []
    for tgt in targets:
        # File name format: ping_<local>_to_<target>.txt
        filename = f"ping_{target_address}_to_{tgt}.txt"
        cmd = f"ping -i {interval_sec} -c {count} {tgt} > {filename}"
        lines.append(cmd)
    script_content = "\n".join(lines)
    return script_content, targets

def write_local_temp_file(filename, content):
    """Write content to a local temporary file and make it executable."""
    with open(filename, "w") as f:
        f.write(content)
    os.chmod(filename, 0o755)

def parse_ping_file(filename):
    """
    Parse the ping output file and extract latency data from "time=xxx ms".
    """
    latencies = []
    pattern = re.compile(r"time=([\d\.]+) ms")
    with open(filename, "r") as f:
        for line in f:
            match = pattern.search(line)
            if match:
                latencies.append(float(match.group(1)))
    return latencies

def plot_cdf(latencies, output_file):
    """Plot the latency CDF and save it as an image file."""
    if not latencies:
        print("No latency data available to plot.")
        return
    sorted_latencies = sorted(latencies)
    n = len(sorted_latencies)
    yvals = [i / float(n) for i in range(1, n + 1)]
    plt.figure()
    plt.plot(sorted_latencies, yvals, marker=".", linestyle="none")
    plt.xlabel("Ping Latency (ms)")
    plt.ylabel("Cumulative Distribution (CDF)")
    plt.title("Ping Latency CDF")
    plt.grid(True)
    plt.savefig(output_file)
    plt.close()
    print(f"CDF chart saved as {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Ping test script")
    parser.add_argument("-c", "--config", required=True, help="Path to the configuration file")
    args = parser.parse_args()

    config_file = args.config
    config = read_config(config_file)

    # Test connectivity for each host and record the final target address
    for host in config["hosts"]:
        host["target"] = test_connectivity(host["host"], host["ip"])

    local_download_dir = "ping_results"
    os.makedirs(local_download_dir, exist_ok=True)
    all_result_files = []

    # Iterate over each host to perform remote operations
    for host in config["hosts"]:
        target_address = host["target"]
        print(f"Attempting to connect to {target_address} ...")
        # Test SSH connection with a simple command
        test_cmd = "echo 'Connection successful'"
        res = run_remote_command(target_address, config, test_cmd)
        if res.returncode != 0:
            print(f"Failed to connect to {target_address}, skipping this host.")
            continue

        # Copy the configuration file and current script to the remote host
        current_script = os.path.basename(sys.argv[0])
        copy_files_to_remote(target_address, config, [config_file, sys.argv[0]])

        # Generate the remote ping script content
        remote_script_name = "run_ping.sh"
        script_content, targets = create_remote_ping_script(target_address, config)
        local_script_temp = f"temp_{target_address}_run_ping.sh"
        write_local_temp_file(local_script_temp, script_content)

        # Copy the generated ping script to the remote host
        copy_files_to_remote(target_address, config, [local_script_temp])
        os.remove(local_script_temp)  # Remove local temporary file

        # Execute the ping script on the remote host
        run_remote_command(target_address, config, f"bash {remote_script_name}")

        # Retrieve the ping result files from the remote host
        for tgt in targets:
            remote_filename = f"ping_{target_address}_to_{tgt}.txt"
            local_filename = os.path.join(local_download_dir, f"{target_address}_to_{tgt}.txt")
            retrieve_file_from_remote(target_address, config, remote_filename, local_filename)
            all_result_files.append(local_filename)

    # Aggregate all ping latency data
    all_latencies = []
    for file in all_result_files:
        all_latencies.extend(parse_ping_file(file))

    # Plot and save the CDF chart
    plot_cdf(all_latencies, "ping_cdf.png")

if __name__ == "__main__":
    main()
