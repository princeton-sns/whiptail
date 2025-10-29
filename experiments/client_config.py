import json
import math


def generate_config(nodes, client_totals):
    """
    Generate a full client configuration for distributed experiments.

    Args:
        nodes: list of dicts, each node has:
            {
              "name": "client-0-0",
              "cpus": 40   # total number of logical CPUs on this node
            }
        client_totals: list[int]
            Target total number of client processes for each experiment group.

    Returns:
        dict ready to dump as JSON configuration.
    """

    num_nodes = len(nodes)
    max_cpus = max(n["cpus"] for n in nodes)

    config = {
        "client_total": client_totals,
        "client_processes_per_client_node": [],
        "clients": [],
        "pin_client_processes": []
    }

    for total in client_totals:
        # Estimate number of nodes to use (at least one process per node)
        nodes_used = min(num_nodes, math.ceil(total / 1))
        print(f"Total clients: {total}, initial nodes used: {nodes_used}")

        # Try to evenly assign processes per node
        proc_per_node = math.ceil(total / nodes_used)
        print(f"  Initial processes per node: {proc_per_node}")

        # Limit by available CPU cores (each process uses one physical or two logical threads)
        proc_per_node = min(proc_per_node, max_cpus)
        print(f"  Limited processes per node by CPU cores: {proc_per_node}")

        # Adjust nodes used again after limiting processes per node
        nodes_used = min(num_nodes, math.ceil(total / proc_per_node))
        print(f"  Adjusted nodes used after limiting processes per node: {nodes_used}")
        total = nodes_used * proc_per_node  # ensure consistency
        print(f"  Final total clients for this group: {total}")

        # Append computed values
        config["client_processes_per_client_node"].append(proc_per_node)

        # Select the first N nodes for this group
        used_nodes = [n["name"] for n in nodes[:nodes_used]]
        config["clients"].append(used_nodes)

        # Generate pinning list: odd CPU indices [1, 3, 5, ...]
        pins = list(range(1, proc_per_node * 2, 2))
        config["pin_client_processes"].append(pins)

    return config


if __name__ == "__main__":
    
    totoal_node = 15
    cores = 20
    sites = 2
    node_names = [
        "client-0-1", "client-1-0", "client-1-1", "client-2-0", "client-2-1",
        "client-3-0", "client-3-1", "client-4-0", "client-4-1", "client-5-0",
        "client-5-1", "client-6-0", "client-6-1", "client-7-0", "client-7-1"
    ]
    # 🧠 Example input: 16 nodes, each with 40 logical CPUs
    nodes = [{"name": n, "cpus": 40} for n in node_names]


    # Total client process counts for each experiment group
    client_totals = [1,2,4,8,15,30,60,75,90,105,120,135,150,180,240,300]

    print(client_totals)
    config = generate_config(nodes, client_totals)

    # Write output to JSON
    with open("experiment_config.json", "w") as f:
        json.dump(config, f, indent=2)

    print("✅ Generated experiment_config.json successfully.")
