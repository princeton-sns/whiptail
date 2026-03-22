## IPv6 Addresses
1. `python3 experiments/get_ipv6.py --config experiments/configs/compare-cc.json --subnet 10.10.1`
2. `python3 experiments/clean_ipv6.py --config experiments/configs/compare-cc.json   `
3. `python3 experiments/append_ipv6.py --config experiments/configs/compare-cc.json --input experiments/hosts.txt`
4. Paste the ipv6_interfaces.json into the config file as the `ipv6_interface` field.