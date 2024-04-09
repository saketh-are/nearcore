import sys
import urllib.request
import pickle
import numpy as np
import matplotlib.pyplot as plt

""" Build mapping of IP address to node public key """
def get_keys(node_ips):
    node_key_to_label = {} # maps node public key to label
    for (node, ip) in node_ips.items():
        url = "http://{}:3030/status".format(ip)
        status = urllib.request.urlopen(url).read()

        key = b'\"node_public_key\"'
        start = status.find(key) + len(key) + 2
        end = status.find(b'"', start)
        print(status)
        print(start, end)
        print(status[slice(start, len(status))])
        peer_id = str(status[slice(start, end)], 'utf-8')

        node_key_to_label[peer_id] = node
        print("node_public_key ", peer_id, " is at ", ip)

    return node_key_to_label

""" Parse contents of `near_peer_rtt_bucket` metric """
def parse_rtt_metric(line):
    m = {}
    line = line[29:].decode('utf-8').replace('"', '')
    for kv in line.split(','):
        kv = kv.split('=')
        m[kv[0]] = kv[1]

    tail = m['le'].split('}')
    return (m['sender'], m['receiver'], tail[0], m['size'], int(tail[1]))

""" Assemble `le` and `size` key-sets given IP address """
def get_key_sets(ip):
    def compress(mapping, val):
        if val in mapping:
            return mapping.get(val)
        assign = len(mapping)
        mapping[val] = assign
        return assign

    le_keys = set()
    size_keys = set()

    url = "http://{}:3030/metrics".format(ip)
    resource = urllib.request.urlopen(url)
    metrics = resource.read().split(b'\n')

    for line in metrics:
        if line.startswith(b'near_peer_rtt_bucket'):
            contents = parse_rtt_metric(line)
            le_keys.add(contents[2])
            size_keys.add(contents[3])

    le_keys = sorted(le_keys, key=float)
    size_keys = sorted(size_keys, key=int)

    le_map = {} # maps bucket lower boundary to int
    for le in le_keys:
        le_map[le] = len(le_map)
    size_map = {} # maps size to int
    for size in size_keys:
        size_map[size] = len(size_map)

    return le_keys, le_map, size_keys, size_map

""" Fetch RTT data by querying node metrics """
def get_rtt_data(node_ips, node_key_to_label, le_map, size_map):
    rtt_data = {} # maps (sender, receiver) -> ct[le][size]

    for (node, ip) in node_ips.items():
        url = "http://{}:3030/metrics".format(ip)
        resource = urllib.request.urlopen(url)
        metrics = resource.read().split(b'\n')

        for line in metrics:
            if line.startswith(b'near_peer_rtt_bucket'):
                (sender, receiver, le, size, ct) = parse_rtt_metric(line)

                if not (sender, receiver) in rtt_data:
                    rtt_data[(sender, receiver)] =\
                        [[0 for x in range(len(size_map))] for y in range(len(le_map))]

                rtt_data[(sender, receiver)][le_map[le]][size_map[size]] = ct

    return rtt_data

if len(sys.argv) < 2:
    print("Usage: python rtt.py <node_ips>")
    sys.exit(1)

""" Read node ips from input file """
node_ips = {} # maps a label to an ip address
with open(sys.argv[1], 'r') as file:
    for line in file:
        key, value = line.strip().split(' ')
        node_ips[key] = value

""" Load the data """
if len(sys.argv) < 3:
    node_key_to_label = get_keys(node_ips)

    some_ip = next(iter(node_ips.values()))
    (le_keys, le_map, size_keys, size_map) = get_key_sets(some_ip)

    rtt_data = get_rtt_data(node_ips, node_key_to_label, le_map, size_map)

    all_data = (node_key_to_label, le_keys, le_map, size_keys, size_map, rtt_data)
    with open('data.pkl', 'wb') as file:
        pickle.dump(all_data, file)
else:
    with open(sys.argv[2], 'rb') as file:
        data_loaded = pickle.load(file)
        (node_key_to_label, le_keys, le_map, size_keys, size_map, rtt_data) = data_loaded

""" Draw plots """
for ((sender, receiver), arr) in rtt_data.items():
    if sender not in node_key_to_label:
        print("WARN unrecognized peer id {}", sender)
        continue
    else:
        sender = node_key_to_label[sender]

    if receiver not in node_key_to_label:
        print("WARN unrecognized peer id {}", receiver)
        continue
    else:
        receiver = node_key_to_label[receiver]

    for x in range(len(le_map) - 1, 0, -1):
        for y in range(0, len(size_map)):
            arr[x][y] -= arr[x - 1][y]

    fig, ax = plt.subplots()
    ax.imshow(arr, aspect='auto')
    ax.set_yticks(np.arange(len(le_keys)),
        labels=["{0:0.0f}".format(float(key)) for key in le_keys])
    ax.set_xticks(np.arange(len(size_keys)),
        labels=[key + " MB" for key in size_keys])

    for i in range(len(le_keys)):
        for j in range(len(size_keys)):
            if arr[i][j] > 0:
                text = ax.text(j, i, arr[i][j],
                   ha="center", va="center", color="w")

    plt.gca().invert_yaxis()
    plt.title( "{} to {}".format(sender, receiver) )
    plt.show()

