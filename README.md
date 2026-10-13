# patchwork

Run a local cluster with configurable control/data node counts and an isolated
MySQL-compatible metadata database:

```sh
python3 -m pip install -r scripts/requirements.txt
python3 scripts/local_cluster.py --control-nodes 2 --data-nodes 4 --open-browser
```

The Flask dashboard runs at `http://127.0.0.1:3300`. See
[the local testing rig](docs/local-cluster.md) for setup and the smoke test.
