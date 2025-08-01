import atexit
import base64
import copy
import json
import os
import pathlib
from typing import Optional

import rc
from geventhttpclient import Session, useragent
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback
import typing
import uuid
from rc import gcloud
from retrying import retry

import base58

import network
from configured_logger import logger
from key import Key
from proxy import NodesProxy
import state_sync_lib

# cspell:ignore nretry pmap preemptible proxify uefi useragent
os.environ["ADVERSARY_CONSENT"] = "1"

remote_nodes = []
remote_nodes_lock = threading.Lock()
cleanup_remote_nodes_atexit_registered = False

Config = typing.Dict[str, typing.Any]

# Example value: [
#   ("num_block_producer_seats_per_shard", [100]),
#   ("epoch_length", 100)
# ]
# Note that we also support using list instead of a tuple here, but that
# should be discouraged
GenesisConfigChanges = typing.List[typing.Tuple[str, typing.Any]]

# Example value: {
#   "tracked_shards_config": "NoShards",
#   "consensus.min_block_production_delay": {
#       "secs": 1,
#       "nanos": 300000000
#   }
# }
ClientConfigChange = typing.Dict[str, typing.Any]
# Key represent the index of the node.
ClientConfigChanges = typing.Dict[int, ClientConfigChange]


# Return the session object that can be used for making http requests.
#
# Please note that if the request is consistently failing the default parameters
# mean that the calls will take connection_timeout + timeout * (1 + max_retries) ~ 1 minute.
#
# The return value is a context manager that should be used in a with statement.
# e.g.
# with session() as s:
#   r = s.get("http://example.com")
def session(timeout=9, max_retries=5) -> Session:
    return Session(connection_timeout=6,
                   network_timeout=timeout,
                   max_retries=max_retries,
                   retry_delay=0.1)


class DownloadException(Exception):
    pass


def atexit_cleanup(node):
    logger.info("Cleaning up node %s:%s on script exit" % node.addr())
    logger.info("Executed store validity tests: %s" % node.store_tests)
    try:
        node.cleanup()
    except:
        logger.info("Cleaning failed!")
        traceback.print_exc()
        pass


def atexit_cleanup_remote():
    with remote_nodes_lock:
        if remote_nodes:
            rc.pmap(atexit_cleanup, remote_nodes)


# custom retry that is used in wait_for_rpc() and get_status()
def nretry(fn, timeout):
    started = time.time()
    delay = 0.05
    while True:
        try:
            return fn()
        except:
            if time.time() - started >= timeout:
                raise
            time.sleep(delay)
            delay *= 1.2


BootNode = typing.Union[None, 'BaseNode', typing.Iterable['BaseNode']]


def make_boot_nodes_arg(boot_node: BootNode) -> typing.Tuple[str]:
    """Converts `boot_node` argument to `--boot-nodes` command line argument.

    If the argument is `None` returns an empty tuple.  Otherwise, returns
    a tuple representing arguments to be added to `neard` invocation for setting
    boot nodes according to `boot_node` argument.

    Apart from `None` as described above, `boot_node` can be a [`BaseNode`]
    object, or an iterable (think list) of [`BaseNode`] objects.  The boot node
    address of a BaseNode object is constructed using [`BaseNode.addr_with_pk`]
    method.

    If iterable of nodes is given, the `neard` is going to be configured with
    multiple boot nodes.

    Args:
        boot_node: Specification of boot node(s).
    Returns:
        A tuple to add to `neard` invocation specifying boot node(s) if any
        specified.
    """
    if not boot_node:
        return ()
    try:
        it = iter(boot_node)
    except TypeError:
        it = iter((boot_node,))
    nodes = ','.join(node.addr_with_pk() for node in it)
    if not nodes:
        return ()
    return ('--boot-nodes', nodes)


class BlockId(typing.NamedTuple):
    """Stores block’s height and hash.

    The values can be accessed either through properties or by structural
    deconstruction, e.g.:

        block_height, block_hash = block_id
        assert block_height == block_id.height
        assert block_hash == block_id.hash

    Attributes:
        height: Block’s height.
        hash: Block’s hash encoding using base58.
        hash_bytes: Block’s hash decoded as raw bytes.  Note that this attribute
            cannot be accessed through aforementioned deconstruction.
    """
    height: int
    hash: str

    @classmethod
    def from_header(cls, header: typing.Dict[str, typing.Any]) -> 'BlockId':
        return cls(height=int(header['height']), hash=header['hash'])

    @property
    def hash_bytes(self) -> bytes:
        return base58.b58decode(self.hash.encode('ascii'))

    def __str__(self) -> str:
        return f'#{self.height} {self.hash}'

    def __eq__(self, rhs) -> bool:
        return (isinstance(rhs, BlockId) and self.height == rhs.height and
                self.hash == rhs.hash)


class BaseNode(object):

    def __init__(self):
        self._start_proxy = None
        self._proxy_local_stopped = None
        self.proxy = None
        self.store_tests = 0
        self.is_check_store = True

    def change_config(self, overrides: typing.Dict[str, typing.Any]) -> None:
        """Change client config.json of a node by applying given overrides.

        Changes to the configuration need to be made while the node is stopped.
        More precisely, while the changes may be made at any point, the node
        reads the time at startup only.

        The overrides are a dictionary specifying new values for configuration
        keys.  Non-dictionary values are applied directly, while dictionaries
        are non-recursively merged.  For example if the original config is:

            {
                'foo': 42,
                'bar': {'a': 1, 'b': 2, 'c': {'A': 3}},
            }

        and overrides are:

            {
                'foo': 24,
                'bar': {'a': -1, 'c': {'D': 3}, 'd': 1},
            }

        then resulting configuration file will be:

            {
                'foo': 24,
                'bar': {'a': -1, 'b': 2, 'c': {'D': 3}, 'd': 1},
            }

        Args:
            overrides: A dictionary of config overrides.  Non-dictionary values
                are set as is, dictionaries are non-recursively merged.
        Raises:
            NotImplementedError: Currently changing the configuration is
                supported on local node only.
        """
        name = type(self).__name__
        raise NotImplementedError('change_config not supported by ' + name)

    def _get_command_line(self,
                          near_root,
                          node_dir,
                          boot_node: BootNode,
                          binary_name='neard'):
        cmd = (os.path.join(near_root, binary_name), '--home', node_dir, 'run')
        return cmd + make_boot_nodes_arg(boot_node)

    def get_command_for_subprogram(self, cmd: tuple):
        return (os.path.join(self.near_root,
                             self.binary_name), '--home', self.node_dir) + cmd

    def addr_with_pk(self) -> str:
        pk_hash = self.node_key.pk.split(':')[1]
        host, port = self.addr()
        return '{}@{}:{}'.format(pk_hash, host, port)

    def wait_for_rpc(self, timeout=1):
        nretry(lambda: self.get_status(), timeout=timeout)

    # Send the given JSON-RPC request to the node and return the response.
    #
    # Please note that if the request is consistently failing the default parameters
    # mean that the call will take connection_timeout + timeout * (1 + max_retries) ~ 1 minute.
    def json_rpc(self, method, params, timeout=9, max_retries=5):
        j = {
            'method': method,
            'params': params,
            'id': 'dontcare',
            'jsonrpc': '2.0'
        }
        with session(timeout, max_retries) as s:
            r = s.post("http://%s:%s" % self.rpc_addr(), json=j)
            r.raise_for_status()
        return json.loads(r.content)

    def send_tx(self, signed_tx):
        return self.json_rpc('broadcast_tx_async',
                             [base64.b64encode(signed_tx).decode('utf8')])

    def send_tx_and_wait(self, signed_tx, timeout):
        return self.json_rpc('broadcast_tx_commit',
                             [base64.b64encode(signed_tx).decode('utf8')],
                             timeout=timeout)

    def send_tx_and_wait_until(self, signed_tx, wait_until, timeout):
        params = {
            'signed_tx_base64': base64.b64encode(signed_tx).decode('utf8'),
            "wait_until": wait_until
        }
        return self.json_rpc('send_tx', params, timeout=timeout)

    def get_status(self,
                   check_storage: bool = True,
                   timeout: float = 4,
                   verbose: bool = False):
        with session(timeout) as s:
            r = s.get("http://%s:%s/status" % self.rpc_addr())
            r.raise_for_status()
            status = json.loads(r.content)
        if verbose:
            logger.info(f'Status: {status}')
        if check_storage and status['sync_info']['syncing'] == False:
            # Storage is not guaranteed to be in consistent state while syncing
            self.check_store()
        if verbose:
            logger.info(status)
        return status

    def get_metrics(self, timeout: float = 4):
        with session(timeout) as s:
            r = s.get("http://%s:%s/metrics" % self.rpc_addr())
            r.raise_for_status()
        return r.content

    def get_latest_block(self, **kw) -> BlockId:
        """
        Get the hash and height of the latest block.
        If you need the whole block info, use `.get_block_by_finality('optimistic')`
        """
        sync_info = self.get_status(**kw)['sync_info']
        return BlockId(height=sync_info['latest_block_height'],
                       hash=sync_info['latest_block_hash'])

    def get_all_heights(self):

        # Helper function to check if the block response is a "block not found" error.
        def block_not_found(block) -> bool:
            error = block.get('error')
            if error is None:
                return False

            data = error.get('data')
            if data is None:
                return False

            return 'DB Not Found Error: BLOCK:' in data

        hash_ = self.get_latest_block().hash
        heights = []

        while True:
            block = self.get_block(hash_)
            if block_not_found(block):
                break
            elif 'result' not in block:
                logger.info(block)

            height = block['result']['header']['height']
            if height == 0:
                break
            heights.append(height)
            hash_ = block['result']['header']['prev_hash']

        return reversed(heights)

    def get_validators(self, epoch_id=None):
        if epoch_id is None:
            args = [None]
        else:
            args = {'epoch_id': epoch_id}
        return self.json_rpc('validators', args)

    def get_account(self,
                    acc,
                    finality='optimistic',
                    block=None,
                    do_assert=True,
                    **kwargs):
        query = {
            "request_type": "view_account",
            "account_id": acc,
        }
        if block is not None:
            # this can be either height or hash
            query["block_id"] = block
        else:
            query["finality"] = finality
        res = self.json_rpc('query', query, **kwargs)
        if do_assert:
            assert 'error' not in res, res

        return res

    def call_function(self,
                      acc,
                      method,
                      args,
                      finality='optimistic',
                      timeout=2):
        return self.json_rpc('query', {
            "request_type": "call_function",
            "account_id": acc,
            "method_name": method,
            "args_base64": args,
            "finality": finality
        },
                             timeout=timeout)

    def get_access_key_list(self, acc, finality='optimistic'):
        return self.json_rpc(
            'query', {
                "request_type": "view_access_key_list",
                "account_id": acc,
                "finality": finality
            })

    def get_access_key(self, account_id, public_key, finality='optimistic'):
        return self.json_rpc(
            'query', {
                "request_type": "view_access_key",
                "account_id": account_id,
                "public_key": public_key,
                "finality": finality
            })

    def wait_at_least_one_block(self):
        start_height = self.get_latest_block().height
        timeout_sec = 5
        started = time.monotonic()
        while time.monotonic() - started < timeout_sec:
            height = self.get_latest_block().height
            if height > start_height:
                break
            time.sleep(0.2)

    def get_nonce_for_pk(self, acc, pk, finality='optimistic'):
        for access_key in self.get_access_key_list(acc,
                                                   finality)['result']['keys']:
            if access_key['public_key'] == pk:
                return access_key['access_key']['nonce']
        return None

    def get_block(self, block_id, **kwargs):
        return self.json_rpc('block', [block_id], **kwargs)

    def get_block_by_height(self, block_height, **kwargs):
        return self.json_rpc('block', {'block_id': block_height}, **kwargs)

    def get_final_block(self, **kwargs):
        return self.get_block_by_finality('final')

    def get_block_by_finality(self, finality, **kwargs):
        assert finality in ('final', 'optimistic'), \
            f"invalid finality value: {finality}"
        return self.json_rpc('block', {'finality': finality}, **kwargs)

    def get_chunk(self, chunk_id):
        return self.json_rpc('chunk', [chunk_id])

    def get_prev_epoch_id(self) -> str:
        """ Get ID of the previous epoch. """
        latest_block = self.get_block_by_finality('optimistic')['result']
        next_epoch_id = latest_block['header']['next_epoch_id']
        # Next epoch ID is a hash of some block from the previous epoch
        return self.get_epoch_id(block_hash=next_epoch_id)

    def get_epoch_id(
        self,
        block_height: Optional[int] = None,
        block_hash: Optional[str] = None,
    ) -> str:
        """
        Get epoch ID for a given block (either by block height or hash).
        If neither height nor hash is given, return the current epoch ID.
        """
        assert block_height is None or block_hash is None, "use either height or has, not both"
        if block_height is not None:
            block = self.get_block_by_height(block_height)['result']
        elif block_hash is not None:
            block = self.get_block(block_hash)['result']
        else:
            block = self.get_block_by_finality('optimistic')['result']
        return block['header']['epoch_id']

    # Get the transaction status.
    #
    # The default timeout is quite high - 15s - so that is longer than the
    # node's default polling_timeout. It's done this way to differentiate
    # between the case when the transaction is not found on the node and when
    # the node is dead or not responding.
    def get_tx(self, tx_hash, tx_recipient_id, timeout=15):
        return self.json_rpc(
            'tx',
            [tx_hash, tx_recipient_id],
            timeout=timeout,
            max_retries=0,
        )

    def get_block_effects(self, changes_in_block_request):
        return self.json_rpc('block_effects', changes_in_block_request)

    # `EXPERIMENTAL_changes_in_block` is deprecated as of 2.8, use `get_block_effects` instead
    def get_changes_in_block(self, changes_in_block_request):
        return self.json_rpc('EXPERIMENTAL_changes_in_block',
                             changes_in_block_request)

    def get_changes(self, changes_request):
        return self.json_rpc('changes', changes_request)

    # `EXPERIMENTAL_changes` is deprecated as of 2.7, use `get_changes` test instead
    def get_experimental_changes(self, changes_request):
        return self.json_rpc('EXPERIMENTAL_changes', changes_request)

    def validators(self):
        return set(
            map(lambda v: v['account_id'],
                self.get_status()['validators']))

    def stop_checking_store(self):
        logger.warning("Stopping checking Storage for inconsistency for %s:%s" %
                       self.addr())
        self.is_check_store = False

    def check_store(self):
        if self.is_check_store:
            try:
                res = self.json_rpc('adv_check_store', [])
                if not 'result' in res:
                    # cannot check Storage Consistency for the node, possibly not Adversarial Mode is running
                    pass
                else:
                    if res['result'] == 0:
                        logger.error(
                            "Storage for %s:%s in inconsistent state, stopping"
                            % self.addr())
                        self.kill()
                    self.store_tests += res['result']
            except useragent.BadStatusCode:
                pass


class RpcNode(BaseNode):
    """ A running node only interact by rpc queries """

    def __init__(self, host, rpc_port):
        super(RpcNode, self).__init__()
        self.host = host
        self.rpc_port = rpc_port

    def rpc_addr(self):
        return (self.host, self.rpc_port)


class LocalNode(BaseNode):

    def __init__(
        self,
        port,
        rpc_port,
        near_root,
        node_dir,
        blacklist,
        binary_name=None,
        single_node=False,
        ordinal=None,
    ):
        super(LocalNode, self).__init__()
        self.port = port
        self.rpc_port = rpc_port
        self.near_root = str(near_root)
        self.node_dir = node_dir
        self.binary_name = binary_name or 'neard'
        self.ordinal = ordinal
        self.cleaned = False
        self.validator_key = Key.from_json_file(
            os.path.join(node_dir, "validator_key.json"))
        self.node_key = Key.from_json_file(
            os.path.join(node_dir, "node_key.json"))
        self.signer_key = Key.from_json_file(
            os.path.join(node_dir, "validator_key.json"))
        self._process = None

        self.change_config({
            'network': {
                'addr': f'0.0.0.0:{port}',
                'blacklist': list(blacklist)
            },
            'rpc': {
                'addr': f'0.0.0.0:{rpc_port}',
            },
            'consensus': {
                'min_num_peers': int(not single_node)
            },
        })

        atexit.register(atexit_cleanup, self)

    def change_config(self, overrides: typing.Dict[str, typing.Any]) -> None:
        apply_config_changes(self.node_dir, overrides)

    def addr(self):
        return ("127.0.0.1", self.port)

    def rpc_addr(self):
        return ("127.0.0.1", self.rpc_port)

    def start_proxy_if_needed(self):
        if self._start_proxy is not None:
            self._proxy_local_stopped = self._start_proxy()

    def output_logs(self):
        stdout = pathlib.Path(self.node_dir) / 'stdout'
        stderr = pathlib.Path(self.node_dir) / 'stderr'
        if os.environ.get('CI_HACKS'):
            logger.info('=== stdout: ')
            logger.info(stdout.read_text('utf-8', 'replace'))
            logger.info('=== stderr: ')
            logger.info(stderr.read_text('utf-8', 'replace'))
        else:
            logger.info(f'=== stdout: available at {stdout}')
            logger.info(f'=== stderr: available at {stderr}')

    def start(
            self,
            *,
            boot_node: BootNode = None,
            skip_starting_proxy=False,
            extra_env: typing.Dict[str, str] = dict(),
    ):
        logger.info(f"Starting node {self.ordinal}.")
        cmd = self._get_command_line(
            self.near_root,
            self.node_dir,
            boot_node,
            self.binary_name,
        )

        if self._proxy_local_stopped is not None:
            while self._proxy_local_stopped.value != 2:
                logger.info(f'Waiting for previous proxy instance to close')
                time.sleep(1)

        self.run_cmd(cmd=cmd, extra_env=extra_env)

        if not skip_starting_proxy:
            self.start_proxy_if_needed()

        try:
            self.wait_for_rpc(10)
        except:
            logger.error(
                '=== failed to start node, rpc is not ready in 10 seconds')

    def run_cmd(self, *, cmd: tuple, extra_env: typing.Dict[str, str] = dict()):

        env = os.environ.copy()
        env["RUST_BACKTRACE"] = "1"
        env["RUST_LOG"] = "actix_web=warn,mio=warn,tokio_util=warn,actix_server=warn,actix_http=warn," + env.get(
            "RUST_LOG", "debug")
        env.update(extra_env)
        node_dir = pathlib.Path(self.node_dir)
        self.stdout_name = node_dir / 'stdout'
        self.stderr_name = node_dir / 'stderr'
        with open(self.stdout_name, 'ab') as stdout, \
                open(self.stderr_name, 'ab') as stderr:
            self._process = subprocess.Popen(cmd,
                                             stdin=subprocess.DEVNULL,
                                             stdout=stdout,
                                             stderr=stderr,
                                             env=env)
        self._pid = self._process.pid

    def kill(self, *, gentle=False):
        logger.info(f"Killing node {self.ordinal}.")
        """Kills the process.  If `gentle` sends SIGINT before killing."""
        if self._proxy_local_stopped is not None:
            self._proxy_local_stopped.value = 1
        if self._process and gentle:
            self._process.send_signal(signal.SIGINT)
            try:
                self._process.wait(5)
                self._process = None
            except subprocess.TimeoutExpired:
                pass
        if self._process:
            self._process.kill()
            self._process.wait(5)
            self._process = None

    def reload_updatable_config(self):
        logger.info(f"Reloading updatable config for node {self.ordinal}.")
        """Sends SIGHUP signal to the process in order to trigger updatable config reload."""
        self._process.send_signal(signal.SIGHUP)

    def reset_data(self):
        shutil.rmtree(os.path.join(self.node_dir, "data"))

    def reset_validator_key(self, new_key):
        self.validator_key = new_key
        with open(os.path.join(self.node_dir, "validator_key.json"), 'w+') as f:
            json.dump(new_key.to_json(), f)

    def remove_validator_key(self):
        logger.info(
            f"Removing validator_key.json file for node {self.ordinal}.")
        self.validator_key = None
        file_path = os.path.join(self.node_dir, "validator_key.json")
        if os.path.exists(file_path):
            os.remove(file_path)

    def reset_node_key(self, new_key):
        self.node_key = new_key
        with open(os.path.join(self.node_dir, "node_key.json"), 'w+') as f:
            json.dump(new_key.to_json(), f)

    def cleanup(self):
        if self.cleaned:
            return

        try:
            self.kill()
        except:
            logger.critical('Kill failed on cleanup!', exc_info=sys.exc_info())

        # move the node dir to avoid weird interactions with multiple serial test invocations
        target_path = self.node_dir + '_finished'
        if os.path.exists(target_path) and os.path.isdir(target_path):
            shutil.rmtree(target_path)
        os.rename(self.node_dir, target_path)
        self.node_dir = target_path
        self.output_logs()
        self.cleaned = True

    def stop_network(self):
        logger.info(f'Stopping network for process {self._pid}')
        network.stop(self._pid)

    def resume_network(self):
        logger.info(f'Resuming network for process {self._pid}')
        network.resume_network(self._pid)


class GCloudNode(BaseNode):

    def __init__(self, *args, username=None, project=None, ssh_key_path=None):
        if len(args) == 1:
            name = args[0]
            # Get existing instance assume it's ready to run.
            self.instance_name = name
            self.port = 24567
            self.rpc_port = 3030
            self.machine = gcloud.get(name,
                                      username=username,
                                      project=project,
                                      ssh_key_path=ssh_key_path)
            self.ip = self.machine.ip
        elif len(args) == 4:
            # Create new instance from scratch
            instance_name, zone, node_dir, binary = args
            self.instance_name = instance_name
            self.port = 24567
            self.rpc_port = 3030
            self.node_dir = node_dir
            self.machine = gcloud.create(
                name=instance_name,
                machine_type='n1-standard-2',
                disk_size='50G',
                image_project='gce-uefi-images',
                image_family='ubuntu-1804-lts',
                zone=zone,
                firewall_allows=['tcp:3030', 'tcp:24567'],
                min_cpu_platform='Intel Skylake',
                preemptible=False,
            )
            # self.ip = self.machine.ip
            self._upload_config_files(node_dir)
            self._download_binary(binary)
            with remote_nodes_lock:
                global cleanup_remote_nodes_atexit_registered
                if not cleanup_remote_nodes_atexit_registered:
                    atexit.register(atexit_cleanup_remote)
                    cleanup_remote_nodes_atexit_registered = True
        else:
            raise Exception()

    def _upload_config_files(self, node_dir):
        self.machine.run('bash', input='mkdir -p ~/.near')
        self.machine.upload(os.path.join(node_dir, '*.json'),
                            f'/home/{self.machine.username}/.near/')
        self.validator_key = Key.from_json_file(
            os.path.join(node_dir, "validator_key.json"))
        self.node_key = Key.from_json_file(
            os.path.join(node_dir, "node_key.json"))
        self.signer_key = Key.from_json_file(
            os.path.join(node_dir, "validator_key.json"))

    @retry(wait_fixed=1000, stop_max_attempt_number=3)
    def _download_binary(self, binary):
        p = self.machine.run('bash',
                             input=f'''
/snap/bin/gsutil cp gs://nearprotocol_nearcore_release/{binary} neard
chmod +x neard
''')
        if p.returncode != 0:
            raise DownloadException(p.stderr)

    def addr(self):
        return (self.ip, self.port)

    def rpc_addr(self):
        return (self.ip, self.rpc_port)

    def start(self,
              *,
              boot_node: BootNode = None,
              extra_env: typing.Dict[str, str] = dict()):
        if "RUST_BACKTRACE" not in extra_env:
            extra_env["RUST_BACKTRACE"] = "1"
        extra_env = [f"{k}={v}" for (k, v) in extra_env]
        extra_env = " ".join(extra_env)
        self.machine.run_detach_tmux(
            extra_env +
            " ".join(self._get_command_line('.', '.near', boot_node)))
        self.wait_for_rpc(timeout=30)

    def kill(self):
        self.machine.run('tmux send-keys -t python-rc C-c')
        time.sleep(3)
        self.machine.kill_detach_tmux()

    def destroy_machine(self):
        self.machine.delete()

    def cleanup(self):
        self.kill()
        # move the node dir to avoid weird interactions with multiple serial test invocations
        target_path = self.node_dir + '_finished'
        if os.path.exists(target_path) and os.path.isdir(target_path):
            shutil.rmtree(target_path)
        os.rename(self.node_dir, target_path)

        # Get log and delete machine
        rc.run(f'mkdir -p /tmp/pytest_remote_log')
        self.machine.download(
            '/tmp/python-rc.log',
            f'/tmp/pytest_remote_log/{self.machine.name}.log')
        self.destroy_machine()

    def json_rpc(self, method, params, timeout=15):
        return super().json_rpc(method, params, timeout=timeout)

    def get_status_impl(self):
        with session(timeout=15) as s:
            return s.get("http://%s:%s/status" % self.rpc_addr())

    def get_status(self):
        r = nretry(lambda: self.get_status_impl, timeout=45)
        r.raise_for_status()
        return json.loads(r.content)

    def stop_network(self):
        rc.run(
            f'gcloud compute firewall-rules create {self.machine.name}-stop --direction=EGRESS --priority=1000 --network=default --action=DENY --rules=all --target-tags={self.machine.name}'
        )

    def resume_network(self):
        rc.run(f'gcloud compute firewall-rules delete {self.machine.name}-stop',
               input='yes\n')

    def reset_validator_key(self, new_key):
        self.validator_key = new_key
        with open(os.path.join(self.node_dir, "validator_key.json"), 'w+') as f:
            json.dump(new_key.to_json(), f)
        self.machine.upload(os.path.join(self.node_dir, 'validator_key.json'),
                            f'/home/{self.machine.username}/.near/')


def spin_up_node(
    config,
    near_root,
    node_dir,
    ordinal,
    *,
    boot_node: BootNode = None,
    blacklist=(),
    proxy=None,
    skip_starting_proxy=False,
    single_node=False,
    sleep_after_start=3,
) -> BaseNode:
    is_local = config['local']

    args = make_boot_nodes_arg(boot_node)
    logger.info("Starting node %s %s" %
                (ordinal,
                 ('with ' + '='.join(args) if args else 'as BOOT NODE')))
    if is_local:
        blacklist = [
            "127.0.0.1:%s" % (24567 + 10 + bl_ordinal)
            for bl_ordinal in blacklist
        ]
        node = LocalNode(24567 + 10 + ordinal,
                         3030 + 10 + ordinal,
                         near_root,
                         node_dir,
                         blacklist,
                         config.get('binary_name'),
                         single_node,
                         ordinal=ordinal)
    else:
        # TODO: Figure out how to know IP address beforehand for remote deployment.
        assert len(
            blacklist) == 0, "Blacklist is only supported in LOCAL deployment."

        instance_name = '{}-{}-{}'.format(
            config['remote'].get('instance_name', 'near-pytest'), ordinal,
            uuid.uuid4())
        zones = config['remote']['zones']
        zone = zones[ordinal % len(zones)]
        node = GCloudNode(instance_name, zone, node_dir,
                          config['remote']['binary'])
        with remote_nodes_lock:
            remote_nodes.append(node)
        logger.info(f"node {ordinal} machine created")

    if proxy is not None:
        proxy.proxify_node(node)

    node.start(boot_node=boot_node, skip_starting_proxy=skip_starting_proxy)
    time.sleep(sleep_after_start)
    logger.info(f"node {ordinal} started")
    return node


def init_cluster(
    num_nodes: int,
    num_observers: int,
    num_shards: int,
    config: Config,
    genesis_config_changes: GenesisConfigChanges,
    client_config_changes: ClientConfigChanges,
    prefix="test",
    extra_state_dumper=False,
) -> typing.Tuple[str, typing.List[str]]:
    """
    Create cluster configuration
    """
    if 'local' not in config and 'nodes' in config:
        logger.critical(
            "Attempt to launch a regular test with a mocknet config")
        sys.exit(1)

    if not prefix.startswith("test"):
        logger.critical(f"The prefix must begin with 'test'. prefix = {prefix}")
        sys.exit(1)

    is_local = config['local']
    near_root = config['near_root']
    binary_name = config.get('binary_name', 'neard')
    binary_path = os.path.join(near_root, binary_name)

    if extra_state_dumper:
        num_observers += 1

    logger.info("Creating %s cluster configuration with %s nodes" %
                ("LOCAL" if is_local else "REMOTE", num_nodes + num_observers))

    process = subprocess.Popen(
        [
            binary_path,
            "localnet",
            "--validators",
            str(num_nodes),
            "--non-validators",
            str(num_observers),
            "--shards",
            str(num_shards),
            "--tracked-shards",
            "none",
            "--prefix",
            prefix,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = process.communicate()
    assert 0 == process.returncode, err

    node_dirs = [
        line.split()[-1]
        for line in err.decode('utf8').split('\n')
        if '/test' in line
    ]
    assert len(
        node_dirs
    ) == num_nodes + num_observers, "node dirs: %s num_nodes: %s num_observers: %s" % (
        len(node_dirs), num_nodes, num_observers)

    logger.info("Search for stdout and stderr in %s" % node_dirs)

    # if extra_state_dumper is True, we added 1 to num_observers above and we will enable
    # state dumping to a local tmp dir on the last node in node_dirs. The other nodes will have their
    # state_sync configs point to this tmp dir
    # TODO: remove this extra_state_dumper option when centralized state sync is no longer used
    if extra_state_dumper:
        (node_config_dump,
         node_config_sync) = state_sync_lib.get_state_sync_configs_pair(
             tracked_shards_config=None)
        syncing_nodes = node_dirs[:-1]
        dumper_node = node_dirs[-1]
        for node_dir in syncing_nodes:
            apply_config_changes(node_dir, node_config_sync)
        apply_config_changes(dumper_node, node_config_dump)

    # apply config changes
    for i, node_dir in enumerate(node_dirs):
        apply_genesis_changes(node_dir, genesis_config_changes)
        overrides = client_config_changes.get(i,
                                              DEFAULT_CLIENT_CONFIG_OVERRIDES)
        if overrides:
            apply_config_changes(node_dir, overrides)

    # apply config changes for nodes marked as archival node.
    # for now, we do this only for local nodes (eg. nayduck tests).
    for i, node_dir in enumerate(node_dirs):
        configure_cold_storage_for_archival_node(node_dir)

    return near_root, node_dirs


def configure_cold_storage_for_archival_node(node_dir: str):
    """ If the node is marked as an archival node, configures the split storage.

    In particular, it assumes that the hot storage is already configured, and
    it creates and configures the cold storage based on the hot storage.
    """
    node_dir = pathlib.Path(node_dir)
    fname = node_dir / 'config.json'
    with open(fname) as fd:
        config_json = json.load(fd)

    # Skip if this is not an archival node or cold storage is already configured.
    if not config_json.get("archive",
                           False) or config_json.get("cold_store") is not None:
        return

    logger.debug(f"Configuring cold storage for archival node: {node_dir.stem}")

    hot_store_config = config_json.get("store")
    assert hot_store_config is not None, "Hot storage is not configured"

    cold_store_config = copy.deepcopy(hot_store_config)
    cold_store_config["path"] = "cold-data"
    config_json["cold_store"] = cold_store_config
    config_json["save_trie_changes"] = True

    if "split_storage" not in config_json:
        config_json["split_storage"] = {
            "enable_split_storage_view_client": True,
            "cold_store_initial_migration_loop_sleep_duration": {
                "secs": 0,
                "nanos": 100000000
            },
            "cold_store_loop_sleep_duration": {
                "secs": 0,
                "nanos": 100000000
            },
        }

    with open(fname, 'w') as fd:
        json.dump(config_json, fd, indent=2)


def apply_genesis_changes(node_dir: str,
                          genesis_config_changes: GenesisConfigChanges):
    # apply genesis.json changes
    fname = os.path.join(node_dir, 'genesis.json')
    with open(fname) as fd:
        genesis_config = json.load(fd)
    for change in genesis_config_changes:
        cur = genesis_config
        for s in change[:-2]:
            cur = cur[s]
        assert change[-2] in cur
        cur[change[-2]] = change[-1]
    with open(fname, 'w') as fd:
        json.dump(genesis_config, fd, indent=2)


def apply_config_changes(node_dir: str,
                         client_config_change: ClientConfigChange):
    # apply config.json changes
    fname = os.path.join(node_dir, 'config.json')
    with open(fname) as fd:
        config_json = json.load(fd)

    # ClientConfig keys which are valid but may be missing from the config.json
    # file.  Those are often Option<T> types which are not stored in JSON file
    # when None.
    allowed_missing_configs = (
        'archive',
        'consensus.block_fetch_horizon',
        'consensus.block_header_fetch_horizon',
        'consensus.min_block_production_delay',
        'consensus.max_block_production_delay',
        'consensus.max_block_wait_delay',
        'consensus.state_sync_external_timeout',
        'consensus.state_sync_external_backoff',
        'consensus.state_sync_p2p_timeout',
        'expected_shutdown',
        'log_summary_period',
        'max_gas_burnt_view',
        'rosetta_rpc',
        'save_trie_changes',
        'save_tx_outcomes',
        'split_storage',
        'state_sync',
        'state_sync_enabled',
        'store.state_snapshot_config.state_snapshot_type',
        'tracked_shard_schedule',
        'tracked_shards_config.Schedule',
        'tracked_shards_config.ShadowValidator',
        'cold_store',
        'store.load_mem_tries_for_tracked_shards',
    )

    for k, v in client_config_change.items():
        if not (k in allowed_missing_configs or k in config_json):
            raise ValueError(f'Unknown configuration option: {k}')
        if k in config_json and isinstance(v, dict):
            config_json[k].update(v)
        else:
            # Support keys in the form of "a.b.c".
            parts = k.split('.')
            current = config_json
            for part in parts[:-1]:
                if part not in current:
                    raise ValueError(
                        f'{part} is not found in config.json. Key={k}, Value={v}'
                    )
                if not isinstance(current[part], dict):
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = v

    with open(fname, 'w') as fd:
        json.dump(config_json, fd, indent=2)


def get_config_json(node_dir):
    fname = os.path.join(node_dir, 'config.json')
    with open(fname) as fd:
        return json.load(fd)


def set_config_json(node_dir, config_json):
    fname = os.path.join(node_dir, 'config.json')
    with open(fname, 'w') as fd:
        json.dump(config_json, fd, indent=2)


def start_cluster(
    num_nodes: int,
    num_observers: int,
    num_shards: int,
    config: typing.Optional[Config],
    genesis_config_changes: GenesisConfigChanges,
    client_config_changes: ClientConfigChanges,
    message_handler=None,
    extra_state_dumper=False,
) -> typing.List[BaseNode]:
    if not config:
        config = load_config()

    dot_near = pathlib.Path.home() / '.near'
    if (dot_near / 'test0').exists():
        near_root = config['near_root']
        node_dirs = [
            str(dot_near / name)
            for name in os.listdir(dot_near)
            if name.startswith('test') and not name.endswith('_finished')
        ]
    else:
        near_root, node_dirs = init_cluster(
            num_nodes,
            num_observers,
            num_shards,
            config,
            genesis_config_changes,
            client_config_changes,
            extra_state_dumper=extra_state_dumper)

    proxy = NodesProxy(message_handler) if message_handler is not None else None
    ret = []

    def spin_up_node_and_push(i: int, boot_node: BootNode) -> BaseNode:
        single_node = len(node_dirs) == 1
        node = spin_up_node(
            config,
            near_root,
            node_dirs[i],
            ordinal=i,
            boot_node=boot_node,
            proxy=proxy,
            skip_starting_proxy=True,
            single_node=single_node,
        )
        ret.append((i, node))
        return node

    boot_node = spin_up_node_and_push(0, None)

    handles = []
    for i in range(1, len(node_dirs)):
        handle = threading.Thread(target=spin_up_node_and_push,
                                  args=(i, boot_node))
        handle.start()
        handles.append(handle)

    for handle in handles:
        handle.join()

    nodes = [node for _, node in sorted(ret)]
    for node in nodes:
        node.start_proxy_if_needed()

    return nodes


ROOT_DIR = pathlib.Path(__file__).resolve().parents[2]


def get_near_root():
    cargo_target_dir = os.environ.get('CARGO_TARGET_DIR', 'target')
    default_root = (ROOT_DIR / cargo_target_dir / 'debug').resolve()
    return os.environ.get('NEAR_ROOT', str(default_root))


DEFAULT_CONFIG: Config = {
    'local': True,
    'near_root': get_near_root(),
    'binary_name': 'neard',
    'release': False,
}
CONFIG_ENV_VAR = 'NEAR_PYTEST_CONFIG'
DEFAULT_CLIENT_CONFIG_OVERRIDES = {
    'save_tx_outcomes':
        True,  # Allow querying transaction outcomes in tests by default.
}


def load_config() -> Config:
    config = DEFAULT_CONFIG

    config_file = os.environ.get(CONFIG_ENV_VAR, '')
    if config_file:
        try:
            with open(config_file) as f:
                new_config = json.load(f)
                config.update(new_config)
                logger.info(f"Load config from {config_file}, config {config}")
        except FileNotFoundError:
            logger.info(
                f"Failed to load config file, use default config {config}")
    else:
        logger.info(f"Use default config {config}")
    return config


# Returns the protocol version of the binary.
def get_binary_protocol_version(config) -> typing.Optional[int]:
    binary_name = config.get('binary_name', 'neard')
    near_root = config.get('near_root')
    binary_path = os.path.join(near_root, binary_name)

    # Get the protocol version of the binary
    # The --version output looks like this:
    # neard (release trunk) (build 1.1.0-3884-ge93793a61-modified) (rustc 1.71.0) (protocol 137) (db 37)
    out = subprocess.check_output([binary_path, "--version"], text=True)
    out = out.replace('(', '')
    out = out.replace(')', '')
    tokens = out.split()
    n = len(tokens)
    for i in range(n):
        if tokens[i] == "protocol" and i + 1 < n:
            return int(tokens[i + 1])
    return None


def corrupt_state_snapshot(config, node_dir, shard_layout_version):
    near_root = config['near_root']
    binary_name = config.get('binary_name', 'neard')
    binary_path = os.path.join(near_root, binary_name)

    cmd = [
        binary_path,
        "--home",
        node_dir,
        "database",
        "corrupt-state-snapshot",
        "--shard-layout-version",
        str(shard_layout_version),
    ]

    env = os.environ.copy()
    env["RUST_BACKTRACE"] = "1"
    env["RUST_LOG"] = "db=warn,db_opener=warn," + env.get("RUST_LOG", "debug")

    out = subprocess.check_output(cmd, text=True, env=env)

    return out