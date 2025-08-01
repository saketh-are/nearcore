#!/usr/bin/env python3
# This test checks the ultimate undercharging scenario where a chunk takes
# long time to apply but consumes little gas. This is to simulate real
# undercharging in a more controlled manner.

import sys
import json
import unittest
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from transaction import sign_deploy_contract_tx, sign_function_call_tx
from configured_logger import logger
from cluster import start_cluster
from utils import load_test_contract, poll_blocks

TGAS = 10**12


class SlowChunkTest(unittest.TestCase):

    # Spin up multiple validator nodes in a multi shard chain. Deploy a contract
    # to one of the shards and call a function that sleeps for a long time.
    # Check that the shard is able to recover and that new chunks appear.
    def test(self):
        # The number of validators and the number of shards.
        n = 4

        # The validator nodes should used single shard tracking. The nodes with
        # indices in range [0, n) are validators.
        val_client_config_changes = {
            i: {
                "tracked_shards_config": "NoShards"
            } for i in range(n)
        }
        # The rpc node should track all shards. The node with index n is the rpc node.
        rpc_client_config_changes = {n: {"tracked_shards_config": "AllShards"}}

        # Combine the configs changes for validators and rpc.
        client_config_changes = {
            **val_client_config_changes,
            **rpc_client_config_changes,
        }

        # Configure long epoch to not worry about full epoch without chunks.
        genesis_config_changes = [["epoch_length", 100]]
        [node1, node2, node3, node4, rpc] = start_cluster(
            n,
            1,
            n,
            None,
            genesis_config_changes,
            client_config_changes,
        )

        # The chain is slow to warm up. Wait until the chain is ready otherwise
        # the missing chunks congestion will kick in due to missing blocks.
        list(poll_blocks(rpc, __target=10))

        self.__deploy_contract(rpc)

        tx_hash = self.__call_contract(rpc)

        # Wait for a missing chunk.
        missingChunk = False
        for height, hash in poll_blocks(rpc, __target=50):
            chunk_mask = self.__get_chunk_mask(rpc, hash)
            logger.info(f"#{height} chunk mask: {chunk_mask}")

            if not all(chunk_mask):
                logger.info("Successfully caused missing chunks.")
                missingChunk = True
                break

        self.assertTrue(missingChunk)

        # Wait until the chain recovers and all chunks are present.
        recovered = False
        for height, hash in poll_blocks(rpc, __target=50):
            chunk_mask = self.__get_chunk_mask(rpc, hash)
            logger.info(f"#{height} chunk mask: {chunk_mask}")

            if all(chunk_mask):
                logger.info("The chain recovered. All chunks are present.")
                recovered = True
                break

        self.assertTrue(recovered)

        # Check that the function call did succeed
        self.__check_call_result(rpc, tx_hash)

    def __deploy_contract(self, node):
        logger.info("Deploying contract.")

        block_hash = node.get_latest_block().hash_bytes
        contract = load_test_contract('rs_contract.wasm')

        tx = sign_deploy_contract_tx(node.signer_key, contract, 10, block_hash)
        node.send_tx(tx)

    def __call_contract(self, node):
        logger.info("Calling contract.")

        block_hash = node.get_latest_block().hash_bytes

        # duration is measured in nanoseconds
        second = int(1e9)
        duration_nanos = 5 * second
        duration_bytes = duration_nanos.to_bytes(8, byteorder="little")

        tx = sign_function_call_tx(
            node.signer_key,
            node.signer_key.account_id,
            'sleep',
            duration_bytes,
            150 * TGAS,
            1,
            20,
            block_hash,
        )
        # asynchronously send the transaction, since we expect delays which
        # might be more than the pooling timeout on the RPC
        result = node.send_tx(tx)

        self.assertIn('result', result, result)
        logger.debug(json.dumps(result, indent=2))
        return result['result']

    def __check_call_result(self, node, tx_hash):
        result = node.get_tx(tx_hash, node.signer_key.account_id)
        self.assertIn('result', result, result)
        self.assertIn('status', result['result'])
        self.assertIn('SuccessValue', result['result']['status'])

    def __get_chunk_mask(self, node, block_hash):
        block = node.json_rpc("block", {"block_id": block_hash})
        return block['result']['header']['chunk_mask']


if __name__ == '__main__':
    unittest.main()
