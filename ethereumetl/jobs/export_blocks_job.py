# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import json

from ethereumetl.enumeration.chain_type import ChainType
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.json_rpc_requests import generate_get_block_by_number_json_rpc
from ethereumetl.mappers.block_mapper import EthBlockMapper
from ethereumetl.mappers.transaction_mapper import EthTransactionMapper
from ethereumetl.misc.retriable_value_error import RetriableValueError
from ethereumetl.utils import rpc_response_batch_to_results, validate_range, extract_domain, extract_rpc


# Exports blocks and transactions
class ExportBlocksJob(BaseJob):
    def __init__(
            self,
            start_block,
            end_block,
            batch_size,
            batch_web3_provider,
            max_workers,
            item_exporter,
            export_blocks=True,
            export_transactions=True,
            chain=ChainType.ETHEREUM):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.batch_web3_provider = batch_web3_provider

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.export_blocks = export_blocks
        self.export_transactions = export_transactions
        if not self.export_blocks and not self.export_transactions:
            raise ValueError('At least one of export_blocks or export_transactions must be True')

        self.block_mapper = EthBlockMapper()
        self.transaction_mapper = EthTransactionMapper()
        self.chain = chain

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
            total_items=self.end_block - self.start_block + 1
        )

    def _export_batch(self, block_number_batch):
        blocks_rpc = list(generate_get_block_by_number_json_rpc(block_number_batch, self.export_transactions))
        response = self.batch_web3_provider.make_batch_request(json.dumps(blocks_rpc))
        results = rpc_response_batch_to_results(response)
        response_rpc = extract_rpc(response)
        blocks = [self.block_mapper.json_dict_to_block(result) for result in results]
        self.verify_blocks(blocks, response_rpc)
        for block in blocks:
            self._export_block(block, response_rpc)

    def _export_block(self, block, rpc):
        rpc_domain = extract_domain(rpc)
        if self.export_blocks:
            block_data = self.block_mapper.block_to_dict(block)
            block_data['rpc'] = rpc_domain
            self.item_exporter.export_item(block_data)
        if self.export_transactions:
            self.verify_transactions(block, rpc)
            for tx in block.transactions:
                tx_data = self.transaction_mapper.transaction_to_dict(tx)
                tx_data['rpc'] = rpc_domain
                self.item_exporter.export_item(tx_data)

    def verify_blocks(self, blocks, rpc):
        min_block_number = min([block.number for block in blocks])
        max_block_number = max([block.number for block in blocks])
        if len(blocks) != max_block_number - min_block_number + 1:
            raise RetriableValueError(f'rpc: {rpc}, len(blocks) {len(blocks)} != max_block_number - min_block_number '
                                      f'+ 1 {max_block_number - min_block_number + 1}')

    def verify_transactions(self, block, rpc):
        self.verify_transaction_unique(block, rpc)
        self.verify_transaction_conut(block, rpc)
        self.verify_transaction_from_address_nonce(block, rpc)

    def verify_transaction_unique(self, block, rpc):
        transactions = block.transactions
        transaction_hash_list = []
        for transaction in transactions:
            transaction_hash_list.append(transaction.hash)
        transaction_hash_set = list(set(transaction_hash_list))
        if len(transaction_hash_set) != len(transaction_hash_list):
            raise RetriableValueError(f'rpc: {rpc}, Transaction hash is not unique, '
                                      f'block_number: {block.number}')

    def verify_transaction_conut(self, block, rpc):
        if block.transaction_count != len(block.transactions):
            raise RetriableValueError(f'rpc: {rpc}, block.transaction_count {block.transaction_count} != len('
                                      f'block.transactions) {len(block.transactions)}')

    def verify_transaction_from_address_nonce(self, block, rpc):
        if block.transaction_count <= 1:
            return
        transactions = block.transactions
        from_address_list = []
        for transaction in transactions:
            if self.chain == ChainType.POLYGON \
                    and transaction.to_address == '0x0000000000000000000000000000000000000000':
                continue
            if self.chain == ChainType.FANTOM \
                    and transaction.to_address == '0xd100a01e00000000000000000000000000000000':
                continue
            from_address_list.append(transaction.from_address)
        from_address_set = list(set(from_address_list))
        if len(from_address_set) == 1 and from_address_set[0] == '0x0000000000000000000000000000000000000000' \
                and block.transaction_count > 1:
            raise RetriableValueError(f'rpc: {rpc}, Transactions within a block should not all be 0x0000000, '
                                      f'block_number: {block.number}')
        # 同一个 block 内 from_address 是 0x0000 的数量超过 50% 时，则此 block 为异常 block
        nonce_address_count = 0
        for from_address in from_address_list:
            if from_address == '0x0000000000000000000000000000000000000000':
                nonce_address_count += 1
        if len(from_address_list) > 0 and nonce_address_count / len(
                from_address_list) > 0.5 and block.transaction_count > 10:
            raise RetriableValueError(f'rpc: {rpc}, Transactions within a block should not have more than 50% 0x0000000, '
                                      f'block_number: {block.number}')

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
