import logging

from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from ethereumetl.enumeration.chain_type import ChainType
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.export_geth_traces_job import ExportGethTracesJob
from ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ethereumetl.jobs.export_traces_job import ExportTracesJob
from ethereumetl.jobs.extract_contracts_job import ExtractContractsJob
from ethereumetl.jobs.extract_geth_traces_job import ExtractGethTracesJob
from ethereumetl.jobs.extract_token_transfers_job import ExtractTokenTransfersJob
from ethereumetl.jobs.extract_tokens_job import ExtractTokensJob
from ethereumetl.mappers.receipt_log_mapper import EthReceiptLogMapper
from ethereumetl.misc.retriable_value_error import RetriableValueError
from ethereumetl.providers.multi_batch_rpc import BatchMultiHTTPProvider
from ethereumetl.service.token_transfer_extractor import TRANSFER_EVENT_TOPIC
from ethereumetl.streaming.enrich import enrich_transactions, enrich_logs, enrich_token_transfers, enrich_traces, \
    enrich_contracts, enrich_tokens, enrich_geth_traces
from ethereumetl.streaming.eth_item_id_calculator import EthItemIdCalculator
from ethereumetl.streaming.eth_item_timestamp_calculator import EthItemTimestampCalculator
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.web3_utils import build_web3


class EthStreamerAdapter:
    def __init__(
            self,
            chain,
            batch_web3_provider,
            item_exporter=ConsoleItemExporter(),
            batch_size=100,
            max_workers=5,
            entity_types=tuple(EntityType.ALL_FOR_STREAMING),
            geth_traces_provider=None):
        self.chain = chain
        self.batch_web3_provider = batch_web3_provider
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.entity_types = entity_types
        self.item_id_calculator = EthItemIdCalculator()
        self.item_timestamp_calculator = EthItemTimestampCalculator()
        self.geth_traces_provider = geth_traces_provider

    def open(self):
        self.item_exporter.open()

    def get_current_block_number(self):
        if isinstance(self.batch_web3_provider, ThreadLocalProxy):
            http_provider = self.batch_web3_provider._get_thread_local_delegate()
            if isinstance(http_provider, BatchMultiHTTPProvider):
                http_provider.endpoint_uri = http_provider.endpoint_manager.get_next_active_endpoint().endpoint_url
        w3 = build_web3(self.batch_web3_provider)
        return int(w3.eth.getBlock("latest").number)

    def export_all(self, start_block, end_block):
        # Export blocks and transactions
        blocks, transactions = [], []
        if self._should_export(EntityType.BLOCK) or self._should_export(EntityType.TRANSACTION):
            blocks, transactions = self._export_blocks_and_transactions(start_block, end_block)

        # Export receipts and logs
        receipts, logs = [], []
        if self._should_export(EntityType.RECEIPT) or self._should_export(EntityType.LOG):
            receipts, logs = self._export_receipts_and_logs(transactions)

        # Extract token transfers
        token_transfers = []
        if self._should_export(EntityType.TOKEN_TRANSFER):
            token_transfers = self._extract_token_transfers(logs)

        # Export traces
        traces = []
        if self._should_export(EntityType.TRACE):
            traces = self._export_traces(start_block, end_block)
        geth_traces = []
        if self._should_export(EntityType.GETH_TRACES):
            geth_traces = self._export_geth_traces(start_block, end_block)
            geth_traces = self._extract_geth_traces(geth_traces)

        # Export contracts
        contracts = []
        if self._should_export(EntityType.CONTRACT):
            contracts = self._export_contracts(traces)

        # Export tokens
        tokens = []
        if self._should_export(EntityType.TOKEN):
            tokens = self._extract_tokens(contracts)

        enriched_blocks = blocks \
            if EntityType.BLOCK in self.entity_types else []
        enriched_transactions = enrich_transactions(transactions, receipts) \
            if EntityType.TRANSACTION in self.entity_types else []
        enriched_logs = enrich_logs(blocks, logs) \
            if EntityType.LOG in self.entity_types else []
        enriched_token_transfers = enrich_token_transfers(blocks, token_transfers) \
            if EntityType.TOKEN_TRANSFER in self.entity_types else []
        enriched_traces = enrich_traces(blocks, traces, self.chain) \
            if EntityType.TRACE in self.entity_types else []
        enriched_geth_traces = enrich_geth_traces(blocks, transactions, geth_traces) \
            if EntityType.GETH_TRACES in self.entity_types else []
        enriched_contracts = enrich_contracts(blocks, contracts) \
            if EntityType.CONTRACT in self.entity_types else []
        enriched_tokens = enrich_tokens(blocks, tokens) \
            if EntityType.TOKEN in self.entity_types else []

        logging.info('Exporting with ' + type(self.item_exporter).__name__)

        all_items = \
            sort_by(enriched_blocks, 'number') + \
            sort_by(enriched_transactions, ('block_number', 'transaction_index')) + \
            sort_by(enriched_logs, ('block_number', 'log_index')) + \
            sort_by(enriched_token_transfers, ('block_number', 'log_index')) + \
            sort_by(enriched_traces, ('block_number', 'trace_index')) + \
            sort_by(enriched_geth_traces, ('block_number', 'trace_index')) + \
            sort_by(enriched_contracts, ('block_number',)) + \
            sort_by(enriched_tokens, ('block_number',))

        self.calculate_item_ids(all_items)
        self.calculate_item_timestamps(all_items)

        self.item_exporter.export_items(all_items)

    def _export_blocks_and_transactions(self, start_block, end_block):
        blocks_and_transactions_item_exporter = InMemoryItemExporter(item_types=['block', 'transaction'])
        blocks_and_transactions_job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=blocks_and_transactions_item_exporter,
            export_blocks=self._should_export(EntityType.BLOCK),
            export_transactions=self._should_export(EntityType.TRANSACTION)
        )
        blocks_and_transactions_job.run()
        blocks = blocks_and_transactions_item_exporter.get_items('block')
        transactions = blocks_and_transactions_item_exporter.get_items('transaction')

        if EntityType.TRANSACTION in self.entity_types:
            self.verify_transaction_from_address_nonce(blocks, transactions)
            self.verify_transaction_count(blocks, transactions)

        return blocks, transactions

    def verify_transaction_from_address_nonce(self, blocks, transactions):
        # 将 transactions 按 block_number 分组
        transaction_group = {}
        for transaction in transactions:
            block_number = transaction['block_number']
            if transaction_group.get(block_number) is None:
                transaction_group[block_number] = []
            if self.chain == ChainType.POLYGON \
                    and transaction['to_address'] == '0x0000000000000000000000000000000000000000':
                continue
            if self.chain == ChainType.FANTOM \
                    and transaction['to_address'] == '0xd100a01e00000000000000000000000000000000':
                continue
            transaction_group[block_number].append(transaction['from_address'])

        for block in blocks:
            block_number = block['number']
            from_address_list = transaction_group.get(block_number, [])
            from_address_set = list(set(from_address_list))
            if len(from_address_set) == 1 and from_address_set[0] == '0x0000000000000000000000000000000000000000' \
                    and block['transaction_count'] > 1:
                raise RetriableValueError(f'Transactions within a block should not all be 0x0000000, '
                                          f'block_number: {block_number}')
            # 判断 from_address_list 里 0x0000000000000000000000000000000000000000 的占比
            nonce_address_count = 0
            for from_address in from_address_list:
                if from_address == '0x0000000000000000000000000000000000000000':
                    nonce_address_count += 1
            # 如果 0x000 个数占比超过 50%，则报错重试
            if len(from_address_list) > 0 and nonce_address_count / len(from_address_list) > 0.5:
                raise RetriableValueError(f'Transactions within a block should not have more than 50% 0x0000000, '
                                          f'block_number: {block_number}')

    def verify_transaction_count(self, blocks, transactions):
        blocks_transaction_count = 0
        for block in blocks:
            blocks_transaction_count += block['transaction_count']
        if blocks_transaction_count != len(transactions):
            raise RetriableValueError(f'Transaction count mismatch, '
                                      f'blocks_transaction_count: {blocks_transaction_count}, '
                                      f'transactions_count: {len(transactions)}')


    def _export_receipts_and_logs(self, transactions):
        exporter = InMemoryItemExporter(item_types=['receipt', 'log'])
        job = ExportReceiptsJob(
            transaction_hashes_iterable=(transaction['hash'] for transaction in transactions),
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=exporter,
            export_receipts=self._should_export(EntityType.RECEIPT),
            export_logs=self._should_export(EntityType.LOG)
        )
        job.run()
        receipts = exporter.get_items('receipt')
        logs = exporter.get_items('log')
        return receipts, logs

    def _extract_token_transfers(self, logs):
        def verify_token_transfers(logs, token_transfers):
            token_transfers_in_logs_count = 0
            receipt_log_mapper = EthReceiptLogMapper()
            for log_dict in logs:
                log = receipt_log_mapper.dict_to_receipt_log(log_dict)
                topics = log.topics
                if topics is None or len(topics) < 1:
                    continue
                if (topics[0]).casefold() == TRANSFER_EVENT_TOPIC:
                    token_transfers_in_logs_count += 1
            if len(token_transfers) != token_transfers_in_logs_count:
                raise RuntimeError('Token transfers count mismatch: '
                                   'token_transfers={}, token_transfers_in_logs={}'.format(
                    len(token_transfers), token_transfers_in_logs_count))

        exporter = InMemoryItemExporter(item_types=['token_transfer'])
        job = ExtractTokenTransfersJob(
            logs_iterable=logs,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter)
        job.run()
        token_transfers = exporter.get_items('token_transfer')
        verify_token_transfers(logs, token_transfers)
        return token_transfers

    def _export_traces(self, start_block, end_block):
        exporter = InMemoryItemExporter(item_types=['trace'])
        job = ExportTracesJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            web3=ThreadLocalProxy(lambda: build_web3(self.batch_web3_provider)),
            max_workers=self.max_workers,
            item_exporter=exporter
        )
        job.run()
        traces = exporter.get_items('trace')
        return traces

    def _export_geth_traces(self, start_block, end_block):
        exporter = InMemoryItemExporter(item_types=['geth_trace'])
        job = ExportGethTracesJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            batch_web3_provider=self.geth_traces_provider,
            max_workers=self.max_workers,
            item_exporter=exporter
        )
        job.run()
        geth_traces = exporter.get_items('geth_trace')
        return geth_traces

    def _extract_geth_traces(self, geth_traces):
        """convert geth_traces to python iterable"""
        traces_iterable = (trace for trace in geth_traces)

        exporter = InMemoryItemExporter(item_types=['trace'])
        job = ExtractGethTracesJob(
            traces_iterable=traces_iterable,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter
        )
        job.run()
        geth_traces = exporter.get_items('trace')
        return geth_traces

    def _export_contracts(self, traces):
        exporter = InMemoryItemExporter(item_types=['contract'])
        job = ExtractContractsJob(
            traces_iterable=traces,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter
        )
        job.run()
        contracts = exporter.get_items('contract')
        return contracts

    def _extract_tokens(self, contracts):
        exporter = InMemoryItemExporter(item_types=['token'])
        job = ExtractTokensJob(
            contracts_iterable=contracts,
            web3=ThreadLocalProxy(lambda: build_web3(self.batch_web3_provider)),
            max_workers=self.max_workers,
            item_exporter=exporter
        )
        job.run()
        tokens = exporter.get_items('token')
        return tokens

    def _should_export(self, entity_type):
        if entity_type == EntityType.BLOCK:
            return True

        if entity_type == EntityType.TRANSACTION:
            return EntityType.TRANSACTION in self.entity_types or self._should_export(EntityType.LOG) \
                   or EntityType.GETH_TRACES in self.entity_types

        if entity_type == EntityType.RECEIPT:
            return EntityType.TRANSACTION in self.entity_types or self._should_export(EntityType.TOKEN_TRANSFER)

        if entity_type == EntityType.LOG:
            return EntityType.LOG in self.entity_types or self._should_export(EntityType.TOKEN_TRANSFER)

        if entity_type == EntityType.TOKEN_TRANSFER:
            return EntityType.TOKEN_TRANSFER in self.entity_types

        if entity_type == EntityType.TRACE:
            return EntityType.TRACE in self.entity_types or self._should_export(EntityType.CONTRACT)

        if entity_type == EntityType.GETH_TRACES:
            return EntityType.GETH_TRACES in self.entity_types

        if entity_type == EntityType.CONTRACT:
            return EntityType.CONTRACT in self.entity_types or self._should_export(EntityType.TOKEN)

        if entity_type == EntityType.TOKEN:
            return EntityType.TOKEN in self.entity_types

        raise ValueError('Unexpected entity type ' + entity_type)

    def calculate_item_ids(self, items):
        for item in items:
            item['item_id'] = self.item_id_calculator.calculate(item)

    def calculate_item_timestamps(self, items):
        for item in items:
            item['item_timestamp'] = self.item_timestamp_calculator.calculate(item)

    def close(self):
        self.item_exporter.close()


def sort_by(arr, fields):
    if isinstance(fields, tuple):
        fields = tuple(fields)
    return sorted(arr, key=lambda item: tuple(item.get(f) for f in fields))
