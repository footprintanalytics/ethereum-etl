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


import itertools
import json
from collections import defaultdict
from ethereumetl.slack import send_to_slack

from ethereumetl.enumeration.chain_type import ChainType
from ethereumetl.misc.retriable_value_error import RetriableValueError


def join(left, right, join_fields, left_fields, right_fields):
    left_join_field, right_join_field = join_fields

    def field_list_to_dict(field_list):
        result_dict = {}
        for field in field_list:
            if isinstance(field, tuple):
                result_dict[field[0]] = field[1]
            else:
                result_dict[field] = field
        return result_dict

    left_fields_as_dict = field_list_to_dict(left_fields)
    right_fields_as_dict = field_list_to_dict(right_fields)

    left_map = defaultdict(list)
    for item in left:
        left_map[item[left_join_field]].append(item)

    right_map = defaultdict(list)
    for item in right:
        right_map[item[right_join_field]].append(item)

    for key in left_map.keys():
        for left_item, right_item in itertools.product(left_map[key], right_map[key]):
            result_item = {}
            for src_field, dst_field in left_fields_as_dict.items():
                result_item[dst_field] = left_item.get(src_field)
            for src_field, dst_field in right_fields_as_dict.items():
                result_item[dst_field] = right_item.get(src_field)

            yield result_item


def multi_join(left, right, join_fields, left_fields, right_fields):
    def field_list_to_dict(field_list):
        result_dict = {}
        for field in field_list:
            if isinstance(field, tuple):
                result_dict[field[0]] = field[1]
            else:
                result_dict[field] = field
        return result_dict

    left_join_fields = join_fields[0]
    right_join_fields = join_fields[1]

    left_fields_as_dict = field_list_to_dict(left_fields)
    right_fields_as_dict = field_list_to_dict(right_fields)

    left_map = defaultdict(list)
    for item in left:
        key = tuple(item[field] for field in left_join_fields)
        left_map[key].append(item)

    right_map = defaultdict(list)
    for item in right:
        key = tuple(item[field] for field in right_join_fields)
        right_map[key].append(item)

    for key in set(left_map.keys()) & set(right_map.keys()):
        for left_item, right_item in itertools.product(left_map[key], right_map[key]):
            result_item = {}
            for src_field, dst_field in left_fields_as_dict.items():
                result_item[dst_field] = left_item.get(src_field)
            for src_field, dst_field in right_fields_as_dict.items():
                result_item[dst_field] = right_item.get(src_field)

            yield result_item


def enrich_transactions(transactions, receipts):
    result = list(join(
        transactions, receipts, ('hash', 'transaction_hash'),
        left_fields=[
            'type',
            'hash',
            'nonce',
            'transaction_index',
            'from_address',
            'to_address',
            'value',
            'gas',
            'gas_price',
            'input',
            'block_timestamp',
            'block_number',
            'block_hash',
            'max_fee_per_gas',
            'max_priority_fee_per_gas',
            'transaction_type'
        ],
        right_fields=[
            ('cumulative_gas_used', 'receipt_cumulative_gas_used'),
            ('gas_used', 'receipt_gas_used'),
            ('contract_address', 'receipt_contract_address'),
            ('root', 'receipt_root'),
            ('status', 'receipt_status'),
            ('effective_gas_price', 'receipt_effective_gas_price'),
            ('l1_fee', 'receipt_l1_fee'),
            ('l1_gas_used', 'receipt_l1_gas_used'),
            ('l1_gas_price', 'receipt_l1_gas_price'),
            ('l1_fee_scalar', 'receipt_l1_fee_scalar')

        ]))

    if len(result) != len(transactions):
        block_numbers = set([tx['block_number'] for tx in transactions])
        message = format_message('transactions', transactions, result, block_numbers)
        send_to_slack(message)
        raise ValueError(message)

    return result


def enrich_logs(blocks, logs):
    result = list(join(
        logs, blocks, ('block_number', 'number'),
        [
            'type',
            'log_index',
            'transaction_hash',
            'transaction_index',
            'address',
            'data',
            'topics',
            'block_number'
        ],
        [
            ('timestamp', 'block_timestamp'),
            ('hash', 'block_hash'),
        ]))

    if len(result) != len(logs):
        block_numbers = set([log['block_number'] for log in logs])
        message = format_message('logs', logs, result, block_numbers)
        send_to_slack(message)
        raise ValueError(message)

    return result


def enrich_token_transfers(blocks, token_transfers):
    result = list(join(
        token_transfers, blocks, ('block_number', 'number'),
        [
            'type',
            'token_address',
            'from_address',
            'to_address',
            'value',
            'transaction_hash',
            'log_index',
            'block_number'
        ],
        [
            ('timestamp', 'block_timestamp'),
            ('hash', 'block_hash'),
        ]))

    if len(result) != len(token_transfers):
        block_numbers = set([token_transfer['block_number'] for token_transfer in token_transfers])
        message = format_message('token_transfers', token_transfers, result, block_numbers)
        send_to_slack(message)
        raise ValueError(message)

    return result


def enrich_traces(blocks, traces, chain=None):
    result = list(join(
        traces, blocks, ('block_number', 'number'),
        [
            'type',
            'transaction_index',
            'from_address',
            'to_address',
            'value',
            'input',
            'output',
            'trace_type',
            'call_type',
            'reward_type',
            'gas',
            'gas_used',
            'subtraces',
            'trace_address',
            'error',
            'status',
            'transaction_hash',
            'block_number',
            'trace_id',
            'trace_index'
        ],
        [
            ('timestamp', 'block_timestamp'),
            ('hash', 'block_hash'),
        ]))

    if len(result) != len(traces):
        block_numbers = set([trace['block_number'] for trace in traces])
        message = format_message('traces', traces, result, block_numbers)
        send_to_slack(message)
        raise ValueError(message)

    # polygon 不做这个检查
    if chain == ChainType.POLYGON:
        return result

    traces_transaction_count = 0
    for trace in traces:
        if trace['trace_address'] == [] and trace['transaction_hash'] is not None:
            traces_transaction_count += 1

    blocks_transaction_count = sum(block['transaction_count'] for block in blocks)
    if traces_transaction_count != blocks_transaction_count:
        print(f'traces transactions count is wrong, block_number: {blocks[0]["number"]}, '
              f'blocks_transaction_count: {blocks_transaction_count}, '
              f'traces_transaction_count: {traces_transaction_count}')
        # raise RetriableValueError('traces transactions count is wrong')

    return result


def enrich_geth_traces(blocks, transactions, traces):
    traces_fields = [
        'type',
        'transaction_index',
        'from_address',
        'to_address',
        'value',
        'input',
        'output',
        'trace_type',
        'call_type',
        'reward_type',
        'gas',
        'gas_used',
        'subtraces',
        'trace_address',
        'error',
        'status',
        'transaction_hash',
        'block_number',
        'trace_id',
        'trace_index'
    ]
    traces_blocks = list(join(
        traces, blocks, ('block_number', 'number'),
        traces_fields,
        [
            ('timestamp', 'block_timestamp'),
            ('hash', 'block_hash'),
        ]))

    result = list(multi_join(
        traces_blocks, transactions, (['block_number', 'transaction_index'], ['block_number', 'transaction_index']),
        traces_fields + ['block_timestamp', 'block_hash'],
        [
            ('hash', 'transaction_hash'),
            'transaction_index',
        ]))

    if len(result) != len(traces):
        block_numbers = set([trace['block_number'] for trace in traces])
        message = format_message('geth_traces', traces, result, block_numbers)
        send_to_slack(message)
        raise ValueError(message)

    return result


def enrich_contracts(blocks, contracts):
    result = list(join(
        contracts, blocks, ('block_number', 'number'),
        [
            'type',
            'address',
            'bytecode',
            'function_sighashes',
            'is_erc20',
            'is_erc721',
            'block_number'
        ],
        [
            ('timestamp', 'block_timestamp'),
            ('hash', 'block_hash'),
        ]))

    if len(result) != len(contracts):
        block_numbers = set([contract['block_number'] for contract in contracts])
        message = format_message('contracts', contracts, result, block_numbers)
        send_to_slack(message)
        raise ValueError(message)

    return result


def enrich_tokens(blocks, tokens):
    result = list(join(
        tokens, blocks, ('block_number', 'number'),
        [
            'type',
            'address',
            'symbol',
            'name',
            'decimals',
            'total_supply',
            'block_number'
        ],
        [
            ('timestamp', 'block_timestamp'),
            ('hash', 'block_hash'),
        ]))

    if len(result) != len(tokens):
        block_numbers = set([token['block_number'] for token in tokens])
        message = format_message('tokens', tokens, result, block_numbers)
        send_to_slack(message)
        raise ValueError(message)

    return result


def format_message(type, should_result, got_result, block_numbers):
    return f'The number of {type} is wrong: should be {len(should_result)}, but got {len(got_result)}, retrying for blocks:{block_numbers}...'
