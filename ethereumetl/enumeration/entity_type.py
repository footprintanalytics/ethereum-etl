class EntityType:
    BLOCK = 'block'
    TRANSACTION = 'transaction'
    RECEIPT = 'receipt'
    LOG = 'log'
    TOKEN_TRANSFER = 'token_transfer'
    TRACE = 'trace'
    GETH_TRACES = 'geth_traces'
    CONTRACT = 'contract'
    TOKEN = 'token'

    ALL_FOR_STREAMING = [BLOCK, TRANSACTION, LOG, TOKEN_TRANSFER, TRACE]
    ALL_FOR_STREAMING_BASIC = [BLOCK, TRANSACTION, LOG, TOKEN_TRANSFER]
    ALL_FOR_INFURA = [BLOCK, TRANSACTION, LOG, TOKEN_TRANSFER]
