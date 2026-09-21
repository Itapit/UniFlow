class BatchHandler:
    """Unpacks one SymbolBatch into its individual Packets and forwards
    each to the aggregator, stamping receiver_id from the batch onto
    every packet inside it. This is the callback receiver_server.py's
    connection threads call directly for each batch they parse.
    """

    def __init__(self, aggregator):
        self._aggregator = aggregator

    def handle_batch(self, batch):
        for packet in batch.packets:
            self._aggregator.add_packet(batch.receiver_id, packet)