from src.log_setup import get_logger

log = get_logger("batch_handler")


class BatchHandler:
    """Unpacks one SymbolBatch into its individual Packets and forwards
    each to the aggregator, stamping receiver_id from the batch onto
    every packet inside it. This is the callback receiver_server.py's
    connection threads call directly for each batch they parse.
    """

    def __init__(self, aggregator):
        self._aggregator = aggregator

    def handle_batch(self, batch):
        log.debug("event=batch_unpack receiver_id=%s packets=%d",
                  batch.receiver_id, len(batch.packets))
        for packet in batch.packets:
            self._aggregator.add_packet(batch.receiver_id, packet)
