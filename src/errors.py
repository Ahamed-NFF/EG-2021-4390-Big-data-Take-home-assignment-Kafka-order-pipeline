"""Error taxonomy for the consumer.

The split between transient and permanent is the whole basis of the retry/DLQ
decision, so it is modelled explicitly rather than inferred from strings:

    TransientError  -> retry with backoff; DLQ only after attempts are exhausted
    PermanentError  -> straight to the DLQ, no retry (retrying cannot help)
"""


class OrderProcessingError(Exception):
    """Base class for anything that stops a record from being processed."""


class TransientError(OrderProcessingError):
    """A failure that is expected to clear on its own.

    Downstream database timeout, socket reset, HTTP 503 from a pricing service.
    Worth retrying because the input is fine and only the environment failed.
    """


class PermanentError(OrderProcessingError):
    """A failure that will recur identically on every attempt.

    Retrying wastes time and delays the rest of the partition, so these go to
    the DLQ immediately.
    """


class DeserializationError(PermanentError):
    """The bytes on the topic are not a record this consumer can decode.

    Corrupt payload, wrong magic bytes, or a schema fingerprint this consumer
    has never seen. No amount of retrying will turn bad bytes into an Order.
    """


class ValidationError(PermanentError):
    """The record decoded cleanly but breaks a business rule.

    A negative price is a real Avro float, so Avro accepts it; the domain does
    not. The record is poison and belongs in the DLQ for a human to look at.
    """
