class PipelineError(Exception):
    """Raised by stage handlers when their try/except packaged the failure.

    Distinguishes Code failures (handler ran) from Runtime failures
    (Lambda runtime killed the process before any handler code) in the
    failure_handling Lambda's classification step. See ADR 0003.
    """
