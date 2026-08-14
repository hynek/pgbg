class SuppressedCrashError(RuntimeError):
    """
    A work factory's context manager swallowed the exception that ended a
    loop run.

    This looks like a clean, silent stop to the supervisor which is almost
    certainly undesired.
    """
