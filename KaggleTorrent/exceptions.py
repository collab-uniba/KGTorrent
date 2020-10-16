"""This module defines exceptions that might be risen while building/updating KaggleTorrent using this package."""

class Error(Exception):
    """Base class for exceptions in this package."""
    pass


class TableNotPreprocessedError(Error):
    """Exception raised when the table that was just read was supposed to be already preprocessed
    (and serialized to .bz2 pickle file) but is not.

    Attributes:
        message (str): short message containing the explanation of the error.
    """

    def __init__(self, message):
        self.message = message
