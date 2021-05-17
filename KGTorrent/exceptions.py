"""This module defines exceptions that might be risen while building/updating KGTorrent using this package."""

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

class DatabaseExistsError(Error):
    """Exception raised when the database name already exists and has not been forced to remove.

    Attributes:
        message (str): short message containing the explanation of the error.
    """

    def __init__(self, message):
        self.message = message
