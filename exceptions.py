class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class TableNotPreprocessedError(Error):
    """Exception raised when the table that was just read was supposed to be already preprocessed
    (and serialized to .bz2 pickle file) but is not.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
