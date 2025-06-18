from .typings import Jsons


class ArangoRDFException(Exception):
    """Base exception for ArangoRDF."""

    pass


class ArangoRDFImportException(ArangoRDFException):
    """Exception for import errors."""

    def __init__(self, error: str, collection: str, documents: Jsons) -> None:
        """Initialize import exception.

        :param error: The error message from the failed import
        :type error: str
        :param collection: The collection that failed to import
        :type collection: str
        :param documents: The batch of documents that failed to import
        :type documents: list
        """
        self.error = error
        self.collection = collection
        self.documents = documents
        super().__init__(
            f"Import error in {collection}: {error}. Failed documents: {documents}"
        )
