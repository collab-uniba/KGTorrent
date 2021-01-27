Refreshing the collection
=========================

To refresh the collection - i.e., to align the dataset to the current state of Kaggle - download the latest version of Meta Kaggle.

Once Meta Kaggle has been downloaded on your machine and the corresponding environment variable has been set, you can start the refresh process by issuing the following command::

    python kgtorrent.py refresh --strategy HTTP

A new MySQL database will be created and populated with the information from the lastest Meta Kaggle. *Warning*: if a database with the same name already exists, it will be overwritten. Then the download procedure will start; this time, the list of notebooks to be downloaded will be checked against the files that are already present in the dataset folder: notebooks that are already locally available will not be downloaded.
Moreover, notebooks from the previous version of KGTorrent that are no more referenced in the refreshed database will be deleted. Indeed, it can happen that notebooks get deleted from the platform and loose their reference in Meta Kaggle.