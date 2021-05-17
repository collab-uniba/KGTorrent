Replicating the collection
==========================

To replicate our collection, you first have to download and uncompress the Meta Kaggle version that we used to build the KGTorrent companion database (retrieved on October 27, 2020). Since it is no more available on Kaggle, we share it as part of the `dataset package <https://doi.org/10.5281/zenodo.4468522>`_: it is the compressed archive named ``MetaKaggle27Oct2020.tar.bz2``.

Once Meta Kaggle is available on your machine and the corresponding environment variable has been set, you can start the creation process by issuing the following command::

    python kgtorrent.py init --strategy HTTP

The ``--strategy`` argument determines whether the notebooks will be downloaded via ``HTTP`` requests or via ``API`` calls. Notebooks downloaded via the official Kaggle API miss the output of code cells, while those downloaded via HTTP requests are complete.

Once you start the creation process, KGTorrent will go through the following steps:

1. *Database initialization*: a new MySQL database is created and set up with the data schema required to store Meta Kaggle data.
2. *Meta Kaggle preprocessing*: Meta Kaggle is an archive containing 29 tables in the ``.csv`` file format. As of today, it cannot be imported in a relational database without incurring in referential integrity violations. This happens because many of the tables miss some rows, as they probably contain private information. Our program overcomes this issue by performing a pre-processing step in which rows with unresolved foreing keys are dropped from each Meta Kaggle table.
3. *Database population*: the MySQL database is populated with information from the filtered Meta Kaggle tables.
4. *Notebooks download*: the Jupyter notebooks are downloaded from Kaggle using the preferred strategy (HTTP or API). An SQL query is used to retrieve the list of notebooks to be downaloded from the MySQL database.