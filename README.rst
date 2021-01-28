KGTorrent
=========

    *TL/DR*: `KGTorrent <http://neo.di.uniba.it:8080/share.cgi?ssid=0syF2vm>`_ is a large dataset of Jupyter notebooks with rich metadata retrieved from `Kaggle <https://www.kaggle.com>`_. This repository contains the Python scripts developed to create and refresh the collection.

Given their growing popularity among data scientists, computational notebooks - and in particular `Jupyter notebooks <https://jupyter.org>`_ - are being increasingly studied by researchers worldwide. Generally, the aim is to understand how they are typically used, identify possible flaws, and inform the design of extensions and updates of the tool. To ease these kind of research endeavors, we collected and shared a large dataset of 248,761 Jupyter notebooks from Kaggle, named `KGTorrent <http://neo.di.uniba.it:8080/share.cgi?ssid=0syF2vm>`_.

`Kaggle <https://www.kaggle.com>`_ is a web platform hosting machine learning competitions that enables the creation and execution of Jupyter notebooks in a containerized computational environment. By leveraging `Meta Kaggle <https://www.kaggle.com/kaggle/meta-kaggle>`_, a dataset that is publicly available on the platform, we also built a companion MySQL database containing metadata on the notebooks in our dataset.

This repository hosts the Python scripts we developed to create KGTorrent. By leveraging the latest version of Meta Kaggle, the same scripts can also be used to refresh the collection.

For further details, please visit the `full documentation of KGTorrent <https://collab-uniba.github.io/KGTorrent/>`_.

Configuration
-------------

**Requirements**

For the development of KGTorrent we used the following environment:

- ``MySQL 5.7.11+``
- ``Conda 4.9.2+``
- ``Python 3.7+``

**Environment setup**

To execute the scripts in this repository, first make sure to have a running installation of MySQL (v. 5.7.11 or later). In case you don't, follow the `official guide <https://dev.mysql.com/doc/mysql-installation-excerpt/8.0/en/>`_ to install the DBMS.
Then, create a new Conda environment and install the Python dependencies of this project (reported in ``environment.yml``)::

    conda <environment name> create -f environment.yml

Once the environment is ready, activate it using the following Conda command::

    conda activate <environment name>

Finally, this project requires a set of environment variables to be defined. You could customize the template file ``/.env`` and source it from your terminal session to setup all the required variables at once.

Here we provide definitions for each required environment variable.

``DB_HOST``
    The address of the machine hosting your MySQL installation (``localhost`` if the DBMS is executed locally).

``DB_PORT``
    The connection port to your MySQL installation.

``DB_NAME``
    The name of the MySQL database where KGTorrent metadata will be stored. By default, it is ``kaggle_torrent``.

``MYSQL_USER``
    Your MySQL username.

``MYSQL_PWD``
    Your MySQL password.

``METAKAGGLE_PATH``
    The path to the folder containing the uncompressed Meta Kaggle dataset.

``NB_DEST_PATH``
    The path to the folder containing the KGTorrent dataset (the Jupyter notebooks archive). This folder should be empty if you are using the scripts to generate the dataset from scratch. On the other hand, this folder should contain the collection of notebooks from a previous version of the dataset if you want to refresh it, by leveraging the latest version of Meta Kaggle.

``LOG_DEST_PATH``
    The path to the folder where KGTorrent will save its log files.



Usage examples
--------------

**Replicating the collection**

To replicate our collection, you first have to download and uncompress the Meta Kaggle version that we used to build the KGTorrent companion database (retrieved on October 27, 2020). Since it is no more available on Kaggle, we share it as part of the `dataset package <http://neo.di.uniba.it:8080/share.cgi?ssid=0syF2vm>`_: it is the compressed archive named ``MetaKaggle27Oct2020.tar.bz2``.

Once Meta Kaggle is available on your machine and the corresponding environment variable has been set, you can start the creation process by issuing the following command::

    python kgtorrent.py init --strategy HTTP

The ``--strategy`` argument determines whether the notebooks will be downloaded via ``HTTP`` requests or via ``API`` calls. Notebooks downloaded via the official Kaggle API miss the output of code cells, while those downloaded via HTTP requests are complete.

Once you start the creation process, KGTorrent will go through the following steps:

1. *Database initialization*: a new MySQL database is created and set up with the data schema required to store Meta Kaggle data.
2. *Meta Kaggle preprocessing*: Meta Kaggle is an archive containing 29 tables in the ``.csv`` file format. As of today, it cannot be imported in a relational database without incurring in referential integrity violations. This happens because many of the tables miss some rows, as they probably contain private information. Our program overcomes this issue by performing a pre-processing step in which rows with unresolved foreing keys are dropped from each Meta Kaggle table.
3. *Database population*: the MySQL database is populated with information from the filtered Meta Kaggle tables.
4. *Notebooks download*: the Jupyter notebooks are downloaded from Kaggle using the preferred strategy (HTTP or API). An SQL query is used to retrieve the list of notebooks to be downaloded from the MySQL database.


**Refreshing the collection**

To refresh the collection - i.e., to align the dataset to the current state of Kaggle - download the latest version of Meta Kaggle.

Once Meta Kaggle has been downloaded on your machine and the corresponding environment variable has been set, you can start the refresh process by issuing the following command::

    python kgtorrent.py refresh --strategy HTTP

A new MySQL database will be created and populated with the information from the lastest Meta Kaggle. *Warning*: if a database with the same name already exists, it will be overwritten. Then the download procedure will start; this time, the list of notebooks to be downloaded will be checked against the files that are already present in the dataset folder: notebooks that are already locally available will not be downloaded.
Moreover, notebooks from the previous version of KGTorrent that are no more referenced in the refreshed database will be deleted. Indeed, it can happen that notebooks get deleted from the platform and loose their reference in Meta Kaggle.

**Using the collection**

Users interested in analyzing the KGTorrent database should download it from its Zenodo repository; it is stored as a compressed archive named ``KGT_dataset.tar.bz2``. The dataset can be analyzed as a whole, although we believe that the most interesting use case is to leverage the companion database to select a subset of notebooks based on specific research criteria. To this aim, along with the dataset, one should download the compressed archive containing the dump of the MySQL database (named ``KGTorrent_dump_10-2020.sql.tar.bz2``), uncompress it and import it into a local MySQL installation.

You can use the Linux ``tar`` command to uncompress both archives::

    tar -xf KGT_dataset.tar.bz2 -C /path/to/local/dataset/folder
    tar -xf KGTorrent_dump_10-2020.sql.tar.bz2

Then, import the MySQL dump in your local MySQL installation. To perform this step, you can follow `this guide <https://www.digitalocean.com/community/tutorials/how-to-import-and-export-databases-in-mysql-or-mariadb#step-2-mdash-importing-a-mysql-or-mariadb-database>`_.

Once the database has been correctly imported, you can query it to select a subset of notebooks based on specific criteria. The Jupyter notebooks in KGTorrent are saved with filenames following this pattern: ``UserName_CurrentUrlSlug``, where ``UserName`` is a field of the ``Users`` table, while ``CurrentUrlSlug`` is a field of the ``Kernels`` table. Therefore, by including such pattern in the ``SELECT`` statement of an SQL query, the result will comprise a column listing the names of the selected Jupyter notebooks from the dataset.

In the following example, I select the filenames of all Python Jupyter notebooks that have been awarded a gold medal in Kaggle::

    SELECT CONCAT(u.UserName, '_', k.CurrentUrlSlug, '.ipynb') FilteredNotebookNames
    FROM ((kernels k JOIN users u ON k.AuthorUserId = u.Id)
        JOIN kernelversions kv ON k.CurrentKernelVersionId = kv.Id)
        JOIN kernellanguages kl ON kv.ScriptLanguageId = kl.Id
    WHERE kl.name LIKE 'IPython Notebook HTML'
        AND k.Medal = 1;

For further information on how to use KGTorrent, please refer to the `KGTorrent documentation <https://collab-uniba.github.io/KGTorrent/docs_build/html/usage/using.html>`_.

Here (at ``docs/imgs/KGTorrent_logical_schema.png``) as well as in the Zenodo repository containing the dataset, we share the logical schema underlying the KGTorrent database. We built this schema by reverse engineering a relationa model from Meta Kaggle data.

.. image:: docs/imgs/KGTorrent_logical_schema.png
  :width: 1200
  :alt: KGTorrent logical schema

All the most relevant relationships among the db tables are explicitly represented in the schema. However, we decided to omit some of them to ensure a good readability of the image.




Versions
--------

- 1.0.0 - First official release of KGTorrent.



Authors
-------

- Luigi Quaranta
- Giovanni Marcello Aloia



License
-------

This project is licensed under the MIT License - see the ``LICENSE`` file for details.



Contributing
------------

- Fork it (https://github.com/collab-uniba/KGTorrent/fork)
- Create your feature branch (git checkout -b feature/fooBar)
- Commit your changes (git commit -am 'Add some fooBar')
- Push to the branch (git push origin feature/fooBar)
- Create a new Pull Request