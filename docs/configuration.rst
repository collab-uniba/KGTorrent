.. _configuration:

Configuration
=============

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