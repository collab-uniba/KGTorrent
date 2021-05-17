KGTorrent
=========

Given their growing popularity among data scientists, computational notebooks - and in particular `Jupyter notebooks <https://jupyter.org>`_ - are being increasingly studied by researchers worldwide. Generally, the aim is to understand how they are typically used, identify possible flaws, and inform the design of extensions and updates of the tool. To ease these kind of research endeavors, we collected and shared a large dataset of 248,761 Jupyter notebooks from Kaggle, named `KGTorrent <https://doi.org/10.5281/zenodo.4468522>`_.

`Kaggle <https://www.kaggle.com>`_ is a web platform hosting machine learning competitions that enables the creation and execution of Jupyter notebooks in a containerized computational environment. By leveraging `Meta Kaggle <https://www.kaggle.com/kaggle/meta-kaggle>`_, a dataset that is publicly available on the platform, we also built a companion MySQL database containing metadata on the notebooks in our dataset.

This repository hosts the Python scripts we developed to create KGTorrent. By leveraging the latest version of Meta Kaggle, the same scripts can also be used to refresh the collection.


.. toctree::
   :maxdepth: 3
   :caption: Contents:

   configuration
   usage/replicating
   usage/refreshing
   usage/using
   modules

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
