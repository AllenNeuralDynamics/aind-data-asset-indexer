Contributor Guidelines
======================

This document will go through best practices for contributing to this
project. We welcome and appreciate contributions or ideas for
improvement.

-  `Bug Reports and Feature
   Requests <#bug-reports-and-feature-requests>`__
-  `Local Installation for
   Development <#local-installation-for-development>`__
-  `Branches and Pull Requests <#branches-and-pull-requests>`__
-  `Release Cycles <#release-cycles>`__

Bug Reports and Feature Requests
--------------------------------

Before creating a pull request, we ask contributors to please open a bug
report or feature request first:
`issues <https://github.com/AllenNeuralDynamics/aind-data-asset-indexer/issues/new/choose>`__

We will do our best to monitor and maintain the backlog of issues.

Local Installation and Development
----------------------------------

For development,

-  For new features or non-urgent bug fixes, create a branch off of
   ``dev``
-  For an urgent hotfix to our production environment, create a branch
   off of ``main``

Consult the `Branches and Pull Requests <#branches-and-pull-requests>`__
and `Release Cycles <#release-cycles>`__ for more details.

From the root directory, run:

.. code:: bash

   pip install -e .[dev]

to install the relevant code for development.

.. _running-indexer-jobs-locally:

Running indexer jobs locally
~~~~~~~~~~~~~~~~~~~~~~~~~~~~


The jobs are intended to be run as scheduled AWS ECS tasks in the same VPC
as the DocDB instance. The job settings are stored in AWS Parameter Store.

If you wish to run the jobs locally, follow these steps:

1. In a new terminal, start ssh session. Credentials can be found in AWS
   Secrets Manager.

.. code:: bash

   ssh -L 27017:{docdb_host}:27017 {ssh_username}@{ssh_host} -N -v

2. For the `IndexAindBucketsJob`, you will need to set the ``INDEXER_PARAM_NAME``.
   Then, run the following:

.. code:: python

   from aind_data_asset_indexer.index_aind_buckets import IndexAindBucketsJob
   from aind_data_asset_indexer.models import AindIndexBucketsJobSettings

   if __name__ == "__main__":
      main_job_settings = AindIndexBucketsJobSettings.from_param_store(param_store_name=INDEXER_PARAM_NAME)
      main_job_settings.doc_db_host = "localhost"
      main_job = IndexAindBucketsJob(job_settings=main_job_settings)
      main_job.run_job()

3. For the `CodeOceanIndexBucketJob`, you will need to set the ``CO_INDEXER_PARAM_NAME``
   and ``DEVELOPER_CODEOCEAN_ENDPOINT``. Then, run the following:

.. code:: python

   from aind_data_asset_indexer.models import CodeOceanIndexBucketJobSettings
   from aind_data_asset_indexer.codeocean_bucket_indexer import CodeOceanIndexBucketJob

   if __name__ == "__main__":
      main_job_settings = CodeOceanIndexBucketJobSettings.from_param_store(param_store_name=CO_INDEXER_PARAM_NAME)
      main_job_settings.doc_db_host = "localhost"
      main_job_settings.temp_codeocean_endpoint=DEVELOPER_CODEOCEAN_ENDPOINT
      main_job = CodeOceanIndexBucketJob(job_settings=main_job_settings)
      main_job.run_job()

4. Close the ssh session when you are done.


Branches and Pull Requests
--------------------------

Branch naming conventions
~~~~~~~~~~~~~~~~~~~~~~~~~

Name your branch using the following format:
``<type>-<issue_number>-<short_summary>``

where:

-  ``<type>`` is one of:

   -  **build**: Changes that affect the build system
      or external dependencies (e.g., pyproject.toml, setup.py)
   -  **ci**: Changes to our CI configuration files and scripts
      (examples: .github/workflows/ci.yml)
   -  **docs**: Changes to our documentation
   -  **feat**: A new feature
   -  **fix**: A bug fix
   -  **perf**: A code change that improves performance
   -  **refactor**: A code change that neither fixes a bug nor adds
      a feature, but will make the codebase easier to maintain
   -  **test**: Adding missing tests or correcting existing tests
   -  **hotfix**: An urgent bug fix to our production code
-  ``<issue_number>`` references the GitHub issue this branch will close
-  ``<short_summary>`` is a brief description that shouldn’t be more than 3
   words.

Some examples:

-  ``feat-12-adds-email-field``
-  ``fix-27-corrects-endpoint``
-  ``test-43-updates-server-test``

We ask that a separate issue and branch are created if code is added
outside the scope of the reference issue.

Commit messages
~~~~~~~~~~~~~~~

Please format your commit messages as ``<type>: <short summary>`` where
``<type>`` is from the list above and the short summary is one or two
sentences.

Testing and docstrings
~~~~~~~~~~~~~~~~~~~~~~

We strive for complete code coverage and docstrings, and we also run
code format checks.

-  To run the code format check:

.. code:: bash

   flake8 .

-  There are some helpful libraries that will automatically format the
   code and import statements:

.. code:: bash

   black .

and

.. code:: bash

   isort .

Strings that exceed the maximum line length may still need to be
formatted manually.

-  To run the docstring coverage check and report:

.. code:: bash

   interrogate -v .

This project uses NumPy’s docstring format: `Numpy docstring
standards <https://numpydoc.readthedocs.io/en/latest/format.html>`__

Many IDEs can be configured to automatically format docstrings in the
NumPy convention.

-  To run the unit test coverage check and report:

.. code:: bash

   coverage run -m unittest discover && coverage report

-  To view a more detailed html version of the report, run:

.. code:: bash

   coverage run -m unittest discover && coverage report
   coverage html

and then open ``htmlcov/index.html`` in a browser.

Pull requests
~~~~~~~~~~~~~

Pull requests and reviews are required before merging code into this
project. You may open a ``Draft`` pull request and ask for a preliminary
review on code that is currently a work-in-progress.

Before requesting a review on a finalized pull request, please verify
that the automated checks have passed first.

Release Cycles
--------------------------

For this project, we have adopted the `Git
Flow <https://www.gitkraken.com/learn/git/git-flow>`__ system. We will
strive to release new features and bug fixes on a two week cycle. The
rough workflow is:

Hotfixes
~~~~~~~~

-  A ``hotfix`` branch is created off of ``main``
-  A Pull Request into is ``main`` is opened, reviewed, and merged into
   ``main``
-  A new ``tag`` with a patch bump is created, and a new ``release`` is
   deployed
-  The ``main`` branch is merged into all other branches

Feature branches and bug fixes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  A new branch is created off of ``dev``
-  A Pull Request into ``dev`` is opened, reviewed, and merged

Release branch
~~~~~~~~~~~~~~

-  A new branch ``release-v{new_tag}`` is created
-  Documentation updates and bug fixes are created off of the
   ``release-v{new_tag}`` branch.
-  Commits added to the ``release-v{new_tag}`` are also merged into
   ``dev``
-  Once ready for release, a Pull Request from ``release-v{new_tag}``
   into ``main`` is opened for final review
-  A new tag will automatically be generated
-  Once merged, a new GitHub Release is created manually

Pre-release checklist
~~~~~~~~~~~~~~~~~~~~~

-  ☐ Increment ``__version__`` in
   ``aind_data_asset-indexer/__init__.py`` file
-  ☐ Run linters, unit tests, and integration tests
-  ☐ Verify code is deployed and tested in test environment
-  ☐ Update examples
-  ☐ Update documentation

   -  Run:

   .. code:: bash

      sphinx-apidoc -o docs/source/ src
      sphinx-build -b html docs/source/ docs/build/html

-  ☐ Update and build UML diagrams

   -  To build UML diagrams locally using a docker container:

   .. code:: bash

      docker pull plantuml/plantuml-server
      docker run -d -p 8080:8080 plantuml/plantuml-server:jetty

Post-release checklist
~~~~~~~~~~~~~~~~~~~~~~

-  ☐ Merge ``main`` into ``dev`` and feature branches
-  ☐ Edit release notes if needed
-  ☐ Post announcement
