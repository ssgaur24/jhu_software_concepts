Grad Cafe Analytics Documentation
==================================

Welcome to the Grad Cafe Analytics system documentation. This is a comprehensive data analysis and web application for processing graduate school application data from The Grad Cafe.

The system consists of three main components:

- **Web Layer**: Flask application serving analysis pages and interactive controls
- **ETL Pipeline**: Data scraping, cleaning, and loading processes  
- **Database Layer**: PostgreSQL storage and query operations

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   overview
   architecture
   api
   testing
   operational

Quick Start
-----------

To get started with the Grad Cafe Analytics system:

1. Set up your environment variables (see :doc:`overview`)
2. Install dependencies: ``pip install -r requirements.txt``
3. Initialize the database: ``python src/load_data.py --init``
4. Run the Flask app: ``python src/flask_app.py``

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`