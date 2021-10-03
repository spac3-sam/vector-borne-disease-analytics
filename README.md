# vector-borne-disease-analytics

CSV formatted final datasets in the `data` folder.

Jupyter notebooks in the `notebooks` folder, mainly used for testing and analysis.

## Usage
If you only want to scrape the data from ProMED-mail, you can run the `scrape_promed.py` script. 
To use this, install the following packages: `requests`, `beautifulsoup4`, and `pandas`.
Then run the following command: `python scrape_promed.py search_term`, and replace search term with what you want to search (i.e. to scrape malaria data, run `python scrape_promed.py search_term`). 
This will extract all articles with that search term and place them in `promed_(search_term).csv`.

To run my data extraction scripts, you need to install all the packages in the `extract_data.py` file. I created an conda `environment.yml` file. If you have conda installed, just run `conda env create -f environment.yml` in the root of this repository. *Note, you will need nvidia cuda installed.* 

However, `extract_data.py` will require the full combined dataset that I created in one of the jupyter notebooks in the notebook folder. It might be easier to use if you just export the functions and use them in your own code/data extraction pipeline.

Feel free to open up an issue if you are struggling with usage.