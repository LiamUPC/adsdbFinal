# ADSDB Data Management Backbone

This repository contains our data management and analysis backbone to our ADSDB project.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the libraries necessary for running the backbone.

```bash
pip install -r requirements.txt
```

## Usage

By default, the repository comes with all the available files in the data/temporal directory. They can be found in a zip file. To be able to run the project, you will have to extract in the data/temporal directory. Once they have been extracted, there are two options:

- Running main.py with all the files in dataManagementBackbone/data/landing/temporal
- Keeping some files back and running main.py. Then placing the files you have saved back into the dataManagementBackbone/data/landing/temporal directory and running main.py again. That way you can simulate the process of the data management backbone receiving new data, processing and adding it to the exploitation zone database.


```bash
python main.py
```