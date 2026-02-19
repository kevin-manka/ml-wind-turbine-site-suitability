# Machine Learning-Driven Assessment of Wind Turbine Site Suitability Using High-Resolution Meteorological Data

## Overview

This repository contains all source code and documentation related to the project, which evaluates whether machine learning can improve the identification of suitable locations for wind turbine deployment in Canada with the use of data from 2,741 weather stations in the Canadian Mesonet and the Government of Canada’s wind turbine database. After labelling turbine suitability on weather measurement samples and preprocessing, six machine learning models were trained and tested, with tree-based models achieving the highest performance.

## Dataset

The Canadian Mesonet and Government of Canada Turbine Database datasets can be downloaded [here](https://drive.google.com/file/d/1QeNd0ZOAMqQdXCkgtwPhSDCslPXuHCJ8/view?usp=sharing).

### Instructions

1. Add a `data` folder containing the datasets to the project root directory.
2. Run `uv sync` to install project dependencies.
3. Run `label_score.py` to process the labelled Mesonet data.
4. Run `turbine_prediction_model.ipynb` to execute pre-processing, exploratory data analysis, and the training and evaluation of the models.
