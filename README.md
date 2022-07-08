# SENG8080 - Case Studies

## Airline sentiment analysis

This repository contains the development for Conestoga's SENG8080 - Case Studies. The Airline industry was chosen, with a scope around social media, more specifically about classifying airlines tweets for sentiment analysis.

## Initial analysis

An initial analysis is done with the help of the following resources.

* [Sentiment Analysis on US Airline dataset (1 of 2)](https://towardsdatascience.com/sentiment-analysis-on-us-twitter-airline-dataset-1-of-2)

## Sentiment Analysis on fresh Tweets

As part of the Second project update, there are two major inclusions to the progress of the project:

1. Hugging Face algorithm adoption: Following our professor's advice, we looked into the Hugging Face transformers library and were able to select an optimal algorithm to help us better cover that requirement. The [`cardiffnlp/twitter-xlm-roberta-base-sentiment`](https://huggingface.co/cardiffnlp/twitter-xlm-roberta-base-sentiment) model was chosen due to its ability to classify text in various languages.

2. Integration with the Twitter API was added. Fresher tweets can now be used, and retrieved as we need to continue testing and eventually implement the live sentiment analysis classification.

The above inclusions can be found in the [hugging-face](./hugging-face.ipynb) notebook.
