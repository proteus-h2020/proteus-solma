# proteus-solma


# SAX-SVM

The SAX-VSM algorithm is implemented as two independent algorithms in
SOLMA: `SAX` and `SAXDictionary`. Both algorithms can be used together
following the SAX-VSM implementation, or they can be used independently.

References for this algorithm can be found at:

```
Senin, Pavel, and Sergey Malinchik. "Sax-vsm: Interpretable time series
classification using SAX and vector space model."
2013 IEEE 13th International Conference on Data Mining (ICDM),
IEEE, 2013.
```

## SAX

The SAX algorithm provides a method to transform a `DataStream[Double]`
into a set of words supporting PAA transformation.

To train the SAX and use it to transform a time series use the following
approach:

```
 // Obtain the training and evaluation datasets.
 val trainingDataSet : DataSet[Double] = env.fromCollection(...)
 val evalDataSet : DataStream[Double] = streamingEnv.fromCollection(...)

 // Define the SAX algorithm
 val sax = new SAX().setPAAFragmentSize(2).setWordSize(2)
 // Fit the SAX
 sax.fit(trainingDataSet)
 // Transform the datastream
 val transformed = sax.transform(evalDataSet)
```

The algorithm accepts the following parameters:
* **PAAFragmentSize**: It indicates the number of elements that will be
averaged during the PAA process.
* **WordSize**: It determines the size of the words to be used
* **AlphabetSize**: It determines the size of the alphabet in terms of
number of symbols

## SAXDictionary

```
// Dataset 1 corresponds to class 1
val dataset1 : DataSet[String] = env.fromCollection(...)
val toFit1 = dataset1.map((_, 1))
// Dataset 2 corresponds to class 2
val dataset2 : DataSet[String] = env.fromCollection(trainingData2)
val toFit2 = dataset2.map((_, 2))
// Build the SAX dictionary
val saxDictionary = new SAXDictionary()
// Fit the dictionary
saxDictionary.fit(toFit1)
saxDictionary.fit(toFit2)
// Evaluation datastream
val evalDataSet : DataStream[String] = streamingEnv.fromCollection(...)
// Determine the number of words in the window
val evalDictionary = saxDictionary.setNumberWords(3)
val predictions = evalDictionary.predict[String, SAXPrediction](evalDataSet)
```

The algorithm supports the following parameters:
* **NumberWords**: It determines the number of words that need to
appear in a window to compute the prediction.

## SAX + SAXDictionary

To use both algorithms:

1. Select a training set from the signal
2. Use the training set to fit the `SAX` algorithm
3. Transform the training set to obtain the words and use that to fit
the `SAXDictionary`
4. Select the evaluation stream
5. Connect the evaluation stream to pass first through the `SAX`
algorithm and then through the `SAXDictionary` to obtain the
predictions.