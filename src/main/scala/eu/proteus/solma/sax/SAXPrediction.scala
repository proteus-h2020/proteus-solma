package eu.proteus.solma.sax

/**
 * Class to represent the prediction of a given window of the time series. It will
 * be used to return the nearest class.
 *
 * @param classId The class identifier.
 * @param similarity The similarity value.
 */
case class SAXPrediction(classId: String, similarity: Double)
