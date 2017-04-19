package eu.proteus.solma.sax

import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer
import java.util.{HashMap => JHashMap}
import java.util.{Map => JMap}
import java.util.{HashSet => JHashSet}

/**
 * Class that contains all the words in a given class
 *
 * @param id The class identifier.
 * @param words The word frequency.
 * @param tfIdf The weighted TF*IDF.
 */
case class WordBag(
  var id: Option[String] = None,
  words: JMap[String, AtomicLong] = new JHashMap[String, AtomicLong](),
  tfIdf: JMap[String, Double] = new JHashMap[String, Double]()
){

  /**
   * Add a new word to the word bag. If the word exists, the frequency will be increased.
   * @param word The word.
   */
  def addWord(word: String) : Unit = {
    this.words.putIfAbsent(word, new AtomicLong(0))
    this.words.get(word).incrementAndGet()
  }

  /**
   * Compute the cosine similarity between the given class and the vector. The similarity for
   * two vectors a and b, being a the vector defined by the words in this wordbag, and b the
   * vector to be compared is defined as:
   *
   * {{{
   *   similarity(a,b) = (sum a_i*b_i) / sqrt(sum a_i^2) * sqrt(sum b_i^2)
   * }}}
   *
   *
   * @param vector The vector.
   * @return The similarity.
   */
  def similarity(vector: JMap[String, AtomicLong]) : Double = {
    val keys = new JHashSet[String](vector.keySet())
    keys.addAll(this.tfIdf.keySet())
    val instance = this
    var sumTotal = 0.0d
    var aSquared = 0.0d
    var bSquared = 0.0d
    keys.forEach(new Consumer[String] {
      override def accept(word: String): Unit = {
        val a = instance.tfIdf.getOrDefault(word, 0.0d)
        val b = vector.getOrDefault(word, new AtomicLong(0)).get()
        sumTotal = sumTotal + (a*b)
        aSquared = aSquared + (a*a)
        bSquared = bSquared + (b*b)
      }
    })
    val similarity = sumTotal / (Math.sqrt(aSquared) * Math.sqrt(bSquared))

    similarity
  }

}
