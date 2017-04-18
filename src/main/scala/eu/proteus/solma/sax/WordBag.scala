package eu.proteus.solma.sax

import java.util.concurrent.atomic.AtomicLong
import java.util.{HashMap => JHashMap}
import java.util.{Map => JMap}

/**
 * Class that contains all the words in a given class
 */
case class WordBag(var id: Option[String] = None, words: JMap[String, AtomicLong] = new JHashMap[String, AtomicLong]()){

  /**
   * Add a new word to the word bag. If the word exists, the frequency will be increased.
   * @param word The word.
   */
  def addWord(word: String) : Unit = {
    this.words.putIfAbsent(word, new AtomicLong(0))
    this.words.get(word).incrementAndGet()
  }

}
