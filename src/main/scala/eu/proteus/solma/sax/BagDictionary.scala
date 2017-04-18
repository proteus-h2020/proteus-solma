package eu.proteus.solma.sax

import java.util.{HashMap => JHashMap}
import java.util.{Map => JMap}

/**
 * Dictionary to store all the words.
 */
case class BagDictionary(bags : JMap[String, WordBag] = new JHashMap[String, WordBag]()){

  /**
   * Maximum number of words in any given class.
   * @return The maximum number of words.
   */
  def maxWords() : Int = {
    var max = 0
    val iterator = bags.entrySet().iterator()
    while(iterator.hasNext){
      val bag = iterator.next()
      if(bag.getValue.words.size() > max){
        max = bag.getValue.words.size()
      }
    }
    max
  }


}
