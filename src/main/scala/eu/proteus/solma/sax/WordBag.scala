/*
 * Copyright (C) 2017 The Proteus Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.proteus.solma.sax

import java.io.File
import java.io.PrintWriter
import java.util.function.BiConsumer
import java.util.function.Consumer
import java.util.{HashMap => JHashMap}
import java.util.{Map => JMap}
import java.util.{HashSet => JHashSet}

import scala.io.Source

object WordBag {

  /**
   * Load a word bag from the model stored in the file. The file must contain two columns with the
   * word and the TF-IDF value.
   * @param wordBagPath The path of the word bag class model.
   */
  def fromFile(wordBagPath : String) : WordBag = {
    val inputFile = new File(wordBagPath)
    val wordBagId = inputFile.getName.replaceFirst("[.][^.]+$", "")

    val tfIdf: JMap[String, Double] = new JHashMap[String, Double]()

    val lines = Source.fromFile(inputFile).getLines()
    while(lines.hasNext){
      val toAdd = lines.next().split(";")
      tfIdf.put(toAdd(0), toAdd(1).toDouble)
    }

    new WordBag(id = Some(wordBagId), tfIdf = tfIdf)
  }

}

/**
 * Class that contains all the words in a given class
 *
 * @param id The class identifier.
 * @param words The word frequency.
 * @param tfIdf The weighted TF*IDF.
 */
case class WordBag(
  var id: Option[String] = None,
  words: JMap[String, Long] = new JHashMap[String, Long](),
  tfIdf: JMap[String, Double] = new JHashMap[String, Double]()
){

  /**
   * Add a new word to the word bag. If the word exists, the frequency will be increased.
   * @param word The word.
   */
  def addWord(word: String) : Unit = {
    this.words.putIfAbsent(word, 0)
    val previous = this.words.get(word)
    this.words.put(word, previous + 1)
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
  def similarity(vector: JMap[String, Long]) : Double = {
    val keys = new JHashSet[String](vector.keySet())
    keys.addAll(this.tfIdf.keySet())
    val instance = this
    var sumTotal = 0.0d
    var aSquared = 0.0d
    var bSquared = 0.0d
    keys.forEach(new Consumer[String] {
      override def accept(word: String): Unit = {
        val a = instance.tfIdf.getOrDefault(word, 0.0d)
        val b = vector.getOrDefault(word, 0)
        sumTotal = sumTotal + (a*b)
        aSquared = aSquared + (a*a)
        bSquared = bSquared + (b*b)
      }
    })
    val similarity = sumTotal / (Math.sqrt(aSquared) * Math.sqrt(bSquared))

    similarity
  }

  /**
   * Store the word bag. The bag is stored in a CSV on a file with the name of the class.
   * @param basePath The base path for storing the file.
   */
  def storeWordBag(basePath : String) : Unit = {
    if(this.id.isEmpty){
      throw new UnsupportedOperationException("Cannot store wordbag without identifier")
    }

    if(this.tfIdf.size() == 0){
      throw new UnsupportedOperationException("Cannot store wordbag without words")
    }

    val outputPath = basePath + File.separator + this.id.get + ".csv"
    val output = new PrintWriter(new File(outputPath))

    this.tfIdf.forEach(new BiConsumer[String, Double] {
      override def accept(word: String, tfIdf: Double): Unit = {
        output.println(s"${word};${tfIdf}")
      }
    })

    output.close()
  }

}
