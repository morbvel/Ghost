import java.util.Random

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object ghost {

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Ghost")
      .master("local")
      .getOrCreate()

    val palabra = StringBuilder.newBuilder
    val lista_palabras: DataFrame = readFile("src/main/resources/ghost.csv", spark)

    println("Introduzca la primera letra: ")
    do{
      palabra.append(scala.io.StdIn.readChar())

      palabraEncontrada(lista_palabras,palabra.toString()) match{
        case true => {
          println("-------------------------------------------------------------------------------------------------")
          println("Oh, mira, un listo que se cree guay por ganar con la palabra "+palabra.toString.toUpperCase+" ¬¬ ")
          println("-------------------------------------------------------------------------------------------------")
        }
        case false  => {
          val listCaracteres = maquinaAnadeLetra(lista_palabras,palabra.toString())
          val rand = new Random(System.currentTimeMillis())
          val random_index = rand.nextInt(listCaracteres.length)
          val result = listCaracteres(random_index)

          listCaracteres.contains("|") match{
            case false => {
              palabra.append(result)
              palabraEncontrada(lista_palabras,palabra.toString()) match{
                case true => {
                  println("-------------------------------------------------------------------------------------------------")
                  println("La máquina ha encontrado la palabra "+palabra.toString.toUpperCase+" antes que tú, puto loser.")
                  println("-------------------------------------------------------------------------------------------------")
                }
                case _  => {
                  println("-------------------------------------------------------------------------------------------------")
                  println("La palabra construida de momento es: "+palabra.toString.toUpperCase+". Siguiente letra: ")
                  println("-------------------------------------------------------------------------------------------------")
                }
              }
            }
          }
        }
      }

    }while(!palabraEncontrada(lista_palabras,palabra.toString()))


  }

  private def readFile(path: String, spark: SparkSession) ={
    spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(path)
  }


  private def palabraEncontrada(palabras: DataFrame, palabra: String): Boolean ={
    var bool:Boolean = false
    var aux:Int = 0
    palabras.collect().map(f  =>  {
      if(comprobar(f.mkString.toLowerCase,palabra.toLowerCase))
        aux = 1
      else
        bool = false
    } )

    if (aux == 1)
      bool = true

    bool
  }

  private def comprobar(s1: String, s2: String): Boolean = {
    if(s1.equals(s2)) true else false
  }


  private def maquinaAnadeLetra(palabras: DataFrame, palabra: String):List[Char]  = {
    val chartList = new ListBuffer[Char]()

    palabras.collect().map(x  => {
        if(palabra.length < x.mkString.length && x.mkString.toLowerCase.substring(0, palabra.length).equals(palabra.toLowerCase) ){
          chartList += x.mkString.charAt(palabra.length)
        }

      }
    )

    chartList.toList
  }
}
