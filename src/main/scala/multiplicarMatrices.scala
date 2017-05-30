package main

import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType}
import org.apache.spark.sql.Row


object multiplicarMatrices{


  def main(args: Array[String]){

    //args(0): direccion de la primera matriz
    //args(1): direccion de la segunda matriz
    //args(2): direccion donde se guardara la matriz resultante en formato df

    val spark = SparkSession.builder()
    .appName("Multiplicacion de Matrices")
    .enableHiveSupport()
    .getOrCreate()

    val sc = spark.sparkContext

    var num = 0.toLong  //variable auxiliar donde se guardara el numero de filas o numero de columnas dependiendo de la matriz

    val abrir = udf { (s: String) => s.split(" ")} //con esta funcion paralela se realizara la apertura del strig que contiene los datos, puesto que estos en cada fila viene separados por espacios
    val multiplicar = udf { (a: String,b: String) => a.toDouble*b.toDouble } //con esta funcion paralela se multiplicaran los valores de la matriz

//-------------------------paso 1: lectura de las dos matrices en formato txt----------------------------------------------------

    val dfA = spark.read.format("text").load("/user/bigdata1-1701/matrices/"+args(0))//lectura del archivo que contiene la primera matriz a multiplicar Mij
    val dfB = spark.read.format("text").load("/user/bigdata1-1701/matrices/"+args(1))//lectura del archivo que contiene la segunda matriz a multiplicar Njk

//------------------------paso 2: transformacion de la matriz----------------------------------------------------


//                -------------------step 1------------------    ----------------step 2-------------------------------------------     --------------step 3--------------------------------
    val filA = dfA.withColumn("i", monotonicallyIncreasingId).withColumn("Array", abrir(dfA("value"))).drop("value").toDF("i","Array").withColumn("Mij", explode(col("Array"))).drop("Array")
    // a la primera matriz se le agrega una columna con un id autoincremental que representa el indice de la fila y a su vez tambien se separa el string de los datos de cada fila
    //dejando todo eso en formato indice de fila y un array que representa todos sus datos, ahora lo que haremos es aplicar la funcion explode, que se encarga de "abrir" el Array
    //y dejando todo el dataframe en formato indice de fila y datos de la matriz, ahora solo nos queda faltando agregar el indice de columna.

//-----------------------------------------------PASO 3:Cantidad de columnas-------------------------------------------------------

    num = filA.select("i").where("i=0").count()//variable auxiliar con la cual sabremos cuandos datos hay por fila, en este caso de la primera fila

    val matrizA = filA.withColumn("j", (monotonicallyIncreasingId % num))
    //ahora agregaremos el indice j, osea el indice de las columnas, para esto usaremos la varible auxiliar mun, y ahora lo q haremos sera, poder un indice autoincremental
    //a cada registro y obtener el resultado de la operación modulo con la cantidad de datos por fila, asi el incrementara de 0 hasta el numero de datos de la fila, y asi
    //sucecibamente por cada fila.

//-----------------------------Guardado de la matriz en memoria------------------------
    val matrizAP = matrizA.persist(MEMORY_ONLY_SER)//se guarda en memoria el dataframe resultado de dicho procesamiento

//--------------------------------repetir lo del paso 2 y 3 para la matriz B------------------------------------------------
    //se repite el mismo proceso para la segunda matriz Njk
    val filB = dfB.withColumn("j", monotonicallyIncreasingId).withColumn("Array", abrir(dfB("value"))).drop("value").withColumn("Njk", explode(col("Array"))).drop("Array")

    num = filB.select("j").where("j=0").count()

    val matrizB = filB.withColumn("k", (monotonicallyIncreasingId % num))

    val matrizBP = matrizB.persist(MEMORY_ONLY_SER)//fin de procesamiento segunda matriz


//---------------------------------paso 4--------------------------------------------------
    val matrizUnida = matrizAP.join(matrizBP, matrizAP("j") === matrizBP("j")).drop("j")//con esto uniremos las dos matrices por su indice j, osea uniremos todos los datos de las filas con su respeciva columna

//------------------------paso 5------------------------------------------------

//                                 ------------------------------step 1----------------------------------------                      -----------step 2-----------------
    val matrizFinal = matrizUnida.withColumn("Resultado", multiplicar(matrizUnida("Njk"),matrizUnida("Mij"))).drop("Mij").drop("Njk").groupBy("i","k").sum("Resultado").toDF("i","k","Resultado")
    //ahora propiamente realizaremos la multiplicacion, lo primero que realizaremos sera cojer cada uno de los valores obtenidos y multiplicarlos osea,
    //la primera posicion de la primera matriz por la primera posicion de la primera fila de la segunda matriz, asi sucecibamente, y por ultimos agruparemos por los indices i y k
    //y sumaremos dicho resultado, osea sumar la multiplicacion de todos los valores de cada columna de la primera matriz con los valores de cada fila de la segunda amtriz

    matrizFinal.write.mode(SaveMode.Overwrite).format("parquet").save(args(2))//por ultimo dicho resultado lo guardaremos en la direccion deseada en fomato parquet


  }
}
