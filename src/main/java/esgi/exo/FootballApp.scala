package esgi.exo


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window

object FootballApp {




  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("FootballApp")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    val inputFile = args(0)

    val csvSchema = StructType(Array(
      StructField("X2", StringType, true),
      StructField("X4", StringType, true),
      StructField("X5", StringType, true),
      StructField("X6", StringType, true),
      StructField("adversaire", StringType, true),
      StructField("score_france", IntegerType, true),
      StructField("score_adversaire", IntegerType, true),
      StructField("penalty_france", StringType, true),
      StructField("penalty_adversaire", StringType, true),
      StructField("date", DateType, true),
      StructField("year", StringType, true),
      StructField("outcome", StringType, true),
      StructField("no", IntegerType, true)
    ))


    var df = spark
      .read
      .option("header", "true")
      .option("sep", ",")
      .schema(csvSchema)
      .csv(inputFile)
      .withColumnRenamed("X4", "match")
      .withColumnRenamed("X6", "competition")


    df = cleanData(df)
    df = addColumnHome(df)

    val dfStatistiques = calculStats(df);

    writeDFInParquet(dfStatistiques, "./src/data/stats.parquet")




    dfStatistiques.show(100)

  }
  //UDF permettant de transormer une string en Int ou 0 si la valeur egale NA
  val penaltyStringToInt: String => Int = (penaltyValue) => {
    if(penaltyValue == "NA") {
      0
    } else {
      penaltyValue.toInt
    }
  }

  //Utilise l'udf penaltyStringToInt pour transformer les penalty
  def penaltyNAto0orToInt(df: DataFrame): DataFrame = {
    val UDFPenaltyStringToInt = udf(penaltyStringToInt)
    df
      .withColumn("penalty_france", UDFPenaltyStringToInt(df("penalty_france")))
      .withColumn("penalty_adversaire", UDFPenaltyStringToInt(df("penalty_adversaire")))
  }

  //Selectionne les colonnes utiles
  def selectUsefulColumn(df: DataFrame): DataFrame = {
    df.select(
      df("match"),
      df("competition"),
      df("adversaire"),
      df("score_france"),
      df("score_adversaire"),
      df("penalty_france"),
      df("penalty_adversaire"),
      df("date"))
  }
  //Filtre les dates en ne gardant que celles supérieures au param date
  def filterDate(df: DataFrame, date:String ): DataFrame = {
    df.filter(df("date").gt(lit(date)))
  }

  //Utilise les différentes fonctions pour nettoyer les data et renvoieune df propre
  def cleanData(df: DataFrame): DataFrame = {

    val dfWithIntPenalty = penaltyNAto0orToInt(df)

    val dfWithGoodColumns = selectUsefulColumn(dfWithIntPenalty)

    val dfWithDateFiltered = filterDate(dfWithGoodColumns, "1980-03")

    dfWithDateFiltered
  }

  val startWithFrance: String => Boolean = matchTeams => {
    if(matchTeams.take(6) == "France"){
      true
    } else {
      false
    }
  }

  def addColumnHome(df: DataFrame): DataFrame = {
    val extractStartWithFrance = udf(startWithFrance)

    df.withColumn("Domicile", extractStartWithFrance(df("match")))
  }

  def calculStats(df: DataFrame): DataFrame = {
    df.groupBy(df("adversaire"))
      .agg(
        avg(df("score_france")).alias("moyenne_score_france"),
        avg(df("score_adversaire")).alias("moyenne_score_adversaire"),
        count(df("adversaire")).alias("nombre_rencontre"),
        (sum(df("Domicile").cast(IntegerType))/count(df("Domicile"))*100).alias("pourcentage_jouer_domicile_france"),
        count(df("competition").startsWith("Coupe du monde")).alias("nombre_rencontre_coupe_du_monde"),
        (sum("penalty_france") - sum("penalty_adversaire")).alias("ratio_penalty")
      )
  }

  def writeDFInParquet(df: DataFrame, filePath: String): Unit = {
    df.write.parquet(filePath)
  }


}
