package esgi.exo

import io.univalence.sparktest.SparkTest
import org.scalatest.FlatSpec
import esgi.exo.FootballApp._
import org.apache.spark.sql.types.IntegerType

class FootballAppTest extends FlatSpec with SparkTest {

  "penaltyNAto0orToInt" should "return a Dataframe with columns penalty_france and penalty_france with int values and 0 in place of NA" in {
    //Given
    val df = dataframe(
      "{penalty_france: \"NA\", penalty_adversaire: \"5\"}",
      "{penalty_france: \"NA\", penalty_adversaire: \"NA\"}",
      "{penalty_france: \"2\", penalty_adversaire: \"NA\"}"
    )

    //When
    val result = penaltyNAto0orToInt(df)

    //Then
    var expected = dataframe(
      "{penalty_france: 0, penalty_adversaire: 5}",
      "{penalty_france: 0, penalty_adversaire: 0}",
      "{penalty_france: 2, penalty_adversaire: 0}"
    )
    expected = expected.select(expected("penalty_france").cast(IntegerType), expected("penalty_adversaire").cast(IntegerType))



    result.assertEquals(expected)
  }

  "selectUsefulColumn" should "return a Dataframe with only columns match, competition, adversaire, score_france, score_adversaire, penalty_france, penalty_adversaire, date" in {
    //Given
    val df = dataframe(
      "{\"X2\":\"1er mai 1904\",\"match\":\"Belgique - France\",\"X5\":\"3-3\",\"competition\":\"Match amical\",\"adversaire\":\"Belgique\",\"score_france\":3,\"score_adversaire\":3,\"penalty_france\":0,\"penalty_adversaire\":0,\"date\":\"1904-05-01\",\"year\":\"1904\",\"outcome\":\"draw\",\"no\":1}",
      "{\"X2\":\"12 février 1905\",\"match\":\"France - Suisse\",\"X5\":\"1-0\",\"competition\":\"Match amical\",\"adversaire\":\"Suisse\",\"score_france\":1,\"score_adversaire\":0,\"penalty_france\":0,\"penalty_adversaire\":0,\"date\":\"1905-02-12\",\"year\":\"1905\",\"outcome\":\"win\",\"no\":1}",
      "{\"X2\":\"7 mai 1905\",\"match\":\"Belgique - France\",\"X5\":\"7-0\",\"competition\":\"Match amical\",\"adversaire\":\"Belgique\",\"score_france\":0,\"score_adversaire\":7,\"penalty_france\":0,\"penalty_adversaire\":0,\"date\":\"1905-05-07\",\"year\":\"1905\",\"outcome\":\"loss\",\"no\":2}"
    )

    //When
    val result = selectUsefulColumn(df)

    //Then
    val expected = dataframe(
      "{\"match\":\"Belgique - France\",\"competition\":\"Match amical\",\"adversaire\":\"Belgique\",\"score_france\":3,\"score_adversaire\":3,\"penalty_france\":0,\"penalty_adversaire\":0,\"date\":\"1904-05-01\"}",
      "{\"match\":\"France - Suisse\",\"competition\":\"Match amical\",\"adversaire\":\"Suisse\",\"score_france\":1,\"score_adversaire\":0,\"penalty_france\":0,\"penalty_adversaire\":0,\"date\":\"1905-02-12\"}",
      "{\"match\":\"Belgique - France\",\"competition\":\"Match amical\",\"adversaire\":\"Belgique\",\"score_france\":0,\"score_adversaire\":7,\"penalty_france\":0,\"penalty_adversaire\":0,\"date\":\"1905-05-07\"}"
    )

    result.assertEquals(expected)
  }

  "filterDate" should "return a Dataframe with columns date filter with only date greater than the date enter" in {
    //Given
    val df = dataframe(
      "{date: \"2018-06-04\"}",
      "{date: \"2018-10-04\"}",
      "{date: \"2017-10-09\"}",
      "{date: \"2017-02-23\"}"
    )

    //When
    val result = filterDate(df, "2018-01")

    //Then
    val expected =  dataframe(
      "{date: \"2018-06-04\"}",
      "{date: \"2018-10-04\"}"
    )

    result.assertEquals(expected)
  }

  "addColumnHome" should "return a Dataframe with new column 'Domicile' based on given column 'match' and Domicile should be true when 'match' start by France" in {
    //Given
    val df = dataframe(
      "{match: \"France - Italie\"}",
      "{match: \"Angleterre - Italie\"}",
      "{match: \"Angleterre - France\"}",
      "{match: \"France - Angleterre\"}"
    )

    //When
    val result = addColumnHome(df)

    //Then
    val expected =  dataframe(
      "{match: \"France - Italie\", Domicile: true}",
      "{match: \"Angleterre - Italie\", Domicile: false}",
      "{match: \"Angleterre - France\", Domicile: false}",
      "{match: \"France - Angleterre\", Domicile: true}"
    )

    result.assertEquals(expected)
  }

  "getJoinData" should "return a join Dataframe of the two Dataframe in parameters, on column 'adversaire' and 'stat_adversaire'" in {
    //Given
    val df1 = dataframe(
      "{adversaire: \"Italie\", date: \"2017-03-06\"}",
      "{adversaire: \"Pays-Bas\", date: \"2019-03-26\"}",
      "{adversaire: \"URSS\", date: \"2012-12-12\"}",
      "{adversaire: \"Allemagne\", date: \"2006-03-26\"}"
    )

    val df2 = dataframe(
      "{stat_adversaire: \"Italie\", competition: \"Match amical\"}",
      "{stat_adversaire: \"Pays-Bas\", competition: \"Coupe du monde\"}",
      "{stat_adversaire: \"URSS\", competition: \"Match amical\"}",
      "{stat_adversaire: \"Allemagne\", competition: \"Coupe du monde\"}"
    )

     //Then
    val result = getJoinData(df1, df2)

     //Expected
    val expected = dataframe(
      "{adversaire: \"Italie\", date: \"2017-03-06\", stat_adversaire: \"Italie\", competition: \"Match amical\"}",
      "{adversaire: \"Pays-Bas\", date: \"2019-03-26\", stat_adversaire: \"Pays-Bas\", competition: \"Coupe du monde\"}",
      "{adversaire: \"URSS\", date: \"2012-12-12\", stat_adversaire: \"URSS\", competition: \"Match amical\"}",
      "{adversaire: \"Allemagne\", date: \"2006-03-26\", stat_adversaire: \"Allemagne\", competition: \"Coupe du monde\"}"
    )
  }
}
