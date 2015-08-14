package TheadSpark

/**
 * Created by phong on 8/6/2015.
 */

import java.net.URI

import com.mongodb.{Mongo, ServerAddress}
import com.redis._
import net.liftweb.common.Full
import net.liftweb.mongodb._
import net.liftweb.mongodb.record.field.{BsonRecordListField, StringPk, StringRefField}
import net.liftweb.mongodb.record.{BsonMetaRecord, BsonRecord, MongoMetaRecord, MongoRecord}
import net.liftweb.record.field.{DoubleField, IntField, StringField}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by phong on 8/5/2015.
 */
object ThreadSpark {

  object MongoUrl {

    def defineDb(id: MongoIdentifier, url: String) {

      val uri = new URI(url)

      val db = uri.getPath drop 1
      val server = new Mongo(new ServerAddress(uri.getHost, uri.getPort))

      Option(uri.getUserInfo).map(_.split(":")) match {
        case Some(Array(user, pass)) => MongoDB.defineDbAuth(id, server, db, user, pass)
        case _ => MongoDB.defineDb(id, server, db)
      }
    }

  }

  def main(args: Array[String]) {

    MongoUrl.defineDb(DefaultMongoIdentifier, "mongodb://10.15.171.35:27017/ScoringCardDB")
    val thread = new Thread {
      override def run {

        val conf = new SparkConf().setMaster("local[*]").setAppName("CamusApp")
        val sc = new SparkContext(conf)
        val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
        val df = hiveContext.read.json("hdfs://10.15.171.41:54310/home/phonghh2/project/demo/camusDisk/topics/Scoring/hourly/*/*/*/*")
        df.registerTempTable("HDFS")

        val DBListModel = ModelInfo.findAll
        for(x<-DBListModel){
          RangeScoring(x.id.toString(), hiveContext, x.name.toString())
          PercentOptionOfFactor(x.id.toString(), hiveContext,x.name.toString())
        }

        val DBListFT = Factor.findAll

        var listDBCuoi: List[Factor] = List()

        for (factor <- DBListFT) {
          if (factor.FactorOption.value.size != 0)
            listDBCuoi = listDBCuoi ::: List(factor)
        }

        for(x<-listDBCuoi.distinct){
          for(y<-x.FactorOption.value.distinct){
            if(y.FactorOptionId.toString().equals("") == false)
              TopBotOption(y.FactorOptionId.toString(), hiveContext)
          }
        }

        sc.stop()
      }
    }
    thread.start
    Thread.sleep(7200*1000) // slow the loop down a bit
  }

  def PercentOptionOfFactor(ModelId : String, hiveContext:HiveContext, ModelName:String) = {
    val query = hiveContext.sql("SELECT part.factor_name, part.factor_option_name, COUNT(scoring) application_count " +
                                "FROM HDFS LATERAL VIEW explode(resultin) resultinable AS part " +
                                "WHERE rate.modelid = '" + ModelId + "' " +
                                "GROUP BY part.factor_name, part.factor_option_name " +
                                "ORDER BY factor_name")
    val a = query.toJSON.collect()
    val r = new RedisClient("10.15.171.41", 6379)
    r.del("Spark-PercentOptionOfFactor-" + ModelId)
    r.rpush("Spark-PercentOptionOfFactor-" + ModelId, "{\"modelName\":\"" + ModelName + "\"}")
    for (x <- a ){
      r.rpush("Spark-PercentOptionOfFactor-" + ModelId, x)
    }

    println("PercentOptionOfFactor " + ModelId + " DONE")
  }

  def RangeScoring(ModelId : String, hiveContext:HiveContext, ModelName:String) = {
    val query = hiveContext.sql("SELECT rating_code, rating_status, COUNT(scoring) application_count, SUM(scoring)/COUNT(scoring) TB FROM HDFS WHERE rate.modelid = '" + ModelId + "' GROUP BY rating_code, rating_status ORDER BY TB")
    val a = query.toJSON.collect()
    val r = new RedisClient("10.15.171.41", 6379)
    r.del("Spark-RangeScoring-" + ModelId)
    r.rpush("Spark-RangeScoring-" + ModelId, "{\"modelName\":\"" + ModelName + "\"}")
    for (x <- a ){
      r.rpush("Spark-RangeScoring-" + ModelId, x)
    }
    println("RangeScoring " + ModelId + " DONE")
  }

  def TopBotOption(factorOptionId : String, hiveContext:HiveContext) = {
    val queryTop = hiveContext.sql("SELECT scoring, rating_code, customer_name, part.factor_option_name FROM HDFS LATERAL VIEW explode(resultin) resultinable AS part WHERE part.factor_option_id = '" + factorOptionId + "' ORDER BY scoring LIMIT 5").toJSON.collect()
    val queryBot = hiveContext.sql("SELECT scoring, rating_code, customer_name, part.factor_option_name FROM HDFS LATERAL VIEW explode(resultin) resultinable AS part WHERE part.factor_option_id = '" + factorOptionId + "' ORDER BY scoring DESC LIMIT 5").toJSON.collect()

    val r = new RedisClient("10.15.171.41", 6379)
    r.del("Spark-TopBotOption-Top-" + factorOptionId)
    for (x <- queryTop) {
      r.rpush("Spark-TopBotOption-Top-" + factorOptionId, x)
    }
    r.del("Spark-TopBotOption-Bot-" + factorOptionId)
    for (x <- queryBot) {
      r.lpush("Spark-TopBotOption-Bot-" + factorOptionId, x)
    }
    println("TopBotOption " + factorOptionId + " DONE")
  }

  class ModelInfo private () extends MongoRecord[ModelInfo] with StringPk[ModelInfo] {

    override def meta = ModelInfo

    // An embedded document:
    //  object modelinfo extends BsonRecordField(this, modelinfoIN)
    object name extends StringField(this, 1024)
    object description extends StringField(this, 1024)
    object status extends StringField(this, 1024)
    object min extends DoubleField(this)
    object max extends DoubleField(this)

  }

  object ModelInfo extends ModelInfo with MongoMetaRecord[ModelInfo] {
    override def collectionName = "modelinfo"
  }

  class Factor private () extends MongoRecord[Factor] with StringPk[Factor] {

    override def meta = Factor

    object ModelId extends StringRefField(this, ModelInfo, 512){
      override def options = ModelInfo.findAll.map(rd => (Full(rd.id.is), rd.name.is) )
    }
    object Parentid extends StringRefField(this, Factor, 512){
      override def options = Factor.findAll.map(rd => (Full(rd.id.is), rd.FactorName.is) )
    }
    //  object Parentid extends MongoRefField(this)
    object ParentName extends StringField(this, 512)
    object FactorName extends StringField(this, 512)
    object Description extends StringField(this, 512)
    object Weight extends DoubleField(this)
    object Ordinal extends IntField(this)
    object Status extends StringField(this, 512)
    object Note extends StringField(this, 512)
    object PercentTotal extends DoubleField(this)
    object PathFactor extends BsonRecordListField(this, FactorPath)

    object FactorOption extends BsonRecordListField(this, FactorOptionIN)

    // An embedded document:
    //  object factor extends BsonRecordListField(this, factorIN)

  }

  object Factor extends Factor with MongoMetaRecord[Factor] {
    override def collectionName = "factor"
  }

  class FactorPath private () extends BsonRecord[FactorPath] {
    def meta = FactorPath
    object FactorPathId extends StringRefField(this, Factor, 512){
      override def options = Factor.findAll.map(rd => (Full(rd.id.is), rd.FactorName.is) )
    }
    //  object FactorPathId extends StringField(this, 512)
    object Weight extends DoubleField(this)

  }

  object FactorPath extends FactorPath with BsonMetaRecord[FactorPath]

  class FactorOptionIN private () extends BsonRecord[FactorOptionIN] {
    def meta = FactorOptionIN
    object FactorOptionId extends StringField(this, 50)
    object FactorOptionName extends StringField(this, 512)
    object Description extends StringField(this, 512)
    object Score extends DoubleField(this)
    object Fatal extends StringField(this, 512)
    object Status extends StringField(this, 512)

  }

  object FactorOptionIN extends FactorOptionIN with BsonMetaRecord[FactorOptionIN]
}

