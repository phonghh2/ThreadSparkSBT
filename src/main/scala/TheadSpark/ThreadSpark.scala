package TheadSpark

/**
 * Created by phong on 8/6/2015.
 */

import java.net.URI

import com.mongodb.{Mongo, ServerAddress}
import com.redis._
import net.liftweb.mongodb._
import net.liftweb.mongodb.record.field.StringPk
import net.liftweb.mongodb.record.{MongoMetaRecord, MongoRecord}
import net.liftweb.record.field.{DoubleField, StringField}
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
    val thread = new Thread {
      override def run {
        val conf = new SparkConf().setMaster("local[*]").setAppName("CamusApp")
        val sc = new SparkContext(conf)
        val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
        val df = hiveContext.read.json("hdfs://10.15.171.36:54310/home/phonghh/project/demo/camusDisk/topics/Scoring/hourly/*/*/*/*")
        df.registerTempTable("HDFS")

        MongoUrl.defineDb(DefaultMongoIdentifier, "mongodb://10.15.171.35:27017/ScoringCardDB")
        val DBListModel = ModelInfo.findAll
        for(x<-DBListModel){
          RangeScoring(x.id.toString(), hiveContext)
        }
        TopBotOption("d848e3f9-9ae6-4c46-ba46-62adb892e94d", hiveContext)
        TopBotOption("878578e5-c9f4-430e-a129-446eaa69b374", hiveContext)
      }
    }
    thread.start
    Thread.sleep(7200*1000) // slow the loop down a bit
  }

  def RangeScoring(ModelId : String, hiveContext:HiveContext) = {
    val query = hiveContext.sql("SELECT rating_code, rating_status, COUNT(scoring) application_count, SUM(scoring)/COUNT(scoring) TB FROM HDFS WHERE rate.modelid = '" + ModelId + "' GROUP BY rating_code, rating_status ORDER BY TB")
    val a = query.toJSON.collect()
    val r = new RedisClient("10.15.171.41", 6379)
    r.del("Spark-RangeScoring-" + ModelId)
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
}

