package models
import play.api._
import akka.actor._
import play.api.Play.current
import play.api.libs.concurrent.Akka
import com.github.nscala_time.time.Imports._
import play.api.Play.current
import ModelHelper._
import play.api.libs.json._
import play.api.libs.functional.syntax._
import scala.concurrent.ExecutionContext.Implicits.global
import scalikejdbc._
import org.mongodb.scala.model._
import org.mongodb.scala.bson._

case class AvgRecord(dp_no: String, mt: MonitorType.Value, dateTime: DateTime, value: Option[Double], status: Option[String])
object CopyStep extends Enumeration {
  val hour = Value
}
object DataCopyer {
  case object StartCopy
  case class CopyDay(begin: DateTime)

  var hourCopyer: ActorRef = _

  def startup() = {
    val enabled = Play.current.configuration.getBoolean("dataCopy.enable").getOrElse(false)

    Logger.info(s"DataCopy is $enabled")

    if (enabled) {
      hourCopyer = Akka.system.actorOf(Props(classOf[DataCopyer], CopyStep.hour), name = "hourCopyer")
    }
  }

  val unknownMonitor = "Unknown"

  val mtMap = Map(
    "A214" -> "PM10",
    "A222" -> "二氧化硫",
    "A223" -> "氮氧化物",
    "A283" -> "一氧化氮",
    "A293" -> "二氧化氮",
    "A224" -> "一氧化碳",
    "A225" -> "臭氧",
    "A224" -> "一氧化碳",
    "A225" -> "臭氧",
    "A227" -> "總碳氫化合物",
    "C211" -> "風速",
    "C212" -> "風向",
    "C213" -> "降雨量",
    "C214" -> "溫度",
    "C215" -> "相對濕度",
    "U201" -> "乙烷",
    "U202" -> "乙烯",
    "U203" -> "丙烷",
    "U204" -> "丙烯",
    "U205" -> "異丁烷",
    "U206" -> "正丁烷",
    "U207" -> "乙炔",
    "U208" -> "反2-丁烯",
    "U209" -> "1-丁烯",
    "U210" -> "順2-丁烯",
    "U211" -> "環戊烷",
    "U212" -> "異戊烷",
    "U213" -> "正戊烷",
    "U214" -> "反2-戊烯",
    "U215" -> "1-戊烯",
    "U216" -> "順2-戊烯",
    "U217" -> "2,2-二甲基丁烷",
    "U218" -> "2,3-二甲基丁烷",
    "U219" -> "2-甲基戊烷",
    "U220" -> "3-甲基戊烷",
    "U221" -> "異戊二烯",
    "U222" -> "正己烷",
    "U223" -> "甲基環戊烷",
    "U224" -> "2,4-二甲基戊烷",
    "U225" -> "苯",
    "U226" -> "環己烷",
    "U227" -> "2-甲基己烷",
    "U228" -> "2,3-二甲基戊烷",
    "U229" -> "3-甲基己烷",
    "U230" -> "2,2,4-三甲基戊烷",
    "U231" -> "正庚烷",
    "U232" -> "甲基環己烷",
    "U233" -> "2,3,4-三甲基戊烷",
    "U234" -> "甲苯",
    "U235" -> "2-甲基庚烷",
    "U236" -> "3-甲基庚烷",
    "U237" -> "正辛烷",
    "U238" -> "乙苯",
    "U239" -> "間,對二甲苯",
    "U240" -> "苯乙烯",
    "U241" -> "鄰二甲苯",
    "U242" -> "正壬烷",
    "U243" -> "異丙基苯",
    "U244" -> "正丙基苯",
    "U245" -> "間-乙基甲苯",
    "U246" -> "對-乙基甲苯",
    "U247" -> "1,3,5-三甲基苯",
    "U248" -> "鄰-乙基甲苯",
    "U249" -> "1,2,4-三甲基苯",
    "U250" -> "葵烷",
    "U251" -> "1,2,3-三甲基苯",
    "U252" -> "間-二乙基苯",
    "U253" -> "對-二乙基苯",
    "U254" -> "正十一烷")

  def getCopyStart(step: CopyStep.Value) = {
    val latestF = step match {
      case CopyStep.hour =>
        SysConfig.get(SysConfig.EPAHR_LAST)
    }

    for { latest <- latestF } yield {
      val begin = step match {
        case CopyStep.hour =>
          val rawHour = new DateTime(latest.asDateTime().toDate())
          rawHour.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
      }

      begin
    }
  }

  def copyDbDay(begin: DateTime) {
    Logger.info(s"copy day ${begin.toString("YY/MM/dd HH:mm")}")

    val result = DB readOnly {
      import java.util.Date
      implicit session =>
        val start = begin.toDate()
        val end = (begin + 1.day).toDate()
        sql"""
          Select [MStation], [MDate], [MItem], [MValue]
          From hour_data
          Where [MDate] >= ${start} and [MDate] < ${end} 
        """.map {
          rs =>
            val siteID = rs.int("MStation")
            val date = rs.timestamp("MDate")
            val item = rs.string("MItem").toInt
            val v = rs.doubleOpt("MValue")
            val mt = MonitorType.getMonitorTypeByItemID(item)
            if (mt.isDefined)
              Some(AvgRecord(Monitor.monitorId("環保署", siteID.toString()), mt.get, new DateTime(date), v, Some(MonitorStatus.NormalStat)))
            else {
              Logger.error("Unknown itemID " + item)
              None
            }
        }.list().apply()
    }

    val recordList = result.flatten

    def updateDB = {
      import scala.collection.mutable.Map
      val recordMap = Map.empty[Monitor.Value, Map[DateTime, Map[MonitorType.Value, (Double, String)]]]
      for (record <- recordList) {
        try {
          val monitor = Monitor.withName(record.dp_no)
          val monitorType = record.mt
          val timeMap = recordMap.getOrElseUpdate(monitor, Map.empty[DateTime, Map[MonitorType.Value, (Double, String)]])
          val mtMap = timeMap.getOrElseUpdate(record.dateTime, Map.empty[MonitorType.Value, (Double, String)])
          for {
            v <- record.value
            s <- record.status
          } {
            mtMap.put(record.mt, (v, s))
          }
        } catch {
          case ex: Throwable =>
            Logger.error("skip invalid record ", ex)
        }
      }

      val updateModels =
        for {
          monitorMap <- recordMap
          monitor = monitorMap._1
          timeMaps = monitorMap._2
          dateTime <- timeMaps.keys.toList.sorted
          mtMaps = timeMaps(dateTime) if (!mtMaps.isEmpty)
          doc = Record.toDocument(monitor, dateTime, mtMaps.toList)
          updateList = doc.toList.map(kv => Updates.set(kv._1, kv._2)) if !updateList.isEmpty
        } yield {
          UpdateOneModel(
            Filters.eq("_id", doc("_id")),
            Updates.combine(updateList: _*), UpdateOptions().upsert(true))
        }

      val monitorEnds = recordMap map { map =>
        map._2.keys.max
      }

      val collection =
        MongoDB.database.getCollection(Record.HourCollection)

      val f2 = collection.bulkWrite(updateModels.toList, BulkWriteOptions().ordered(false)).toFuture()
      f2.onFailure(errorHandler)
      waitReadyResult(f2)

    }

    if (!recordList.isEmpty)
      updateDB

    SysConfig.set(SysConfig.EPAHR_LAST, begin + 1.month)
  }
}

class DataCopyer(step: CopyStep.Value) extends Actor with ActorLogging {
  import DataCopyer._

  Logger.info(s"$step Copyer start...")
  val timer = {
    import scala.concurrent.duration._
    step match {
      case CopyStep.hour =>
        Akka.system.scheduler.schedule(Duration(5, SECONDS), Duration(5, MINUTES), self, StartCopy)
    }
  }

  def receive = handler(false)

  def handler(copying: Boolean): Receive = {
    case StartCopy =>
      if (!copying) {
        for (start <- getCopyStart(step)) {
          if (start < DateTime.now()) {
            self ! CopyDay(start)
            context become handler(true)
          }
        }
      }

    case CopyDay(start) =>
      copyDbDay(start)
      val now = DateTime.now()
      val nextStart = start + 1.day
      if (nextStart < now) {
        self ! CopyDay(nextStart)
      } else {
        context become handler(false)
      }
  }

  override def postStop(): Unit = {
    timer.cancel()
  }
}