package models
import play.api._
import EnumUtils._
import play.api.libs.json._
import play.api.libs.functional.syntax._
import models.ModelHelper._
import com.github.nscala_time.time.Imports._
import scala.concurrent.ExecutionContext.Implicits.global
import org.mongodb.scala.bson._
import org.mongodb.scala.model._

case class Monitor(_id: String, indParkName: String, dp_no: String,
                   lat: Option[Double] = None, lng: Option[Double] = None, show: Boolean = false,
                   district: String = "", ordinary: Boolean = true, industrial: Boolean = false, traffic: Boolean = false,
                   park: Boolean = false, background: Boolean = false, other: Boolean = false)

object Monitor extends Enumeration {
  implicit val monitorRead: Reads[Monitor.Value] = EnumUtils.enumReads(Monitor)
  implicit val monitorWrite: Writes[Monitor.Value] = EnumUtils.enumWrites
  implicit val autoAuditRead = Json.reads[AutoAudit]
  implicit val autoAuditWrite = Json.writes[AutoAudit]

  implicit val mWrite = Json.writes[Monitor]
  implicit val mRead = Json.reads[Monitor]

  import org.mongodb.scala.bson.codecs.Macros._
  import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
  import org.bson.codecs.configuration.CodecRegistries.{ fromRegistries, fromProviders }

  import scala.concurrent._
  import scala.concurrent.duration._

  implicit object TransformMonitor extends BsonTransformer[Monitor.Value] {
    def apply(m: Monitor.Value): BsonString = new BsonString(m.toString)
  }

  val colName = "monitors"
  val codecRegistry = fromRegistries(fromProviders(classOf[Monitor]), DEFAULT_CODEC_REGISTRY)
  val collection = MongoDB.database.getCollection[Monitor](colName).withCodecRegistry(codecRegistry)

  def monitorId(indParkName: String, dp_no: String) = s"${indParkName}#${dp_no}"

  def buildMonitor(indParkName: String, dp_no: String) = {
    assert(!indParkName.isEmpty)
    assert(!dp_no.isEmpty)

    Monitor(monitorId(indParkName, dp_no), indParkName, dp_no)
  }

  val districtMap = Map(
    1 -> "基隆市",
    2 -> "臺北市",
    3 -> "新北市",
    4 -> "桃園市",
    5 -> "新竹市",
    6 -> "新竹縣",
    7 -> "苗栗縣",
    8 -> "臺中市",
    9 -> "彰化縣",
    10 -> "南投縣",
    11 -> "雲林縣",
    12 -> "嘉義市",
    13 -> "嘉義縣",
    14 -> "臺南市",
    15 -> "高雄市",
    16 -> "屏東縣",
    17 -> "臺東縣",
    18 -> "花蓮縣",
    19 -> "宜蘭縣",
    20 -> "連江縣",
    21 -> "金門縣",
    22 -> "澎湖縣")

  def importEpaMonitors = {
    import scala.concurrent.ExecutionContext.Implicits.global
    import scalikejdbc._

    Logger.info("Import EPA monitos...")
    
    val result = DB readOnly {
      import java.util.Date
      implicit session =>
        sql"""
          SELECT [SiteId]
          ,[Name]
          ,[district]
          ,[ordinary]
          ,[Industrial]
          ,[traffic]
          ,[park]
          ,[background]
          ,[other]
          FROM [YLDB].[dbo].[EpaMonitor] 
        """.map {
          rs =>
            import org.joda.time.format.DateTimeFormat
            val siteID = rs.int("SiteId")
            val name = rs.string("Name")
            val district = districtMap(rs.intOpt("district").getOrElse(1))
            val ordinary = rs.boolean("ordinary")
            val industrial = rs.boolean("Industrial")
            val traffic = rs.boolean("traffic")
            val park = rs.boolean("park")
            val background = rs.boolean("background")
            val other = rs.boolean("other")
            Monitor(_id = monitorId("環保署", siteID.toString()), indParkName = "環保署", dp_no = name,
              lat = None, lng = None, show = false,
              district = district, ordinary = ordinary, industrial = industrial, traffic = traffic,
              park = park, background = background, other = other)
        }.list().apply()
    }

    Logger.info(s"Total ${result.length} monitors")
    result map { newMonitor }
  }
  def init(colNames: Seq[String]) = {
    if (!colNames.contains(colName)) {
      val f = MongoDB.database.createCollection(colName).toFuture()
      f.onFailure(errorHandler)
      f.onSuccess({
        case _: Seq[t] =>          
      })
      
      waitReadyResult(f)
      importEpaMonitors
    } 
  }

  def newMonitor(m: Monitor) = {
    Logger.debug(s"Create monitor value ${m._id}!")
    val v = Value(m._id)
    map = map + (v -> m)
    mvList = (v :: mvList.reverse).reverse

    val f = collection.insertOne(m).toFuture()
    f.onFailure(errorHandler)
    f.onSuccess({
      case _: Seq[t] =>
    })
    Monitor.withName(m._id)
  }

  private def mList: List[Monitor] =
    {
      val f = collection.find().toFuture()
      val ret = waitReadyResult(f)
      ret.toList
    }

  def refresh = {
    val list = mList
    map = Map.empty[Monitor.Value, Monitor]
    for (m <- list) {
      try {
        val mv = Monitor.withName(m._id)
        map = map + (mv -> m)
      } catch {
        case _: NoSuchElementException =>
          map = map + (Value(m._id) -> m)
      }
    }
    mvList = list.map(m => Monitor.withName(m._id))

  }

  var map: Map[Value, Monitor] = Map(mList.map { e => Value(e._id) -> e }: _*)
  var mvList = mList.map(mt => Monitor.withName(mt._id))
  def indParkSet = mvList.map { map(_).indParkName }.foldRight(Set.empty[String])((name, set) => set + name)
  def indParkMonitor(indParkFilter: Seq[String]) =
    mvList.filter(p => {
      val monitor = Monitor.map(p)
      indParkFilter.contains(monitor.indParkName)
    })

  def indParkMonitor(indPark: String) =
    mvList.filter(p => {
      val monitor = Monitor.map(p)
      monitor.indParkName == indPark
    })

  def getMonitorValueByName(indParkName: String, dp_no: String) = {
    try {
      val id = monitorId(indParkName, dp_no)
      Monitor.withName(id)
    } catch {
      case _: NoSuchElementException =>
        newMonitor(buildMonitor(indParkName, dp_no))
    }
  }

  def getMonitorValueBySiteIdName(indParkName: String, siteID:Int, name: String = "") = {
    try {
      val id = monitorId(indParkName, siteID.toString())
      Monitor.withName(id)
    } catch {
      case _: NoSuchElementException =>
        val monitor = Monitor(_id = monitorId("環保署", siteID.toString()), indParkName = "環保署", dp_no = name,
              lat = None, lng = None, show = false)
        newMonitor(monitor)
    }
  }
  
  def format(v: Option[Double]) = {
    if (v.isEmpty)
      "-"
    else
      v.get.toString
  }

  def updateMonitor(m: Monitor.Value, colname: String, newValue: String) = {
    import org.mongodb.scala._
    import org.mongodb.scala.model.Filters._
    import org.mongodb.scala.model.Updates._
    import org.mongodb.scala.model.FindOneAndUpdateOptions

    import scala.concurrent.ExecutionContext.Implicits.global
    Logger.debug(s"col=$colname newValue=$newValue")
    val idFilter = equal("_id", map(m)._id)
    val opt = FindOneAndUpdateOptions().returnDocument(com.mongodb.client.model.ReturnDocument.AFTER)
    val f =
      if (newValue == "-")
        collection.findOneAndUpdate(idFilter, set(colname, null), opt).toFuture()
      else {
        import java.lang.Double
        collection.findOneAndUpdate(idFilter, set(colname, Double.parseDouble(newValue)), opt).toFuture()
      }

    val mCase = waitReadyResult(f)

    map = map + (m -> mCase)
  }

  def upsert(m: Monitor) = {
    val f = collection.replaceOne(Filters.equal("_id", m._id), m, UpdateOptions().upsert(true)).toFuture()
    f.onFailure(errorHandler)
    f
  }

  def updateMonitorAutoAudit(m: Monitor.Value, autoAudit: AutoAudit) = {
    import org.mongodb.scala._
    import org.mongodb.scala.model.Filters._
    import org.mongodb.scala.model.Updates._
    import org.mongodb.scala.model.FindOneAndUpdateOptions

    import scala.concurrent.ExecutionContext.Implicits.global

    ???
    /*
    val idFilter = equal("_id", map(m)._id)
    val opt = FindOneAndUpdateOptions().returnDocument(com.mongodb.client.model.ReturnDocument.AFTER)
    val f = collection.findOneAndUpdate(idFilter, set("autoAudit", autoAudit.toDocument), opt).toFuture()

    val ret = waitReadyResult(f)

    val mCase = toMonitor(ret)
    map = map + (m -> mCase)
    *
    */
  }

  def getCenterLat(privilege: Privilege) = {
    val monitors = privilege.allowedMonitors.filter { m => privilege.allowedIndParks.contains(Monitor.map(m).indParkName) }
    val latList = monitors.flatMap { m => Monitor.map(m).lat }
    latList.sum / latList.length
  }

  def getCenterLng(privilege: Privilege) = {
    val monitors = privilege.allowedMonitors.filter { m => privilege.allowedIndParks.contains(Monitor.map(m).indParkName) }
    val lngList = monitors.flatMap { m => Monitor.map(m).lng }
    lngList.sum / lngList.length
  }
}