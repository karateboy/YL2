package models

import akka.actor.{ Actor, ActorLogging, Props, ActorRef }
import javax.xml.ws.Holder
import play.api.Play.current
import play.api.libs.concurrent.Akka
import play.api._
import akka.actor.actorRef2Scala
import scala.concurrent.ExecutionContext.Implicits.global
import org.mongodb.scala.model._
import ModelHelper._

object CdxReceiver {
  val props = Props[CdxReceiver]
  case object GetInBoxFiles
  case object ParseXML

  import com.typesafe.config.ConfigFactory
  val cdxConfig = ConfigFactory.load("cdx")
  val enable = cdxConfig.getBoolean("enable")
  val account = cdxConfig.getString("account")
  val password = cdxConfig.getString("password")

  var receiver: ActorRef = _
  def startup() = {
    receiver = Akka.system.actorOf(props, name = "cdxReceiver")
    Logger.info(s"CDX receiver is $enable")
    if (enable) {
      val timer = {
        import scala.concurrent.duration._
        Akka.system.scheduler.schedule(Duration(5, SECONDS), Duration(1, HOURS), receiver, GetInBoxFiles)
      }
    }
  }

  def getInboxFiles = {
    receiver ! GetInBoxFiles
  }
  def parseXML = {
    receiver ! ParseXML
  }
}

class CdxReceiver extends Actor with ActorLogging {
  import CdxReceiver._
  import com.github.nscala_time.time.Imports._

  val path = current.path.getAbsolutePath + "/importEPA/"
  def receive = {
    case GetInBoxFiles =>
      try {
        getInBoxFileList(account, password, "AQX_P_15")
        Logger.info("GetInBoxFiles done.")
      } catch {
        case ex: Throwable =>
          Logger.error("getInBoxFileList failed", ex)
      }
    case ParseXML =>
      try {
        parseAllXml(path)(parser)
        Logger.info("ParseXML done.")
      } catch {
        case ex: Throwable =>
          Logger.error("ParseXML failed", ex)
      }
  }

  def getInBoxFileList(account: String, password: String, serviceId: String) = {
    val errMsgHolder = new Holder("")
    val resultHolder = new Holder[Integer]
    val fileListHolder = new Holder[com.wecc.cdx.ArrayOfAnyType]
    CdxWebService.service.getFileListByServiceId(account, password, "Inbox", serviceId, errMsgHolder, resultHolder, fileListHolder)
    if (resultHolder.value != 1) {
      Logger.error(s"errMsg:${errMsgHolder.value}")
      Logger.error(s"ret:${resultHolder.value.toString}")
    } else {
      val fileList = fileListHolder.value.getAnyType.asInstanceOf[java.util.ArrayList[String]]
      def getFile(fileName: String) = {

        val resultHolder = new Holder[Integer]
        val errMsgHolder = new Holder("")
        val fileBuffer = new Holder[Array[Byte]]
        CdxWebService.service.getFile(account, password, fileName, "Inbox", errMsgHolder, resultHolder, fileBuffer)
        if (resultHolder.value != 1) {
          Logger.error(s"errMsg:${errMsgHolder.value}")
          Logger.error(s"ret:${resultHolder.value.toString}")
        } else {
          import java.io._
          val content = fileBuffer.value
          val os = new FileOutputStream(s"$path$fileName")
          os.write(content)
          os.close()
        }
      }

      def removeFileFromServer(fileName: String) = {
        val resultHolder = new Holder[Integer]
        val errMsgHolder = new Holder("")
        val successHolder = new Holder[java.lang.Boolean]
        CdxWebService.service.getFileFinish(account, password, fileName, "Inbox", errMsgHolder, resultHolder, successHolder)
        if (resultHolder.value != 1) {
          Logger.error(s"errMsg:${errMsgHolder.value}")
          Logger.error(s"ret:${resultHolder.value.toString}")
        }
      }

      def backupFile(fileName: String) = {
        import java.nio.file._
        val srcPath = Paths.get(s"$path$fileName")
        val destPath = Paths.get(s"${path}backup/${fileName}")
        Files.move(srcPath, destPath, StandardCopyOption.REPLACE_EXISTING)
      }

      for (idx <- 0 to fileList.size() - 1) {
        val fileName = fileList.get(idx)
        Logger.debug(s"get ${fileList.get(idx)}")
        getFile(fileName)
        if (fileName.startsWith("AQX")) {
          val file = new java.io.File(s"$path$fileName")
          parser(file)
          backupFile(fileName)
        } else {
          import java.nio.file._
          Files.deleteIfExists(Paths.get(s"$path$fileName"))
        }
        removeFileFromServer(fileName)
      }
    }
  }

  import java.io.File
  def parser(f: File) {
    import scala.xml.Node
    import scala.collection.mutable.Map
    val recordMap = Map.empty[Monitor.Value, Map[DateTime, Map[MonitorType.Value, (Double, String)]]]

    var correctCount = 0
    def processData(data: Node) = {
      val siteIdOpt = data.attribute("SiteId")
      val itemIdOpt = data.attribute("ItemId")
      val monitorDateOpt = data.attribute("MonitorDate")
      val siteName = data.attribute("SiteName")

      try {
        correctCount += 1
        if (siteIdOpt.isDefined && itemIdOpt.isDefined && monitorDateOpt.isDefined) {
          for {
            itemId <- itemIdOpt
            monitorType <- MonitorType.getMonitorTypeByItemID(itemId.text.toInt)
            monitor = Monitor.getMonitorValueBySiteIdName("環保署", siteIdOpt.get.text.toInt, siteName.get.text)
            mDate = DateTime.parse(s"${monitorDateOpt.get.text}", DateTimeFormat.forPattern("YYYY-MM-dd"))
          } {

            val monitorNodeValueSeq =
              for (v <- 0 to 23)
                yield (mDate + v.hour, data.attribute("MonitorValue%02d".format(v)))

            val mtValueSeq = monitorNodeValueSeq.filter(_._2.isDefined).filter { node =>
              val validNumber = try {
                node._2.get.text.toDouble
                true
              } catch {
                case _: NumberFormatException =>
                  false
              }
              validNumber
            }.map(n => (n._1, n._2.get.text.toDouble))

            for {
              mtValuePair <- mtValueSeq
              timeMap = recordMap.getOrElseUpdate(monitor, Map.empty[DateTime, Map[MonitorType.Value, (Double, String)]])
              mtMap = timeMap.getOrElseUpdate(mtValuePair._1, Map.empty[MonitorType.Value, (Double, String)])
              mvValue = mtValuePair._2
            } {
              mtMap.put(monitorType, (mvValue, MonitorStatus.NormalStat))
            }

          }
        }
      } catch {
        case ex: Throwable =>
          Logger.error("skip Invalid record", ex)
      }
    }

    if (f.getName.startsWith("AQX_P_15")) {
      val node = xml.XML.loadFile(f)
      node match {
        case <AqxData>{ data @ _* }</AqxData> =>
          data.map { processData }
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

    val collection =
      MongoDB.database.getCollection(Record.HourCollection)

    val f2 = collection.bulkWrite(updateModels.toList, BulkWriteOptions().ordered(false)).toFuture()
    f2.onFailure(errorHandler)
    waitReadyResult(f2)

    Logger.info(s"${f.getName} finished")

  }

  def parseAllXml(dir: String)(parser: (File) => Unit) = {

    def listAllFiles = {
      import java.io.FileFilter
      new java.io.File(dir).listFiles.filter(_.getName.endsWith(".xml"))
    }

    def backupFile(fileName: String) = {
      import java.nio.file._
      val srcPath = Paths.get(s"$path$fileName")
      val destPath = Paths.get(s"${path}backup/${fileName}")
      try {
        Files.move(srcPath, destPath, StandardCopyOption.REPLACE_EXISTING)
      } catch {
        case ex: Throwable =>
          Logger.error("backup failed", ex)
      }
    }

    val files = listAllFiles
    for (f <- files) {
      if (f.getName.startsWith("AQX")) {
        parser(f)
        backupFile(f.getName)
      } else {
        f.delete()
      }
    }
  }
}