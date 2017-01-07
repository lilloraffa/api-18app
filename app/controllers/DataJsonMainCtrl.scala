package controllers

import javax.inject.Inject

import play.api.Play.current
import play.api.mvc._
import play.api.db._
import play.api.libs.json._
import play.api.libs.json.Reads._
import anorm._
import model.ModelHome
import model.TestDb
import model.KpiView
import model.KpiViewBy
import model.KpiTS
import model.DoubleDate
import services.DataDbMain
import services.DataDbProd
import services.DataDbShop
import services.DataDbPop

class DataJsonMainCtrl @Inject()(db: Database) extends Controller {
  case class DbDetails(colName: String, table: String, kpiName: String)
  val kpiDBMap: Map[String, DbDetails] = Map(
      //Last Value - (Double, Date)
      "lastval_shop_num" -> DbDetails("count(distinct ID_ESERCENTE)", "day_detshop", "Tot Shop Number"),
      
      //View HighLevel - Aggregated part
      "viewhl_tot_user_act" -> DbDetails("userNew_num", "day_aggr", "Total Active Users"),
      "viewhl_tot_valid_val" -> DbDetails("valid_val", "day_aggr", "Total Validation Amount"),
      "viewhl_tot_valid_val_det" -> DbDetails("valid_val", "day_det", "Total Validation Amount"),
      "viewhl_tot_valid_num" -> DbDetails("valid_num", "day_aggr", "Total Validation Number"),
      "viewhl_tot_valid_num_det" -> DbDetails("valid_num", "day_det", "Total Validation Number"),
      "viewhl_tot_pren_val" -> DbDetails("pren_val", "day_aggr", "Total Pren Amount"),
      "viewhl_tot_pren_val_det" -> DbDetails("pren_val", "day_det", "Total Pren Amount"),
      "viewhl_tot_pren_num" -> DbDetails("pren_num", "day_aggr", "Total Pren Number"),
      "viewhl_tot_pren_num_det" -> DbDetails("pren_num", "day_det", "Total Pren Number"),
      
      //TimeSeries part
      "ts_user_act" -> DbDetails("user_num", "day_aggr", "Active Users"),
      "ts_user_act_det" -> DbDetails("user_num", "day_det", "Active Users"),
      "ts_user_new" -> DbDetails("userNew_num", "day_aggr", "New Active User"),
      "ts_valid_val" -> DbDetails("valid_val", "day_aggr", "Validation Amount"),
      "ts_valid_val_det" -> DbDetails("valid_val", "day_det", "Validation Amount"),
      "ts_valid_num" -> DbDetails("valid_num", "day_aggr", "Validation Number"),
      "ts_valid_num_det" -> DbDetails("valid_num", "day_det", "Validation Number"),
      "ts_pren_val" -> DbDetails("valid_val", "day_det", "Pren Amount"),
      "ts_pren_val_det" -> DbDetails("valid_val", "day_det", "Pren Amount"),
      "ts_pren_num" -> DbDetails("valid_val", "day_det", "Pren Number"),
      "ts_pren_num_det" -> DbDetails("valid_val", "day_det", "Pren Number")
      
  ).withDefaultValue(DbDetails("", "", ""))
  
  def getViewHL(kpi_name: String = "", channel: String ="") = Action{
    //Set Headers
    
    implicit val testFormat = Json.format[KpiView]
    val dataSrv = new DataDbMain(db)
    var where = "1=1"
    where += (if (!Option(channel).getOrElse("").isEmpty) s" and CANALE='$channel'" else "")
    
    val dbParam = kpiDBMap(kpi_name)
    
    if (!dbParam.equals(DbDetails("", "", ""))) {
      val result = dataSrv.getViewHL(kpi_col_name=dbParam.colName, 
          table=dbParam.table, 
          where=where)
      Ok(Json.toJson(result).as[JsObject] + ("kpi_name" -> Json.toJson(dbParam.kpiName)))
    } else {
      Ok("{result => 'Nothing found'}")
    }
    
  }
  
  def getLastTot(kpi_name: String = "", channel: String ="", 
      dateStart: String="", dateEnd: String="") = Action{
    implicit val testFormat = Json.format[DoubleDate]
    val dataSrv = new DataDbMain(db)
    var where = "1=1"
    where += (if (!Option(channel).getOrElse("").isEmpty) s" and CANALE='$channel'" else "")
    where += (if (!Option(dateStart).getOrElse("").isEmpty) s" and day>='$dateStart'" else "")
    where += (if (!Option(dateEnd).getOrElse("").isEmpty) s" and day<='$dateEnd'" else "")
    
    val dbParam = kpiDBMap(kpi_name)
    
    if (!dbParam.equals(DbDetails("", "", ""))) {
      val result = dataSrv.getLastTot(kpi_col_name=dbParam.colName, 
          table=dbParam.table, 
          where=where)
      Ok(Json.toJson(result).as[JsObject] + ("kpi_name" -> Json.toJson(dbParam.kpiName)))
    } else {
      Ok("{result => 'Nothing found'}")
    }
    
  }

  def getTS(kpi_name: String = "", channel: String ="", 
      dateStart: String="", dateEnd: String="") = Action{
    implicit val testFormat = Json.format[KpiTS]
    val dataSrv = new DataDbMain(db)
    var where = "1=1"
    where += (if (!Option(channel).getOrElse("").isEmpty) s" and CANALE='$channel'" else "")
    where += (if (!Option(dateStart).getOrElse("").isEmpty) s" and day>='$dateStart'" else "")
    where += (if (!Option(dateEnd).getOrElse("").isEmpty) s" and day<='$dateEnd'" else "")
    
    val dbParam = kpiDBMap(kpi_name)
    
    if (!dbParam.equals(DbDetails("", "", ""))) {
      val result = dataSrv.getTS(kpi_col_name=dbParam.colName, 
          table=dbParam.table, 
          where=where)
      Ok(Json.toJson(result).as[JsObject] + ("kpi_name" -> Json.toJson(dbParam.kpiName)))
    } else {
      Ok("{result => 'Nothing found'}")
    }
    
  }
  
  def getViewHLByAmbito(kpi_name: String = "", channel: String ="", 
      dateStart: String="", dateEnd: String="") = Action{
    implicit val testFormat1 = Json.format[KpiView]
    implicit val testFormat = Json.format[KpiViewBy]
    
    val dataSrv = new DataDbProd(db)
    var where = "1=1"
    where += (if (!Option(channel).getOrElse("").isEmpty) s" and CANALE='$channel'" else "")
    where += (if (!Option(dateStart).getOrElse("").isEmpty) s" and day>='$dateStart'" else "")
    where += (if (!Option(dateEnd).getOrElse("").isEmpty) s" and day<='$dateEnd'" else "")
    
    val dbParam = kpiDBMap(kpi_name)
    
    if (!dbParam.equals(DbDetails("", "", ""))) {
      val result = dataSrv.getViewHLByAmbito(kpi_col_name=dbParam.colName, 
          table=dbParam.table, 
          where=where)
      //Ok(Json.toJson(result).as[JsObject] + ("kpi_name" -> Json.toJson(dbParam.kpiName)))
      Ok(Json.toJson(result))
    } else {
      Ok("{result => 'Nothing found'}")
    }
    
  }
  
  def getViewHLByShop(kpi_name: String = "", channel: String ="", 
      dateStart: String="", dateEnd: String="") = Action{
    implicit val testFormat1 = Json.format[KpiView]
    implicit val testFormat = Json.format[KpiViewBy]
    
    val dataSrv = new DataDbShop(db)
    var where = "1=1"
    where += (if (!Option(channel).getOrElse("").isEmpty) s" and CANALE='$channel'" else "")
    where += (if (!Option(dateStart).getOrElse("").isEmpty) s" and day>='$dateStart'" else "")
    where += (if (!Option(dateEnd).getOrElse("").isEmpty) s" and day<='$dateEnd'" else "")
    
    val dbParam = kpiDBMap(kpi_name)
    
    if (!dbParam.equals(DbDetails("", "", ""))) {
      val result = dataSrv.getViewHLByShop(kpi_col_name=dbParam.colName, 
          table=dbParam.table, 
          where=where)
      //Ok(Json.toJson(result).as[JsObject] + ("kpi_name" -> Json.toJson(dbParam.kpiName)))
      Ok(Json.toJson(result))
    } else {
      Ok("{result => 'Nothing found'}")
    }
    
  }

  def getViewHLByReg(kpi_name: String = "", channel: String ="", 
      dateStart: String="", dateEnd: String="") = Action{
    implicit val testFormat1 = Json.format[KpiView]
    implicit val testFormat = Json.format[KpiViewBy]
    
    val dataSrv = new DataDbPop(db)
    var where = "1=1"
    where += (if (!Option(channel).getOrElse("").isEmpty) s" and CANALE='$channel'" else "")
    where += (if (!Option(dateStart).getOrElse("").isEmpty) s" and day>='$dateStart'" else "")
    where += (if (!Option(dateEnd).getOrElse("").isEmpty) s" and day<='$dateEnd'" else "")
    
    val dbParam = kpiDBMap(kpi_name)
    
    if (!dbParam.equals(DbDetails("", "", ""))) {
      val result = dataSrv.getViewHLByReg(kpi_col_name=dbParam.colName, 
          table=dbParam.table, 
          where=where)
      //Ok(Json.toJson(result).as[JsObject] + ("kpi_name" -> Json.toJson(dbParam.kpiName)))
      Ok(Json.toJson(result))
    } else {
      Ok("{result => 'Nothing found'}")
    }
    
  }
  def getViewHLByProv(kpi_name: String = "", channel: String ="", 
      dateStart: String="", dateEnd: String="") = Action{
    implicit val testFormat1 = Json.format[KpiView]
    implicit val testFormat = Json.format[KpiViewBy]
    
    val dataSrv = new DataDbPop(db)
    var where = "1=1"
    where += (if (!Option(channel).getOrElse("").isEmpty) s" and CANALE='$channel'" else "")
    where += (if (!Option(dateStart).getOrElse("").isEmpty) s" and day>='$dateStart'" else "")
    where += (if (!Option(dateEnd).getOrElse("").isEmpty) s" and day<='$dateEnd'" else "")
    
    val dbParam = kpiDBMap(kpi_name)
    
    if (!dbParam.equals(DbDetails("", "", ""))) {
      val result = dataSrv.getViewHLByProv(kpi_col_name=dbParam.colName, 
          table=dbParam.table, 
          where=where)
      //Ok(Json.toJson(result).as[JsObject] + ("kpi_name" -> Json.toJson(dbParam.kpiName)))
      Ok(Json.toJson(result))
    } else {
      Ok("{result => 'Nothing found'}")
    }
    
  }
  def getViewHLPop(kpi_name: String = "", channel: String ="", 
      dateStart: String="", dateEnd: String="") = Action{
    implicit val testFormat1 = Json.format[KpiView]
    implicit val testFormat = Json.format[KpiViewBy]
    
    val dataSrv = new DataDbPop(db)
    var where = "1=1"
    where += (if (!Option(channel).getOrElse("").isEmpty) s" and CANALE='$channel'" else "")
    where += (if (!Option(dateStart).getOrElse("").isEmpty) s" and day>='$dateStart'" else "")
    where += (if (!Option(dateEnd).getOrElse("").isEmpty) s" and day<='$dateEnd'" else "")
    
    val dbParam = kpiDBMap(kpi_name)
    
    if (!dbParam.equals(DbDetails("", "", ""))) {
      val result = dataSrv.getViewHL(kpi_col_name=dbParam.colName, 
          table=dbParam.table, 
          where=where)
      //Ok(Json.toJson(result).as[JsObject] + ("kpi_name" -> Json.toJson(dbParam.kpiName)))
      Ok(Json.toJson(result).as[JsObject] + ("kpi_name" -> Json.toJson("% Aventi Diritto")))
    } else {
      Ok("{result => 'Nothing found'}")
    }
    
  }
  
}