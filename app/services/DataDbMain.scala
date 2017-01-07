package services

import javax.inject.Inject
import play.api.db._
import anorm._
import model.ModelHome
import model.KpiView
import model.DoubleDate
import model.KpiTS
import java.time.LocalDate


class DataDbMain @Inject()(db: Database){
  case class IntDate(value: Int, date: LocalDate)
  case class LongDate(value: Long, date: LocalDate)
  //case class DoubleDate(value: Double, date: LocalDate)
  
  /*
   * Retrieve a kpi value and its date of last update
   * The kpicolumn needs to have the aggregation function in it.
   */
  def getLastTot(kpi_col_name: String, table: String, where: String = null): DoubleDate = {
    
    //To be reviewed: build a cache table with this info
    
    val parser: RowParser[LongDate] = Macro.namedParser[LongDate]
    
    val day: String = if(table.equals("transactions")) "DATA_EMISSIONE" else "day"
    
    //Get Last Value
    val dataLast: List[LongDate] = db.withConnection { implicit connection =>
      SQL(s"""
        SELECT $kpi_col_name as value, max($day) as date 
        FROM $table
        ${if(!Option(where).getOrElse("").isEmpty) "where " + where else ""}
        """)
   	   .as(parser.*)
    }
    DoubleDate(dataLast(0).value.toDouble, dataLast(0).date)
  }
  
  /*
   * Retrieve the most udated number of total active users 
   * (users that have done at least one transaction).
   * 
   */
  
  def getViewHL(kpi_col_name: String, table: String, where: String = null): KpiView = {
    
    //To be reviewed: build a cache table with this info
    val parserIntDate: RowParser[DoubleDate] = Macro.namedParser[DoubleDate]
    
    //Get Last Value
    val dataLast: List[DoubleDate] = db.withConnection { implicit connection =>
      SQL(s"""
        SELECT sum($kpi_col_name) as kpi, max(day) as date 
        FROM $table
        ${if(!Option(where).getOrElse("").isEmpty) "where " + where else ""}
        """)
        //.on("col_name" -> kpi_col_name)
   	   .as(parserIntDate.*)
    }
    
    //Get one week ago Value
    //val where2 = "day <= {lastDate}"
    val dataWeekAgo: List[DoubleDate] = db.withConnection { implicit connection =>
      SQL(s"""
        SELECT sum($kpi_col_name) as kpi, max(day) as date 
        FROM $table
        ${if(!Option(where).getOrElse("").isEmpty) "where " + where + " and day <= {lastDate}" else "where day <= {lastDate}"}
        """)
        .on("lastDate" ->dataLast(0).date.minusDays(7).toString)
   	    .as(parserIntDate.*)
    }
    
    //Get one month ago Value
    val dataMonthAgo: List[DoubleDate] = db.withConnection { implicit connection =>
      SQL(s"""
        SELECT sum($kpi_col_name) as kpi, max(day) as date 
        FROM $table
        ${if(!Option(where).getOrElse("").isEmpty) "where " + where + " and day <= {lastDate}" else "where day <= {lastDate}"}
        """)
        .on("lastDate" ->dataLast(0).date.minusDays(30).toString)
   	    .as(parserIntDate.*)
    }
    
    KpiView(dataLast(0).kpi, dataWeekAgo(0).kpi, dataMonthAgo(0).kpi)
    
  }
  
  
  /*
   * Retrieve TimeSeries Data
   */
  def getTS(kpi_col_name: String,
      where: String,
      table: String): KpiTS = {
    
    val parser: RowParser[DoubleDate] = Macro.namedParser[DoubleDate]
    val dataLast: List[DoubleDate] = db.withConnection { implicit connection =>
  	  SQL(s"""
  	    select day as date, sum($kpi_col_name) as kpi
  	    from $table
  	    ${if(!Option(where).getOrElse("").isEmpty) "where " + where else ""}
  	    group by day
  	    order by day
  	    """).as(parser.*)
    }
    
    KpiTS(
        dataLast.map(x => x.date),
        dataLast.map(x => x.kpi)
    )
  }
  
  def getTimeSerie2(kpi_col_name: String,
      dateStart: String = "",
      dateEnd: String = "",
      table: String): KpiTS = {
    
    val parser: RowParser[DoubleDate] = Macro.namedParser[DoubleDate]
    val dataLast: List[DoubleDate] = db.withConnection { implicit connection =>
  	  SQL(s"""
  	    select day as date, sum($kpi_col_name) as kpi
  	    from $table
  	    group by day
  	    order by day
  	    """).as(parser.*)
    }
    
    KpiTS(
        dataLast.map(x => x.date),
        dataLast.map(x => x.kpi)
    )
  }
  
  

  
}