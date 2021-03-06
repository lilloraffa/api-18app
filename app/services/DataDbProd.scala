package services

import javax.inject.Inject
import play.api.db._
import anorm._
import model.ModelHome
import model.KpiView
import model.KpiViewBy
import model.DoubleDate
import model.KpiTS
import scala.collection.mutable.ListBuffer
import java.time.LocalDate

class DataDbProd @Inject()(db: Database) {
  case class DoubleDateBy (bycol: String, kpi: Double, 
      kpi_1w_ago: Double,
      kpi_1m_ago: Double,
      date: LocalDate)
  
  /*
   * Retrieve the most udated number of total active users 
   * (users that have done at least one transaction).
   * 
   */
  
  def getViewHLByAmbito(kpi_col_name: String, table: String, where: String = null): List[KpiViewBy] = {
    
    //To be reviewed: build a cache table with this info
    val parser: RowParser[DoubleDateBy] = Macro.namedParser[DoubleDateBy]
    
    //Get Last Date
    val dayMax: LocalDate = db.withConnection { implicit connection =>
      SQL(s"""
        SELECT max(day) as date 
        FROM $table
        ${if(!Option(where).getOrElse("").isEmpty) "where " + where else ""}
        """)
   	   .as(SqlParser.scalar[LocalDate].single)
    }
    
    val dayMaxStr = dayMax.toString
    
    //Get Last Value

    val dataLast: List[DoubleDateBy] = db.withConnection { implicit connection =>
      SQL(s"""
        SELECT case when AMBITO_DESC_S IS NOT NULL then AMBITO_DESC_S else 'NOT PRESENT' end as bycol, 
        sum($kpi_col_name) as kpi,
        sum(case when datediff('$dayMaxStr', day)>=7 then $kpi_col_name else 0 end) as kpi_1w_ago,
        sum(case when datediff('$dayMaxStr', day)>=30 then $kpi_col_name else 0 end) as kpi_1m_ago,
        max(day) as date 
        FROM $table a left join categories c on a.ID_AMBITO=c.ID_AMBITO and a.ID_BENE=c.ID_BENE 
        ${if(!Option(where).getOrElse("").isEmpty) "where " + where else ""}
        group by AMBITO_DESC_S
        """)
        //.on("col_name" -> kpi_col_name)
   	   .as(parser.*)
    }

    
    
    val listBuffer = dataLast.foldLeft(ListBuffer.empty[KpiViewBy]){
      (m, l) => m += (KpiViewBy(l.bycol, KpiView(l.kpi, l.kpi_1w_ago, l.kpi_1m_ago)))
    }
    listBuffer.toList
    
  }
}