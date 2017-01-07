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

class DataDbPop @Inject()(db: Database) {
  case class DoubleDateBy (bycol: String, kpi: Double, 
      kpi_1w_ago: Double,
      kpi_1m_ago: Double,
      date: LocalDate)
  
  /*
   * Retrieve the most udated number of total active users 
   * (users that have done at least one transaction).
   * 
   */
  
  def getViewHLByReg(kpi_col_name: String, table: String, where: String = null): List[KpiViewBy] = {
    
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

    val dataLast: List[DoubleDateBy] = db.withConnection { implicit connection =>
      SQL(s"""
        SELECT a.regione as bycol, 
        sum(userNew_num)/sum(Born_98) as kpi,
        sum(userNew_num_1w)/sum(Born_98) as kpi_1w_ago,
        sum(userNew_num_1m)/sum(Born_98) as kpi_1m_ago,
        max(day) as date 
        FROM ( select regione, 
            sum(userNew_num) as userNew_num,
            sum(case when datediff('$dayMaxStr', day)>=7 then userNew_num else 0 end) as userNew_num_1w,
            sum(case when datediff('$dayMaxStr', day)>=30 then userNew_num else 0 end) as userNew_num_1m,
            max(day) as day
            from day_aggr
            group by regione
            ) a
        join (select RegioneName, sum(Born_98) as Born_98
              from catalogs.mix_prov_born
              group by RegioneName
              ) c on a.regione=c.RegioneName
        ${if(!Option(where).getOrElse("").isEmpty) "where " + where else ""}
        group by regione
        order by kpi desc
        """)
        //.on("col_name" -> kpi_col_name)
   	   .as(parser.*)
    }
    
    val listBuffer = dataLast.foldLeft(ListBuffer.empty[KpiViewBy]){
      (m, l) => m += (KpiViewBy(l.bycol, KpiView(l.kpi, l.kpi_1w_ago, l.kpi_1m_ago)))
    }
    listBuffer.toList
  }
  
  def getViewHLByProv(kpi_col_name: String, table: String, where: String = null): List[KpiViewBy] = {
    
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

    val dataLastBest: List[DoubleDateBy] = db.withConnection { implicit connection =>
      SQL(s"""
        SELECT a.prov as bycol, 
        sum(userNew_num)/sum(Born_98) as kpi,
        sum(userNew_num_1w)/sum(Born_98) as kpi_1w_ago,
        sum(userNew_num_1m)/sum(Born_98) as kpi_1m_ago,
        max(day) as date 
        FROM ( select prov, 
            sum(userNew_num) as userNew_num,
            sum(case when datediff('$dayMaxStr', day)>=7 then userNew_num else 0 end) as userNew_num_1w,
            sum(case when datediff('$dayMaxStr', day)>=30 then userNew_num else 0 end) as userNew_num_1m,
            max(day) as day
            from day_aggr
            group by prov
            ) a
        join (select ProvinciaName, sum(Born_98) as Born_98
              from catalogs.mix_prov_born
              group by ProvinciaName
              ) c on a.prov=c.ProvinciaName
        ${if(!Option(where).getOrElse("").isEmpty) "where " + where else ""}
        group by prov
        order by kpi desc
        limit 10
        """)
        //.on("col_name" -> kpi_col_name)
   	   .as(parser.*)
    }
    // Get the least 10
    val dataLastWorst: List[DoubleDateBy] = db.withConnection { implicit connection =>
      SQL(s"""
        SELECT a.prov as bycol, 
        sum(userNew_num)/sum(Born_98) as kpi,
        sum(userNew_num_1w)/sum(Born_98) as kpi_1w_ago,
        sum(userNew_num_1m)/sum(Born_98) as kpi_1m_ago,
        max(day) as date 
        FROM ( select prov, 
            sum(userNew_num) as userNew_num,
            sum(case when datediff('$dayMaxStr', day)>=7 then userNew_num else 0 end) as userNew_num_1w,
            sum(case when datediff('$dayMaxStr', day)>=30 then userNew_num else 0 end) as userNew_num_1m,
            max(day) as day
            from day_aggr
            group by prov
            ) a
        join (select ProvinciaName, sum(Born_98) as Born_98
              from catalogs.mix_prov_born
              group by ProvinciaName
              ) c on a.prov=c.ProvinciaName
        ${if(!Option(where).getOrElse("").isEmpty) "where " + where else ""}
        group by prov
        order by kpi
        limit 10
        """)
        //.on("col_name" -> kpi_col_name)
   	   .as(parser.*)
    }
    
    val listBufferBest = dataLastBest.foldLeft(ListBuffer.empty[KpiViewBy]){
      (m, l) => m += (KpiViewBy(l.bycol, KpiView(l.kpi, l.kpi_1w_ago, l.kpi_1m_ago)))
    }
    val listBufferFinal = dataLastWorst.foldLeft(listBufferBest){
      (m, l) => m += (KpiViewBy(l.bycol, KpiView(l.kpi, l.kpi_1w_ago, l.kpi_1m_ago)))
    }
    listBufferFinal.toList
  }
  
  def getViewHL(kpi_col_name: String, table: String, where: String = null): KpiView = {
    
    //To be reviewed: build a cache table with this info
    val parser: RowParser[KpiView] = Macro.namedParser[KpiView]
    
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

    val dataLast: KpiView = db.withConnection { implicit connection =>
      SQL(s"""
        SELECT
        sum(userNew_num)/sum(Born_98) as kpi,
        sum(userNew_num_1w)/sum(Born_98) as kpi_1w_ago,
        sum(userNew_num_1m)/sum(Born_98) as kpi_1m_ago
        FROM ( select 
            sum(userNew_num) as userNew_num,
            sum(case when datediff('$dayMaxStr', day)>=7 then userNew_num else 0 end) as userNew_num_1w,
            sum(case when datediff('$dayMaxStr', day)>=30 then userNew_num else 0 end) as userNew_num_1m
            from day_aggr
            ) a, (select sum(Born_98) as Born_98
              from catalogs.mix_prov_born
              ) c
        ${if(!Option(where).getOrElse("").isEmpty) "where " + where else ""}
        """)
        //.on("col_name" -> kpi_col_name)
   	   .as(parser.single)
    }
    
    dataLast
  }
}