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

class TestController @Inject()(db: Database) extends Controller {
  def index = Action {
    var outString = "Number is "
    val conn = db.getConnection()
    
    try {
      val stmt = conn.createStatement
      val rs = stmt.executeQuery("SELECT name_col from test")
      
      while (rs.next()) {
        outString += rs.getString("name_col")
      }
    } finally {
      conn.close()
    }
    Ok(Json.toJson(outString))
  }
  


  def testAnorm = Action {
	  val parser: RowParser[TestDb] = Macro.namedParser[TestDb]
	  implicit val testFormat = Json.format[TestDb]
	  db.withConnection { implicit connection =>
  	  val result: List[TestDb] = SQL("SELECT * FROM test").as(parser.*)
  	  Ok(Json.toJson(result))
    }
	  
  }
}