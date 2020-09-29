package Assignment
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Assignment {
  case class stschema(dt:String,time:String,device_name:String,house_number:String,user_id:String,country_code:String,program_title:String,season:String,season_episode:String,genre:String,product_type:String)
  case class whschema(dt:String,house_number:String,title:String,product_category:String,broadcast_right_region:String,broadcast_right_vod_type:String,broadcast_right_start_date:String,broadcast_right_end_date:String)
  def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					val spark =SparkSession.builder().master("local[*]").getOrCreate()
					spark.sparkContext.setLogLevel("ERROR")
					import spark.sqlContext.implicits._
					
			println("=====================================================================================================")		
			println("Reading the two datasets and converted into Dataframes ")
			val stream_data =sc.textFile("file:///C://Users//HP//Downloads//Archive//started_streams.csv")
			val st_schemadata = stream_data.map(x=>x.split(";")).map(x=>stschema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10))).toDF()
			
			val whdata =sc.textFile("file:///C://Users//HP//Downloads//Archive//whatson.csv")
			val wh_schemadata = whdata.map(x=>x.split(",")).map(x=>whschema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7))).toDF()
			
			println("*******************Created spark sql tables**********************************************************************************")
			st_schemadata.createOrReplaceTempView("streams_data_tab")
			wh_schemadata.createOrReplaceTempView("whatson_data_tab")
			
			println("=====================================================================================================")
			println("Sales and rentals broadcast rights:This should be for Product Types: TVOD and EST. Matching on most recent date for whatson data and joining based on the house_number and country.")
			println("=====================================================================================================")
			val join_data =spark.sql("select a.dt,a.time,a.device_name,a.house_number,a.user_id,a.country_code,a.program_title,a.season,a.season_episode,a.genre,a.product_type,b.broadcast_right_start_date,b.broadcast_right_end_date,broadcast_right_vod_type from streams_data_tab a inner join whatson_data_tab b on a.dt = b.dt and a.house_number = b.house_number where b.broadcast_right_vod_type in ('TVOD','EST')") 
			join_data.show(10)
			println("*****************************************************************************************************")
			
			
			println("=====================================================================================================")
			println("Product and user count:a product is getting and how many unique users are watching the content, in what device, country and what product_type")
			println("=====================================================================================================")
			val product_count =spark.sql("select count(user_id) number_of_users_watching, program_title,device_name,country_code,product_type,count(genre) content_count from streams_data_tab group by program_title,device_name,country_code,product_type,genre") 
			product_count.show(10)
			println("*****************************************************************************************************")
			
			println("=====================================================================================================")
			println("Genre and time of day:list with the most popular Genre and what hours people watch?")
			println("=====================================================================================================")
			val genre_time =spark.sql("select count(time) watched_hours,genre,count(user_id) number_of_users_watching from streams_data_tab group by genre order by count(user_id) desc")
			genre_time.show(10)
                
  }
}
			
			
