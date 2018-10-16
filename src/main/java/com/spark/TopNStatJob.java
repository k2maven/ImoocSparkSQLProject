package com.spark;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.dao.StatDAO;
import com.domin.DayCityVideoAccessStat;
import com.domin.DayVideoAccessStat;
import com.domin.DayVideoTrafficsStat;

/**
 * 统计topN
 * 
 * @author tvu
 *
 */
public class TopNStatJob {
	public static void main(String[] args) {
		SparkSession sc = SparkSession.builder().master("local[2]").appName("TopNStatJob")
				.config("spark.sql.sources.partitionColumnTypeInference.enabled", "false").getOrCreate();
		Dataset<Row> accessDF = sc.read().format("parquet").load("C://Users//tvu//Desktop/access_parquet.log");
		// 复用已有数据
		accessDF.cache();
		// 删除原来的数据
		try {
			StatDAO.deleteData("20161111");
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
		// 最受欢迎的TopN课程访问次数
		videoAccessTopNStat(sc, accessDF, args);

		// 按照地市进行统计TopN课程
		cityAccessTopNStat(sc, accessDF, args);

		// 按照流量进行统计
		videoTrafficsTopNStat(sc, accessDF, args);
		accessDF.unpersist(true);
		sc.stop();

	}

	/**
	 * 需求三：按照流量进行统计
	 */
	private static void videoTrafficsTopNStat(SparkSession sc, Dataset<Row> accessDF, String[] args) {
		Map<String, String> map = new HashMap<String, String>();
		map.put("traffic", "sum");
		Dataset<Row> trafficsDF = accessDF.groupBy("day", "cmsId").agg(map);
		Dataset<Row> as = trafficsDF.orderBy(trafficsDF.col("sum(traffic)").desc());
		// as.show(10,false);
		insertTrafficsAccessTopN(as);
	}

	/**
	 * 需求二：按地市统计最受欢迎TOP N
	 */
	private static void cityAccessTopNStat(SparkSession sc, Dataset<Row> accessDF, String[] args) {

		// insertCityAccessTopN(sc.sql(sql));
		Map<String, String> map = new HashMap<String, String>();
		map.put("cmsId", "count");
		Dataset<Row> aggDF = accessDF.groupBy("day", "city", "cmsId").agg(map);
		JavaRDD<Row> javaRDD = aggDF.toJavaRDD();

		ArrayList<StructField> fields = new ArrayList<StructField>();
		StructField field = null;
		field = DataTypes.createStructField("day", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("city", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("cmsId", DataTypes.LongType, true);
		fields.add(field);
		field = DataTypes.createStructField("times", DataTypes.LongType, true);
		fields.add(field);

		StructType schema = DataTypes.createStructType(fields);

		Dataset<Row> timesDF = sc.createDataFrame(javaRDD, schema);

		timesDF.createOrReplaceTempView("access_logs");
		String sql = "select day,city,cmsId ,times ,rank from "
				+ "( select day,city,cmsId ,times ,row_number() over (PARTITION BY city ORDER BY times DESC) as rank from access_logs) tmp_access where rank <= 3";
		insertCityAccessTopN(sc.sql(sql));
	}

	/**
	 * 需求一：最受欢迎的Top N课程访问次数
	 */
	private static void videoAccessTopNStat(SparkSession sc, Dataset<Row> accessDF, String[] args) {

		accessDF.createOrReplaceTempView("access_logs");

		String sql = "select day,cmsId,count(1) as times from access_logs where day ='" + args[0] + "' and cmsType = '"
				+ args[1] + "' group by day , cmsId  order by times desc";
		// sc.sql(sql).show(10,false);
		insertAccessTopN(sc.sql(sql));

	}

	// 插入数据库的方法
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void insertAccessTopN(Dataset<Row> df) {
		df.foreach(new ForeachFunction() {

			private static final long serialVersionUID = 1L;
			List<DayVideoAccessStat> list = new ArrayList<DayVideoAccessStat>();

			public void call(Object t) throws Exception {
				Row row = (Row) t;
				String day = row.getString(0);
				long cmsId = row.getLong(1);
				long times = row.getLong(2);
				DayVideoAccessStat ds = new DayVideoAccessStat(day, cmsId, times);
				list.add(ds);
				StatDAO.insertDayVideoAccessTopN(list);
			}
		});
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void insertCityAccessTopN(Dataset<Row> df) {

		df.foreach(new ForeachFunction() {

			private static final long serialVersionUID = 1L;
			List<DayCityVideoAccessStat> list = new ArrayList<DayCityVideoAccessStat>();

			public void call(Object t) throws Exception {
				Row row = (Row) t;
				String day = row.getString(0);
				String city = row.getString(1);
				long cmsId = row.getLong(2);
				long times = row.getLong(3);
				int rank = row.getInt(4);
				DayCityVideoAccessStat dcs = new DayCityVideoAccessStat(day, cmsId, city, times, rank);
				list.add(dcs);
				StatDAO.insertDayCityVideoAccessTopN(list);
			}
		});
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void insertTrafficsAccessTopN(Dataset<Row> df) {

		df.foreach(new ForeachFunction() {

			private static final long serialVersionUID = 1L;
			List<DayVideoTrafficsStat> list = new ArrayList<DayVideoTrafficsStat>();

			public void call(Object t) throws Exception {
				Row row = (Row) t;
				String day = row.getString(0);
				long cmsId = row.getLong(1);
				long traffics = row.getLong(2);
				DayVideoTrafficsStat dcs = new DayVideoTrafficsStat(day, cmsId, traffics);
				list.add(dcs);
				StatDAO.insertDayVideoTrafficsAccessTopN(list);
			}
		});
	}
}
