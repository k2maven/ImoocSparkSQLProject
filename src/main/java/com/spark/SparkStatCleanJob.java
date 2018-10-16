package com.spark;

import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.utils.IpUtils;
/**
 * 第二步：使用Spark完成我们的数据清洗操作，将数据从text转换为parquet数据
 */
public class SparkStatCleanJob {
	public static void main(String[] args) {

		SparkSession sc = SparkSession.builder().master("local").appName("SparkStatCleanJob").getOrCreate();
		// 把日志转换成javaRdd
		JavaRDD<String> rdd = sc.sparkContext().textFile("C://Users//tvu//Desktop/access.log", 1).toJavaRDD();
		// 对rdd进行数据清洗操作
		JavaRDD<Row> rowRdd = rdd.filter(x -> {
			String[] split = x.split("\\t");
			String url = split[1];
			return url.contains("http://www.imooc.com/") && !url.contains("html");
		}).map(x -> {
			String domain = "http://www.imooc.com/";

			String[] split = x.split("\t");
			String time = split[0];
			String url = split[1];
			Long traffic = Long.valueOf(split[2]);
			String ip = split[3];

			String cms = url.substring(url.indexOf(domain) + domain.length());
			String[] cmsTypeId = cms.split("/");

			String cmsType = "";
			Long cmsId = 0l;
			try {
				if (cmsTypeId.length == 2) {
					cmsType = cmsTypeId[0];
					cmsId = Long.valueOf(cmsTypeId[1]);
				}
			} catch (NumberFormatException e) {
			}
			String city = IpUtils.getCity(ip);
			String day = time.substring(0, 10).replaceAll("-", "");
			return RowFactory.create(url, cmsType, cmsId, traffic, ip, city, time, day);
		});

		ArrayList<StructField> fields = new ArrayList<StructField>();
		StructField field = null;
		field = DataTypes.createStructField("url", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("cmsType", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("cmsId", DataTypes.LongType, true);
		fields.add(field);
		field = DataTypes.createStructField("traffic", DataTypes.LongType, true);
		fields.add(field);
		field = DataTypes.createStructField("ip", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("city", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("time", DataTypes.StringType, true);
		fields.add(field);
		field = DataTypes.createStructField("day", DataTypes.StringType, true);
		fields.add(field);

		StructType schema = DataTypes.createStructType(fields);
		// RDD => DF
		Dataset<Row> accessDF = sc.createDataFrame(rowRdd, schema);
		// 分成 1 个文件,保存成 parquet 格式，按 day 分区
		accessDF.coalesce(1).write().format("parquet").partitionBy("day").mode(SaveMode.Overwrite)
				.save("C://Users//tvu//Desktop/access_parquet.log");
		sc.stop();
	}
}
