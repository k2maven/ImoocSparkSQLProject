package com.spark;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import com.utils.DateUtil;
/**
 * 第一步清洗：抽取出我们所需要的指定列的数据
 */
public class SparkStatFormatJob {
	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("SparkStatFormatJob").master("local").getOrCreate();
		// RDD
		JavaRDD<String> rdd = spark.sparkContext().textFile("C://Users//tvu//Desktop/10000_access.log", 1).toJavaRDD();

		rdd.flatMap(x -> {
			// 按字段切分
			String[] split = x.split(" ");
			String ip = split[0];
			/**
			 * 原始日志的第三个和第四个字段拼接起来就是完整的访问时间： [10/Nov/2016:00:01:02 +0800] ==> yyyy-MM-dd
			 * HH:mm:ss
			 */
			String time = split[3] + " " + split[4];
			String url = split[11].replaceAll("\"", "");
			String traffic = split[9];
			time = DateUtil.parse(time);
			return Arrays.asList(time + "\t" + url + "\t" + traffic + "\t" + ip).iterator();
		}).saveAsTextFile("C://Users//tvu//Desktop/access.log");

		spark.stop();
	}
}
