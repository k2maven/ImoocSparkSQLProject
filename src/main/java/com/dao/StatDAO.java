package com.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.domin.DayCityVideoAccessStat;
import com.domin.DayVideoAccessStat;
import com.domin.DayVideoTrafficsStat;
import com.utils.MySQLUtils;

public class StatDAO {
	/**
	 * 批量保存DayVideoAccessStat到数据库
	 * 
	 * @throws SQLException
	 */
	public static void insertDayVideoAccessTopN(List<DayVideoAccessStat> list) throws SQLException {
		Connection connection = null;
		PreparedStatement pstmt = null;
		try {
			connection = MySQLUtils.getConnection();

			// 设置手动提交sql
			connection.setAutoCommit(false);
			String sql = "insert into day_video_access_topn_stat(day,cms_id,times) values (?,?,?)";
			pstmt = connection.prepareStatement(sql);

			for (DayVideoAccessStat dayVideoAccessStat : list) {
				pstmt.setString(1, dayVideoAccessStat.getDay());
				pstmt.setLong(2, dayVideoAccessStat.getCmsId());
				pstmt.setLong(3, dayVideoAccessStat.getTimes());

				pstmt.addBatch();
			}
			pstmt.executeBatch();// 执行批量处理
			connection.commit();// 手工提交

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			MySQLUtils.release(connection, pstmt);
		}
	}

	/**
	 * 批量保存DayCityVideoAccessStat到数据库
	 */
	public static void insertDayCityVideoAccessTopN(List<DayCityVideoAccessStat> list) throws SQLException {
		Connection connection = null;
		PreparedStatement pstmt = null;
		try {
			connection = MySQLUtils.getConnection();

			// 设置手动提交sql
			connection.setAutoCommit(false);
			String sql = "insert into day_video_city_access_topn_stat(day,cms_id,city,times,times_rank) values (?,?,?,?,?)";
			pstmt = connection.prepareStatement(sql);

			for (DayCityVideoAccessStat dayVideoAccessStat : list) {
				pstmt.setString(1, dayVideoAccessStat.getDay());
				pstmt.setLong(2, dayVideoAccessStat.getCmsId());
				pstmt.setString(3, dayVideoAccessStat.getCity());
				pstmt.setLong(4, dayVideoAccessStat.getTimes());
				pstmt.setInt(5, dayVideoAccessStat.getTimesRank());

				pstmt.addBatch();
			}
			pstmt.executeBatch();// 执行批量处理
			connection.commit();// 手工提交

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			MySQLUtils.release(connection, pstmt);
		}
	}

	public static void insertDayVideoTrafficsAccessTopN(List<DayVideoTrafficsStat> list) throws SQLException {
		Connection connection = null;
		PreparedStatement pstmt = null;
		try {
			connection = MySQLUtils.getConnection();

			// 设置手动提交sql
			connection.setAutoCommit(false);
			String sql = "insert into day_video_traffics_topn_stat(day,cms_id,traffics) values (?,?,?)";
			pstmt = connection.prepareStatement(sql);

			for (DayVideoTrafficsStat dayVideoAccessStat : list) {
				pstmt.setString(1, dayVideoAccessStat.getDay());
				pstmt.setLong(2, dayVideoAccessStat.getCmsId());
				pstmt.setLong(3, dayVideoAccessStat.getTraffics());
				pstmt.addBatch();
			}
			pstmt.executeBatch();// 执行批量处理
			connection.commit();// 手工提交

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			MySQLUtils.release(connection, pstmt);
		}
	}
	
	/**
	    * 删除指定日期的数据
	 * @throws SQLException 
	    */
	public static void deleteData(String day) throws SQLException {
		List<String> tables = Arrays.asList("day_video_access_topn_stat",
			      "day_video_city_access_topn_stat",
			      "day_video_traffics_topn_stat");
		
		Connection connection = null;
	    PreparedStatement pstmt = null;
	    
	    try {
			connection = MySQLUtils.getConnection();
			
			for (String table  : tables) {
				String deleteSQL = "delete from "+table+" where day = ?";
				pstmt = connection.prepareStatement(deleteSQL);
				        pstmt.setString(1, day);
				        pstmt.executeUpdate();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
		      MySQLUtils.release(connection, pstmt);
	    }
	}
}
