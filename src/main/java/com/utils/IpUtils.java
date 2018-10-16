package com.utils;

import com.ggstar.util.ip.IpHelper;

/**
 * 借助github开源项目
 */
public class IpUtils {
	public static String getCity(String ip) {
		return IpHelper.findRegionByIp(ip);
	}
	
	public static void main(String[] args) {
		System.out.println(getCity("223.73.62.20"));
	}
}
