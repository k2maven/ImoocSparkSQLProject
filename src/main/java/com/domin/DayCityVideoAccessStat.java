package com.domin;

import java.io.Serializable;

public class DayCityVideoAccessStat implements Serializable {
	private static final long serialVersionUID = 1L;

	private String day;

	private Long cmsId;

	private String city;

	private Long times;

	private Integer timesRank;

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public Long getCmsId() {
		return cmsId;
	}

	public void setCmsId(Long cmsId) {
		this.cmsId = cmsId;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public Long getTimes() {
		return times;
	}

	public void setTimes(Long times) {
		this.times = times;
	}

	public Integer getTimesRank() {
		return timesRank;
	}

	public void setTimesRank(Integer timesRank) {
		this.timesRank = timesRank;
	}

	public DayCityVideoAccessStat(String day, Long cmsId, String city, Long times, Integer timesRank) {
		super();
		this.day = day;
		this.cmsId = cmsId;
		this.city = city;
		this.times = times;
		this.timesRank = timesRank;
	}

	@Override
	public String toString() {
		return "DayCityVideoAccessStat [day=" + day + ", cmsId=" + cmsId + ", city=" + city + ", times=" + times
				+ ", timesRank=" + timesRank + "]";
	}
}
