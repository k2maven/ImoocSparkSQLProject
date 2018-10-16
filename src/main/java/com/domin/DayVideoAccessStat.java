package com.domin;

import java.io.Serializable;

public class DayVideoAccessStat implements Serializable {
	private static final long serialVersionUID = 1L;
	private String day;
	private Long cmsId;
	private Long times;

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

	public Long getTimes() {
		return times;
	}

	public void setTimes(Long times) {
		this.times = times;
	}

	public DayVideoAccessStat(String day, Long cmsId, Long times) {
		super();
		this.day = day;
		this.cmsId = cmsId;
		this.times = times;
	}

	@Override
	public String toString() {
		return "DayVideoAccessStat [day=" + day + ", cmsId=" + cmsId + ", times=" + times + "]";
	}
}
