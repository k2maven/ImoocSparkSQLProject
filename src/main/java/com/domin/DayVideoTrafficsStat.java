package com.domin;

import java.io.Serializable;

public class DayVideoTrafficsStat implements Serializable {
	private static final long serialVersionUID = 1L;

	private String day;

	private Long cmsId;

	private Long traffics;

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

	public Long getTraffics() {
		return traffics;
	}

	public void setTraffics(Long traffics) {
		this.traffics = traffics;
	}

	@Override
	public String toString() {
		return "DayVideoTrafficsStat [day=" + day + ", cmsId=" + cmsId + ", traffics=" + traffics + "]";
	}

	public DayVideoTrafficsStat(String day, Long cmsId, Long traffics) {
		super();
		this.day = day;
		this.cmsId = cmsId;
		this.traffics = traffics;
	}
}
