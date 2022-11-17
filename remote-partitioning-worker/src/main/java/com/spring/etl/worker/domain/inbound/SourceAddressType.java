package com.spring.etl.worker.domain.inbound;

public class SourceAddressType {

	
	private Integer addrTypeId;
	private String lookup;
	private String meaning;
	
	public Integer getAddrTypeId() {
		return addrTypeId;
	}
	public void setAddrTypeId(Integer addrTypeId) {
		this.addrTypeId = addrTypeId;
	}
	public String getLookup() {
		return lookup;
	}
	public void setLookup(String lookup) {
		this.lookup = lookup;
	}
	public String getMeaning() {
		return meaning;
	}
	public void setMeaning(String meaning) {
		this.meaning = meaning;
	}
	
	
}

