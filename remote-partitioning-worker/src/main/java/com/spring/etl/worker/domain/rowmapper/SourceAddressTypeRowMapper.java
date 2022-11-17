package com.spring.etl.worker.domain.rowmapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import com.spring.etl.worker.domain.inbound.SourceAddressType;

public class SourceAddressTypeRowMapper implements RowMapper<SourceAddressType> {

	@Override
	public SourceAddressType mapRow(ResultSet rs, int rowNum) throws SQLException {
		SourceAddressType sourceAddr = new SourceAddressType();
		sourceAddr.setAddrTypeId(rs.getInt("addr_type_id"));
		sourceAddr.setLookup(rs.getString("lookup"));
		sourceAddr.setMeaning(rs.getString("meaning"));
		return sourceAddr;
	}
}