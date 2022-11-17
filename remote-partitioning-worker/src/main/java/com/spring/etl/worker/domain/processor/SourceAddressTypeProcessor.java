package com.spring.etl.worker.domain.processor;

import org.springframework.batch.item.ItemProcessor;

import com.spring.etl.worker.domain.inbound.SourceAddressType;
import com.spring.etl.worker.domain.outbound.TargetAddressType;

public class SourceAddressTypeProcessor implements ItemProcessor<SourceAddressType, TargetAddressType> {

	@Override
	public TargetAddressType process(SourceAddressType item) throws Exception {
		TargetAddressType t = new TargetAddressType();
		t.setAddrTypeId(item.getAddrTypeId());
		t.setLookup(item.getLookup());
		t.setMeaning(item.getMeaning());
		return t;
	}

}
