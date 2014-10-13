package com.guavus.acume.cache.utility;

import com.guavus.acume.cache.common.AcumeConstants;
import com.guavus.acume.cache.workflow.RequestType;

import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

public class ACSelectItemVisitor implements SelectItemVisitor {

	@Override
	public void visit(AllColumns allColumns) {
		if(allColumns.toString().contains("ts"))
			SQLUtility.requestType = AcumeConstants.Timeseries();
	}

	@Override
	public void visit(AllTableColumns allTableColumns) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SelectExpressionItem selectExpressionItem) {
		// TODO Auto-generated method stub
		
	}

}
