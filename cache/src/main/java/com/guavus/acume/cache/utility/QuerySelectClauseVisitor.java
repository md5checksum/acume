package com.guavus.acume.cache.utility;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;

/**
 * @author archit.thakur
 */
public class QuerySelectClauseVisitor implements SelectVisitor {

	long startTime = 0l;
	long endTime = 0L;
	String binsource = null;
	
	private List<Tuple> list = null;
	
	public QuerySelectClauseVisitor(long startTime, long endTime, String binsource, List<Tuple> list) {
		
		this.startTime = startTime;
		this.endTime = endTime;
		this.binsource = binsource;
		this.list = list;
	}
	
	@Override
	public void visit(PlainSelect plainSelect) {
		Tuple t = new Tuple();
		Expression expression = plainSelect.getWhere();
		if(expression != null) { 
			
			expression.accept(new QueryWhereClauseVisitor(t));
			expression.accept(new QueryBinSourceWhereClauseVisitor(t));
		}
		long xstartTime = t.getStartTime() == 0l?startTime: t.getStartTime();
		long xendTime = t.getEndTime() == 0l?endTime:t.getEndTime();
		
		String xbinsource = t.getBinsource() == null?binsource:t.getBinsource();
		plainSelect.getFromItem().accept(new QueryFromClauseVisitor(xstartTime, xendTime, xbinsource, list));
	}

	@Override
	public void visit(SetOperationList setOpList) {
		
		List<PlainSelect> linkedList = setOpList.getPlainSelects();
		for(PlainSelect iteratorPlainSelect: linkedList){
			iteratorPlainSelect.accept(new QuerySelectClauseVisitor(startTime, endTime, binsource, list));
		}
	}

	@Override
	public void visit(WithItem withItem) {

		withItem.getSelectBody().accept(new QuerySelectClauseVisitor(startTime, endTime, binsource, list));
	} 
}

