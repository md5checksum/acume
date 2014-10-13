package com.guavus.acume.cache.utility;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class QueryFromClauseVisitor extends AbstractVisitor {

	long startTime = 0L;
	long endTime = 0L;
	
	public QueryFromClauseVisitor(long startTime, long endTime){
		this.startTime = startTime;
		this.endTime = endTime;
	}
	
	public void visit(Table tableName){
		
		Tuple t = new Tuple();
		t.setTableName(tableName.getName());
		t.setStartTime(startTime);
		t.setEndTime(endTime);
		SQLUtility.list.add(t);
	}

	public void visit(SubSelect subSelect){
		PlainSelect plainSelect = (PlainSelect)subSelect.getSelectBody();
		plainSelect.accept(new QuerySelectClauseVisitor(startTime, endTime));
	}

	public void visit(SubJoin subjoin){
		FromItem leftItem = subjoin.getLeft();
		leftItem.accept(new QueryFromClauseVisitor(startTime, endTime));
		FromItem rightItem = subjoin.getJoin().getRightItem();
		rightItem.accept(new QueryFromClauseVisitor(startTime, endTime));
	}
}
