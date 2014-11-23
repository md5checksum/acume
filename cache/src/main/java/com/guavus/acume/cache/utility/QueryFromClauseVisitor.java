package com.guavus.acume.cache.utility;

import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * @author archit.thakur
 */
public class QueryFromClauseVisitor extends AbstractVisitor {

	long startTime = 0L;
	
	long endTime = 0L;
	List<Tuple> list = null;
	
	public QueryFromClauseVisitor(long startTime, long endTime, List<Tuple> list){
		this.startTime = startTime;
		this.endTime = endTime;
		this.list = list;
	}
	
	public void visit(Table tableName){
		
		Tuple t = new Tuple();
		t.setCubeName(tableName.getName());
		t.setStartTime(startTime);
		t.setEndTime(endTime);
		list.add(t);
	}

	public void visit(SubSelect subSelect){
		PlainSelect plainSelect = (PlainSelect)subSelect.getSelectBody();
		plainSelect.accept(new QuerySelectClauseVisitor(startTime, endTime, list));
	}

	public void visit(SubJoin subjoin){
		FromItem leftItem = subjoin.getLeft();
		leftItem.accept(new QueryFromClauseVisitor(startTime, endTime, list));
		FromItem rightItem = subjoin.getJoin().getRightItem();
		rightItem.accept(new QueryFromClauseVisitor(startTime, endTime, list));
	}
}
