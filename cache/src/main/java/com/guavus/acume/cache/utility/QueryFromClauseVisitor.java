package com.guavus.acume.cache.utility;

import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class QueryFromClauseVisitor extends AbstractVisitor {

	List<String> list = new LinkedList<String>();
	boolean istimestamp = false;
	
	//majorly dependent on the implementation of QueryBuilder style.
	//todo implement other methods in next build.
	public QueryFromClauseVisitor() {
	}

	public void visit(Table tableName){
		list.add(tableName.getName());
	}

	public void visit(SubSelect subSelect){
		Pair<Long,Long> pair = new Pair<Long, Long>(0l, 0l);
		PlainSelect plainSelect = (PlainSelect)subSelect.getSelectBody();
		plainSelect.getFromItem().accept(this);
		plainSelect.getWhere().accept(new QueryWhereClauseVisitor(pair));
	}

	public void visit(SubJoin subjoin){
		subjoin.se
	}
}
