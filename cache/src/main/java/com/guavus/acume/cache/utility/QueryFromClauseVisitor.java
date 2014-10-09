package com.guavus.acume.cache.utility;

import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class QueryFromClauseVisitor extends AbstractVisitor {
//
//	List<Tuple> list = new LinkedList<Tuple>();
//	boolean istimestamp = false;
//	
//	//majorly dependent on the implementation of QueryBuilder style.
//	//todo implement other methods in next build.
//	public QueryFromClauseVisitor(LinkedList<Tuple> list) {
//		this.list = list;
//	}
//
//	public void visit(Table tableName){
//		Tuple k = new Tuple();
//		k.setTableName(tableName.getName());
////		list.add(tableName.getName());
//	}
//
//	public void visit(SubSelect subSelect){
////		Pair<Long,Long> pair = new Pair<Long, Long>(0l, 0l);
//		PlainSelect plainSelect = (PlainSelect)subSelect.getSelectBody();
//		plainSelect.getFromItem().accept(this);
//		plainSelect.getWhere().accept(new QueryWhereClauseVisitor(pair));
//	}
//
//	public void visit(SubJoin subjoin){
//		FromItem leftItem = subjoin.getLeft();
//		FromItem rightItem = subjoin.getJoin().getRightItem();
//	}
}
