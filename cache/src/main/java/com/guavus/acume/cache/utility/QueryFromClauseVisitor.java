package com.guavus.acume.cache.utility;

import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class QueryFromClauseVisitor extends AbstractVisitor {

	List<Tuple> list_tl = new LinkedList<Tuple>();//table list.
	List<Tuple> list_ti = new LinkedList<Tuple>();//timestamp list.
	boolean istimestamp = false;
	
	//majorly dependent on the implementation of QueryBuilder style.
	//todo implement other methods in next build.
	public QueryFromClauseVisitor(LinkedList<Tuple> list_ti, LinkedList<Tuple> list_tl) {
		this.list_tl = list_tl;this.list_ti = list_ti;
	}

	public void visit(Table tableName){
		Tuple k = new Tuple();
		k.setTableName(tableName.getName());
		k.setStartTime(0l);
		k.setEndTime(0l);
		list_tl.add(k);
	}

	public void visit(SubSelect subSelect){
		Tuple t = new Tuple();
		PlainSelect plainSelect = (PlainSelect)subSelect.getSelectBody(); 
		plainSelect.getWhere().accept(new QueryWhereClauseVisitor(t));
		t.setTableName("");
		list_ti.add(t);
		plainSelect.getFromItem().accept(this);
//		plainSelect.getWhere().accept(new QueryWhereClauseVisitor(pair));
	}

	public void visit(SubJoin subjoin){
		FromItem leftItem = subjoin.getLeft();
		FromItem rightItem = subjoin.getJoin().getRightItem();
	}
}
