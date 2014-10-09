package com.guavus.acume.cache.utility;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class QuerySelectClauseVisitor extends AbstractVisitor { 
	
//	private List tables;
//
////	public List getTableList(Select select) {
////		tables = new ArrayList();
////		select.getSelectBody().accept(this);
////		return tables;
////	}
////	
////	public long getStartTime() { 
////		
////		return 0;
////	}
////	
////	public long getEndTime() { 
////		
////		return 0;
////	}
//
//	public void visit(PlainSelect plainSelect) {
//		
//		List<Tuple> list = new LinkedList<Tuple>();
//		Expression where = plainSelect.getWhere();
//		where.accept(new QueryWhereClauseVisitor(list));
//		long xStartTime = list.get(0).getStartTime();
//		long xEndTime = list.get(0).getEndTime();
//		
//		FromItem fromItem = plainSelect.getFromItem();
//		fromItem.accept(new QueryFromClauseVisitor());
//	}
}

