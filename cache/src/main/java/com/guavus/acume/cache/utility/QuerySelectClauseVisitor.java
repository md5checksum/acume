package com.guavus.acume.cache.utility;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
	LinkedList<HashMap<String, Object>> linkedlist = null;
	
	private List<Tuple> list = null;
	
	public QuerySelectClauseVisitor(long startTime, long endTime, String binsource, LinkedList<HashMap<String, Object>> linkedlist, List<Tuple> list) {
		
		this.startTime = startTime;
		this.endTime = endTime;
		this.binsource = binsource;
		this.linkedlist = linkedlist;
		this.list = list;
	}
	
	private LinkedList<HashMap<String, Object>> merge(LinkedList<HashMap<String, Object>> list, LinkedList<HashMap<String, Object>> list0) {

		if(list == null && list0 == null)
			return null;
		if(list0 == null || list0.isEmpty())
			return list;
		else if(list == null || list.isEmpty())
			return list0;
		LinkedList<HashMap<String, Object>> newlist = new LinkedList<HashMap<String, Object>>();
		newlist.addAll(list0);

		for (HashMap<String, Object> singleEntityMap : newlist) {
			for (HashMap<String, Object> innerEntityMap : list) {
				boolean flag = false;
				for (Map.Entry<String, Object> singleEntry : innerEntityMap.entrySet()) {

					String key = singleEntry.getKey();
					if (singleEntityMap.containsKey(key) && !singleEntry.getValue().toString().equals(singleEntityMap.get(key).toString())) {
						flag = true;
						break;
					}
				}
				if (!flag) {
					for (Map.Entry<String, Object> singleEntry : innerEntityMap.entrySet()) {
						String key = singleEntry.getKey();
						if(!singleEntityMap.containsKey(key)) {
							Object value = singleEntry.getValue();
							singleEntityMap.put(key, value);
						}
					}
				}
			}
		}

		return newlist;
	}
	
	@Override
	public void visit(PlainSelect plainSelect) {
		Tuple t = new Tuple();
		Expression expression = plainSelect.getWhere();
		if(expression != null) { 
			
			expression.accept(new QueryWhereClauseVisitor(t));
			expression.accept(new QueryBinSourceWhereClauseVisitor(t));
			expression.accept(new QueryKeySingleEntityWhereClauseVisitor(t));
		}
		long xstartTime = t.getStartTime() == 0l?startTime: t.getStartTime();
		long xendTime = t.getEndTime() == 0l?endTime:t.getEndTime();
		
		String xbinsource = t.getBinsource() == null?binsource:t.getBinsource();
		LinkedList<HashMap<String, Object>> locallinkedlist = t.getSingleEntityKeyValueList();
		LinkedList<HashMap<String, Object>> xlinkedlist = merge(linkedlist, locallinkedlist);
		plainSelect.getFromItem().accept(new QueryFromClauseVisitor(xstartTime, xendTime, xbinsource, xlinkedlist, list));
	}

	@Override
	public void visit(SetOperationList setOpList) {
		
		List<PlainSelect> linkedList = setOpList.getPlainSelects();
		for(PlainSelect iteratorPlainSelect: linkedList){
			iteratorPlainSelect.accept(new QuerySelectClauseVisitor(startTime, endTime, binsource, linkedlist, list));
		}
	}

	@Override
	public void visit(WithItem withItem) {

		withItem.getSelectBody().accept(new QuerySelectClauseVisitor(startTime, endTime, binsource, linkedlist, list));
	} 
}

