package com.guavus.acume.cache.utility;

import java.util.List;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;

public class Visitor implements SelectVisitor {

	RequestType type = null;
	
	public Visitor(RequestType type) { 
		
		this.type = type;
	}
	@Override
	public void visit(PlainSelect plainSelect) {
		SelectItemVisitor xv = new ACSelectItemVisitor(type);
		List<SelectItem> itemList = plainSelect.getSelectItems();
		for(SelectItem xi: itemList) {
			xi.accept(xv);
		}
	}

	@Override
	public void visit(SetOperationList setOpList) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WithItem withItem) {
		// TODO Auto-generated method stub
		
	}

}
