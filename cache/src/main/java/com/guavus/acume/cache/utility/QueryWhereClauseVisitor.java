package com.guavus.acume.cache.utility;

import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class QueryWhereClauseVisitor extends AbstractVisitor {
	
	Tuple t = null;
	boolean istimestamp = false;
	
	public QueryWhereClauseVisitor(Tuple t){
		this.t = t;
	}
	
	public void visit(AndExpression andExpression) {
    	andExpression.getRightExpression().accept(this);
		andExpression.getLeftExpression().accept(this);
	}

	public void visit(OrExpression orExpression) {
		orExpression.getRightExpression().accept(this);
		orExpression.getLeftExpression().accept(this);
	}
	
	public void visit(EqualsTo equalsTo) {
		equalsTo.getLeftExpression().accept(this);
	}
	
	public void visit(MinorThan lessThan) {
		lessThan.getLeftExpression().accept(this);
		if(istimestamp){
			t.setEndTime(Long.parseLong(lessThan.getRightExpression().toString()));
			istimestamp = false;
		}
	}

	public void visit(GreaterThanEquals greaterThanEquals) {
		greaterThanEquals.getLeftExpression().accept(this);
		if(istimestamp){
			t.setStartTime(Long.parseLong(greaterThanEquals.getRightExpression().toString()));
			istimestamp = false;
		}
	}
	
	public void visit(Column tableColumn) {
		if(tableColumn.getColumnName().equalsIgnoreCase("ts"))
			istimestamp = true;
	}
}
