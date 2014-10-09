package com.guavus.acume.cache.utility;

import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Column;

public class QueryWhereClauseVisitor extends AbstractVisitor {
    
	Pair<Long,Long> pair = new Pair<Long,Long>(0L, 0L);
	boolean istimestamp = false;
	
	public Pair<Long,Long> getPair(){
		return pair;
	}
	
	public QueryWhereClauseVisitor() {
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
		if(istimestamp)
		{
			pair.setV(Long.parseLong(lessThan.getRightExpression().toString()));
			istimestamp = false;
		}
	}

	public void visit(GreaterThanEquals greaterThanEquals) {
		greaterThanEquals.getLeftExpression().accept(this);
		if(istimestamp)
		{
			pair.setU(Long.parseLong(greaterThanEquals.getRightExpression().toString()));
			istimestamp = false;
		}
	}
	
	public void visit(Column tableColumn) {
		if(tableColumn.getColumnName().equalsIgnoreCase("ts"))
			istimestamp = true;
	}
}
