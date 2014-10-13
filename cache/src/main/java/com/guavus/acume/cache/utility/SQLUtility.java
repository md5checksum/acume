package com.guavus.acume.cache.utility;

import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

import com.guavus.acume.cache.common.AcumeConstants;
import com.guavus.acume.cache.workflow.RequestType;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

public class SQLUtility {

	public static List<Tuple> list = new LinkedList<Tuple>();

	public static String requestType = AcumeConstants.Aggregate();
	

	public List<Tuple> getList(String qx) {
		
		try{
		CCJSqlParserManager sql = SQLParserFactory.getParserManager();
		Statement statement = sql.parse(new StringReader(qx));
		((Select)statement).getSelectBody().accept(new QuerySelectClauseVisitor(0l, 0l));
		return list;
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public String getRequestType(String qx) {
		
		try{
		CCJSqlParserManager sql = SQLParserFactory.getParserManager();
		Statement statement = sql.parse(new StringReader(qx));
		((Select)statement).getSelectBody().accept(new Visitor());
		return requestType;
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
		return requestType;
	}
	
	public static void main(String args[]) {
			
		SQLUtility util = new SQLUtility();
		List<Tuple> list = util.getList("select * from ((select * from (select * from (t full outer join b) where ts < 105 and ts >=10)) full outer join xt) as T where ts<10 and ts >=104");

		for (Tuple tx: list) {
			System.out.println(tx.getStartTime());
			System.out.println(tx.getEndTime());
			System.out.println(tx.getTableName());
		}
	}
}
