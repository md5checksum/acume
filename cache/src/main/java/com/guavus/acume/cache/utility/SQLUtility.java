package com.guavus.acume.cache.utility;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

import com.guavus.acume.cache.common.AcumeConstants;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author archit.thakur
 */
public class SQLUtility {

	public List<Tuple> getList(String qx) {
		
		List<Tuple> list = new ArrayList<Tuple>();
		try{
			CCJSqlParserManager sql = SQLParserFactory.getParserManager();
			Statement statement = sql.parse(new StringReader(qx));
			((Select)statement).getSelectBody().accept(new QuerySelectClauseVisitor(0l, 0l, null, null, list));
			return list;
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public String getRequestType(String qx) {

		RequestType requestType = new RequestType();
		requestType.setRequestType(AcumeConstants.Aggregate());
		try{
			CCJSqlParserManager sql = SQLParserFactory.getParserManager();
			Statement statement = sql.parse(new StringReader(qx));
			((Select)statement).getSelectBody().accept(new Visitor(requestType));
			return requestType.getRequestType();
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
		return requestType.getRequestType();
	}
	
	public static void main(String args[]) {
			
		SQLUtility util = new SQLUtility();
//		List<Tuple> list = util.getList("select * from ((select * from (select * from (t full outer join b) where ts < 105 and ts >=10 and binsource = mybinsource)) full outer join xt) as T where ts<10 and ts >=104 and binsource = ifjkdh");
//		System.out.println(util.getRequestType("select ts,x from ((select * from (select * from (t full outer join b) where ts < 105 and ts >=10)) full outer join xt) as T where ts<10 and ts >=104"));
//		for (Tuple tx: list) {
//			System.out.println(tx.getStartTime());
//			System.out.println(tx.getEndTime());
//			System.out.println(tx.getBinsource());
//			System.out.println(tx.getCubeName());
//		}

		
		List<Tuple> list = util.getList("select * from abc0 where x=1 AND y=2 AND z=3");
		for (Tuple tx: list) {
			System.out.println(tx.getStartTime());
			System.out.println(tx.getEndTime());
			System.out.println(tx.getBinsource());
			System.out.println(tx.getCubeName());
			System.out.println(tx.getSingleEntityKeyValueList());
		}
		
		list = util.getList("select * from abc00 where (x=1 AND y=2 AND z=3) OR (x=102 AND y=37 AND z=456)");
		for (Tuple tx: list) {
			System.out.println(tx.getStartTime());
			System.out.println(tx.getEndTime());
			System.out.println(tx.getBinsource());
			System.out.println(tx.getCubeName());
			System.out.println(tx.getSingleEntityKeyValueList());
		}
		
		list = util.getList("select * from abc000 where ((x=1 AND y=2 AND z=3) OR (x=102 AND y=37 AND z=456)) AND ts >= 1000 AND ts < 100000 AND binsource = 'default'");
		for (Tuple tx: list) {
			System.out.println(tx.getStartTime());
			System.out.println(tx.getEndTime());
			System.out.println(tx.getBinsource());
			System.out.println(tx.getCubeName());
			System.out.println(tx.getSingleEntityKeyValueList());
		}
		
		list = util.getList("select * from ((select * from (select * from (t full outer join b) where x=1 and y=2 and z=3 and ts < 105 and ts >=10 and binsource = 'mybinsource')) full outer join xt) as T where ((x=100 and y=200) or (x=105 and y=205 and z=53) or (x=1 and y=2 and z=3 and gh=4)) and ts<10 and ts >=104 and binsource = 'ifjkdh'");
		for (Tuple tx: list) {
			System.out.println(tx.getStartTime());
			System.out.println(tx.getEndTime());
			System.out.println(tx.getBinsource());
			System.out.println(tx.getCubeName());
			System.out.println(tx.getSingleEntityKeyValueList());
		}
	}
}
