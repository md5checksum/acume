package com.guavus.acume.utility;

import java.io.StringReader;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserManager;

public class SQLParserFactory {
    
    private static CCJSqlParserManager pm = new CCJSqlParserManager();
    
    public static CCJSqlParserManager getParserManager() {
        return pm;
    }

    public static CCJSqlParser getBareParser(String query) {
        return new CCJSqlParser(new StringReader(query));
    }
}
