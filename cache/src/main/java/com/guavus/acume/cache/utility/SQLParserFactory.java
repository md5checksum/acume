package com.guavus.acume.cache.utility;

import java.io.StringReader;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserManager;

/**
 * @author archit.thakur
 */
public class SQLParserFactory {
    
    private static CCJSqlParserManager pm = new CCJSqlParserManager();
    
    public static CCJSqlParserManager getParserManager() {
        return pm;
    }

    public static CCJSqlParser getBareParser(String query) {
        return new CCJSqlParser(new StringReader(query));
    }
}
