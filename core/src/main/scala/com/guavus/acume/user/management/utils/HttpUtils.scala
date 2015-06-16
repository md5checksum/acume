package com.guavus.acume.user.management.utils

import java.util.HashMap
import java.util.Map
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.google.common.collect.BiMap
import com.google.common.collect.HashBiMap
//remove if not needed
import scala.collection.JavaConversions._

class HttpUtils

object HttpUtils {

  private var SESSION: ThreadLocal[Map[String, Any]] = new ThreadLocal[Map[String, Any]]() {

    protected override def initialValue(): Map[String, Any] = new HashMap[String, Any]()


  }

  private val LOGININFO = "loginInfo"

  private val USERAGENT = "userAgent"

  private val CLIENTIP = "clientIp"

  private val SERVERIP = "serverIp"

  private var logger: Logger = LoggerFactory.getLogger(classOf[HttpUtils])

  def getUserAgent(): String = {
    (SESSION.get.get(USERAGENT)).asInstanceOf[String]
  }

  def getUserAgent(obfuscate: Boolean): String = {
    if (obfuscate) encrypt((SESSION.get.get(USERAGENT)).asInstanceOf[String]) else SESSION.get.get(USERAGENT).asInstanceOf[String]
  }

  def setUserAgent(userAgent: String) {
    logger.info("Setting USER-AGENT to " + userAgent)
    SESSION.get.put(USERAGENT, userAgent)
  }

  def getServerIp(): String = {
    (SESSION.get.get(SERVERIP)).asInstanceOf[String]
  }

  def getServerIp(obfuscate: Boolean): String = {
    if (obfuscate) encrypt((SESSION.get.get(SERVERIP)).asInstanceOf[String]) else SESSION.get.get(SERVERIP).asInstanceOf[String]
  }

  def setServerIp(serverIp: String) {
    logger.info("Setting SERVER-IP to " + serverIp)
    SESSION.get.put(SERVERIP, serverIp)
  }

  def getClientIp(): String = {
    (SESSION.get.get(CLIENTIP)).asInstanceOf[String]
  }

  def getClientIp(obfuscate: Boolean): String = {
    if (obfuscate) encrypt((SESSION.get.get(CLIENTIP)).asInstanceOf[String]) else SESSION.get.get(CLIENTIP).asInstanceOf[String]
  }

  def setClientIp(clientIp: String) {
    logger.info("Setting CLIENT-IP to " + clientIp)
    SESSION.get.put(CLIENTIP, clientIp)
  }

  def getLoginInfo(): String = {
    (SESSION.get.get(LOGININFO)).asInstanceOf[String]
  }

  def getLoginInfo(obfuscate: Boolean): String = {
    if (obfuscate) encrypt((SESSION.get.get(LOGININFO)).asInstanceOf[String]) else SESSION.get.get(LOGININFO).asInstanceOf[String]
  }

  def setLoginInfo(loginInfo: String) {
    logger.info("Setting  LOGIN-INFO to " + loginInfo)
    SESSION.get.put(LOGININFO, loginInfo)
  }

  def recycle() {
    SESSION.get.clear()
  }

  val hiddenString = "******"

  private val keyChars = "0123456789abcdefghijklmnopqrstuvwxyz._-"

  private val valChars = "3iesdvmhx45crag8kly6on-2zt.pf19_7ubwq0j"

  private var keymap: BiMap[Character, Character] = HashBiMap.create(keyChars.length)

  if (keyChars.length != valChars.length) {
    throw new IllegalStateException("Username encryption: key string length does not match the input string length")
  }

  for (i <- 0 until keyChars.length) {
    val key = keyChars.charAt(i)
    val `val` = valChars.charAt(i)
    if (keymap.containsKey(key)) {
      throw new IllegalStateException("Username encryption: duplicate characters in input string")
    } else if (keymap.containsValue(`val`)) {
      throw new IllegalStateException("Username encryption: duplicate characters in key string")
    }
    keymap.put(key, `val`)
  }

  private def translate(input: String, encrypt: Boolean): String = {
    if (input == null) {
      return null
    }
    var map: BiMap[Character, Character] = null
    map = if (encrypt) keymap else keymap.inverse()
    val ans = new StringBuilder()
    for (i <- 0 until input.length) {
      val key = input.charAt(i)
      if (!map.containsKey(key)) {
        map.put(key, key)
        ans.append(key)
      } else {
        ans.append(map.get(key))
      }
    }
    ans.toString
  }

  def encrypt(input: String): String = translate(input, true)

  def decrypt(input: String): String = translate(input, false)

  def main(args: Array[String]) {
    if (args.length == 2) {
      println(translate(args(0), java.lang.Boolean.valueOf(args(1))))
    }
  }

/*
Original Java:
package com.guavus.rubix.user.management.utils;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

public class HttpUtils {
		private static ThreadLocal<Map<String, Object>> SESSION = new ThreadLocal<Map<String, Object>>() {
			@Override
			protected Map<String, Object> initialValue() {
				return new HashMap<String, Object>();
			};
		};
	    private static final String LOGININFO = "loginInfo";
	    private static final String USERAGENT = "userAgent";
	    private static final String CLIENTIP = "clientIp";
	    private static final String SERVERIP = "serverIp";
	    
	    private static Logger logger = LoggerFactory.getLogger(HttpUtils.class);

	    public static String getUserAgent() {
	    	return (String)(SESSION.get().get(USERAGENT));
	    }
	    
	    public static String getUserAgent(boolean obfuscate) {
        	return obfuscate ? encrypt((String)(SESSION.get().get(USERAGENT))) : (String)SESSION.get().get(USERAGENT);
        }
	    
	    public static void setUserAgent(String userAgent) {
	    	logger.info("Setting USER-AGENT to " + userAgent);
	    	SESSION.get().put(USERAGENT, userAgent);
	    }
	    
	    public static String getServerIp() {
	    	return (String)(SESSION.get().get(SERVERIP));
	    }
	    
	    public static String getServerIp(boolean obfuscate) {
        	return obfuscate ? encrypt((String)(SESSION.get().get(SERVERIP))) : (String)SESSION.get().get(SERVERIP);
        }
	    
	    public static void setServerIp(String serverIp) {
	    	logger.info("Setting SERVER-IP to " + serverIp);
	    	SESSION.get().put(SERVERIP, serverIp);
	    }

	    public static String getClientIp() {
	    	return (String)(SESSION.get().get(CLIENTIP));
	    }
	    
	    public static String getClientIp(boolean obfuscate) {
        	return obfuscate ? encrypt((String)(SESSION.get().get(CLIENTIP))) : (String)SESSION.get().get(CLIENTIP);
        }
	    
	    public static void setClientIp(String clientIp) {
	    	logger.info("Setting CLIENT-IP to " + clientIp);	    		
	    	SESSION.get().put(CLIENTIP, clientIp);
	    }
	    
        public static String getLoginInfo() {
            return (String)(SESSION.get().get(LOGININFO));
        }

        public static String getLoginInfo(boolean obfuscate) {
        	return obfuscate ? encrypt((String)(SESSION.get().get(LOGININFO))) : (String)SESSION.get().get(LOGININFO);
        }

	    public static void setLoginInfo(String loginInfo) {
	    	logger.info("Setting  LOGIN-INFO to " + loginInfo);
	        SESSION.get().put(LOGININFO, loginInfo);
	    }

	    public static void recycle() {
	        SESSION.get().clear();
	    }

    public static final String hiddenString = "******";
    // The following must not contain duplicate characters
    // The following was obtained from the UI code
    private static final String keyChars = "0123456789abcdefghijklmnopqrstuvwxyz._-";
    // The following is a permutation of the above
    private static final String valChars = "3iesdvmhx45crag8kly6on-2zt.pf19_7ubwq0j";
    private static BiMap<Character, Character> keymap = HashBiMap.create(keyChars.length());

    static {
        if (keyChars.length() != valChars.length()) {
            throw new IllegalStateException(
                "Username encryption: key string length does not match the input string length");
        }

        for (int i = 0; i < keyChars.length(); i++) {
            Character key = keyChars.charAt(i);
            Character val = valChars.charAt(i);
            if (keymap.containsKey(key)) {
                throw new IllegalStateException(
                    "Username encryption: duplicate characters in input string");
            } else if (keymap.containsValue(val)) {
                throw new IllegalStateException(
                    "Username encryption: duplicate characters in key string");
            }
            keymap.put(key, val);
        }
    }

    private static String translate(String input, boolean encrypt) {
        if (input == null) {
            return null;
        }
        
        BiMap<Character, Character> map;
        if (encrypt) {
            map = keymap;
        } else {
            map = keymap.inverse();
        }
        StringBuilder ans = new StringBuilder();
        for (int i = 0; i < input.length(); i++) {
            Character key = input.charAt(i);
            if (!map.containsKey(key)) {
                // If we find a character that does not exist in the
                // allowedChars string then we map it to itself
                map.put(key, key);
                ans.append(key);
            } else {
                ans.append(map.get(key));
            }
        }
        return ans.toString();
    }

    public static String encrypt(String input) {
        return translate(input, true);
    }

    public static String decrypt(String input) {
        return translate(input, false);
    }

    public static void main(String[] args) {
        if (args.length == 2) {
            System.out.println(translate(args[0], Boolean.valueOf(args[1])));
        }
    }
}

*/
}