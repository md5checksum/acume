package com.guavus.equinox.configuration;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public enum EquinoxConfiguration {
    DataDir("dataDir", "data"),
    ClusterName("cluster.name", ""),
    LastModified("lastModified",""+System.currentTimeMillis());

    public static class RPObservable extends Observable {
        public void setChanged() {
            super.setChanged();
        }
    }

    private static Logger logger = LoggerFactory.getLogger(EquinoxConfiguration.class);

    private static CompositeConfiguration properties;
    static {
        try {
            String hostName = java.net.InetAddress.getLocalHost().getHostName();
            ClusterName.value = hostName;
        } catch (UnknownHostException e) {
            logger.warn("Unable to get hostname for cluster name using tc", e);
            ClusterName.value = "tc";
        }
        try {
        	properties = new CompositeConfiguration();
        	properties.addConfiguration(new SystemConfiguration());
        	PropertiesConfiguration propConfig = new PropertiesConfiguration("equinox.configuration");
            properties.addConfiguration(propConfig);
            LastModified.value = "" + propConfig.getFile().lastModified();
            for (EquinoxConfiguration ec : EquinoxConfiguration.values()) {
                if (properties.containsKey(ec.key)) {
                    ec.value = properties.getString(ec.key);
                }
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            logger.info("Using Equinox Configuration {}",
                    Arrays.toString(EquinoxConfiguration.values()));
        }
        
    }

    private static Map<String, EquinoxConfiguration> key2Property;

    private String key, value;
    private RPObservable observables;

    private static Map<String, EquinoxConfiguration> getKey2Property() {
        if (key2Property == null) {
            key2Property = Maps.newHashMap();
        }
        return key2Property;
    }

    public static EquinoxConfiguration getByKey(String key) {
        return key2Property.get(key);
    }

    private EquinoxConfiguration(String key, String value) {
        this.key = key;
        this.value = value;
        getKey2Property().put(key, this);
        observables = new RPObservable();
    }

    public void addObserver(Observer o) {
        observables.addObserver(o);
    }

    public void deleteObserver(Observer o) {
        observables.deleteObserver(o);
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
        properties.setProperty(key, value);
        observables.setChanged();
        observables.notifyObservers();
    }

    public boolean getBooleanValue() {
        return Boolean.parseBoolean(getValue());
    }

    public byte getByteValue() {
        return Byte.parseByte(getValue());
    }

    public int getIntValue() {
        return Integer.parseInt(getValue());
    }

    public long getLongValue() {
        return Long.parseLong(getValue());
    }
    
    public String[] getStringArray(String delimiter) {
    	return getValue().split(delimiter);
    }

    public String toString() {
        return key + "=" + value;
    }

}
