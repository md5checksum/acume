package com.guavus.acume.configuration;

public class ConfigurationValidator {

	public static boolean validate(Class<?> clx) { 
		
		AcumeConfiguration[] configurationValues = AcumeConfiguration.values();
		boolean isOK = true;
		for(AcumeConfiguration config: configurationValues) { 
			
			switch(config){
			case CubeXml: 
				if(config.getValue() == "") { 
					
					isOK = false; 
				}
			}
		}
		return isOK;
	}
}
