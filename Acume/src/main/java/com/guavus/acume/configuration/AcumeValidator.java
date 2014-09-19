package com.guavus.acume.configuration;

public class AcumeValidator {

	public static boolean validate(Class<?> clx) { 
		
		AcumeConfiguration[] configurationValues = AcumeConfiguration.values();
		boolean isOK = true;
		for(AcumeConfiguration config: configurationValues) { 
			
			switch(config){
			case Main_Jar: 
				if(config.getValue() == "") { 
					
					isOK = false; 
				}
			}
		}
		return isOK;
	}
}
