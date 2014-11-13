package com.guavus.acume.core.webservice

import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response
import com.sun.jersey.api.Responses
import scala.reflect.{BeanProperty, BooleanBeanProperty}

@SerialVersionUID(3741047728746673730L)
class BadRequestException(httpErrorCode: Int, acumeErrorCode: String, message : String, diagnostic: String) extends WebApplicationException(httpErrorCode) {
/*
Original Java:
package com.guavus.rubix.RubixWebService;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.sun.jersey.api.Responses;

public class BadRequestException extends WebApplicationException {
	
	
	private String message;
	|**
	 * 
	 *|
	private static final long serialVersionUID = 3741047728746673730L;

	public BadRequestException(int httpErrorCode, String rubixErrorCode, String message, String diagnostic) {
		super(Response.status(httpErrorCode).entity(new WebServiceError(rubixErrorCode, message, diagnostic)).type("application/json").build());
		this.message = message;
	}
	
	public String getMessage(){
		return message;
	}
}

*/
}