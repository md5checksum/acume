package com.guavus.acume.rest.api

import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.lang.annotation.Annotation
import java.lang.reflect.Type
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.MultivaluedMap
import javax.ws.rs.ext.MessageBodyReader
import javax.ws.rs.ext.Provider
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.guavus.rubix.search.SearchCriterion
import com.sun.jersey.spi.resource.Singleton
import MessageBodyReaderJSON._

object MessageBodyReaderJSON {

   var gson: Gson = new GsonBuilder().registerTypeAdapter(classOf[SearchCriterion], new SearchCriterion.SearchCriterionJsonAdapter()).create()
}

@Provider
@Singleton
/**
 * This class parses the json received for RestApis
 */
class MessageBodyReaderJSON extends MessageBodyReader[Any] {

  def getSize(t: AnyRef, `type`: Class[_], genericType: Type, annotations: Array[Annotation], mediaType: MediaType): Long = {
    -1
  }

  def isWriteable(`type`: Class[_], genericType: Type, annotations: Array[Annotation], mediaType: MediaType): Boolean = {
    mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE)
  }

  override def isReadable(`type`: Class[_], genericType: Type, annotations: Array[Annotation], mediaType: MediaType): Boolean = {
    return (mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE) || mediaType.isCompatible(MediaType.APPLICATION_FORM_URLENCODED_TYPE))
  }

  override def readFrom(`type`: Class[Any], genericType: Type, annotations: Array[Annotation], mediaType: MediaType, httpHeaders: MultivaluedMap[String, String], entityStream: InputStream): AnyRef = {
    val buffer = new ByteArrayOutputStream()
    var nRead: Int = 0
    val data = Array.ofDim[Byte](4096)
    while ({(nRead = entityStream.read(data, 0, data.length)); nRead != -1}) {
      buffer.write(data, 0, nRead)
    }
    buffer.flush()
    gson.fromJson(buffer.toString, `type`)
  }

/*
Original Java:
package com.guavus.rubix.report.servlet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.guavus.rubix.RubixWebService.BadRequestException;
import com.guavus.rubix.RubixWebService.HttpError;
import com.guavus.rubix.exceptions.RubixExceptionConstants;
import com.guavus.rubix.search.SearchCriterion;
import com.sun.jersey.spi.resource.Singleton;

@Provider
@Singleton
public class MessageBodyReaderJSON implements MessageBodyReader<Object> {

	private static Gson gson  = new GsonBuilder().registerTypeAdapter(SearchCriterion.class, new SearchCriterion.SearchCriterionJsonAdapter()).create();
	
	public long getSize(final Object t, final Class<?> type, final Type genericType, final Annotation[] annotations, final MediaType mediaType) {
		return -1;
	}

	public boolean isWriteable(final Class<?> type, final Type genericType, final Annotation[] annotations, final MediaType mediaType) {
		return mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE);
	}

	@Override
	public boolean isReadable(Class<?> type, Type genericType,
			Annotation[] annotations, MediaType mediaType) {
		return mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE);
	}
	@Override
	public Object readFrom(Class<Object> type, Type genericType,
			Annotation[] annotations, MediaType mediaType,
			MultivaluedMap<String, String> httpHeaders, InputStream entityStream)
	throws IOException, WebApplicationException {
		
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		int nRead;
		byte[] data = new byte[4096];
		while ((nRead = entityStream.read(data, 0, data.length)) != -1) {
		  buffer.write(data, 0, nRead);
		}
		buffer.flush();
		try{
			Object gsonObject = gson.fromJson(buffer.toString(), type);
			return gsonObject;
		}
		catch (Exception e) {
			throw new BadRequestException(HttpError.PRECONDITION_FAILED, 
					RubixExceptionConstants.MALFORMED_REQUEST.name(), e.getMessage(), ExceptionUtils.getStackTrace(e));
		}
		
	}
}
*/
}