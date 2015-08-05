package com.guavus.acume.rest.api
import java.io.OutputStream
import java.lang.annotation.Annotation
import java.lang.reflect.Type
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.MultivaluedMap
import javax.ws.rs.ext.MessageBodyWriter
import javax.ws.rs.ext.Provider
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.guavus.rubix.query.remote.flex.SearchCriterion
import com.sun.jersey.spi.resource.Singleton
import MessageBodyWriterJSON._
import com.guavus.rubix.query.remote.flex.SearchCriterion

object MessageBodyWriterJSON {

  private var gson: Gson = new GsonBuilder().registerTypeAdapter(classOf[SearchCriterion], new SearchCriterion.SearchCriterionJsonAdapter()).create()
}

@Provider
@Singleton
class MessageBodyWriterJSON extends MessageBodyWriter[Any] {

  def getSize(t: AnyRef, `type`: Class[_], genericType: Type, annotations: Array[Annotation], mediaType: MediaType): Long = {
    -1
  }

  def isWriteable(`type`: Class[_], genericType: Type, annotations: Array[Annotation], mediaType: MediaType): Boolean = {
    mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE)
  }

  override def writeTo(t: AnyRef, `type`: Class[_], genericType: Type, annotations: Array[Annotation], mediaType: MediaType, httpHeaders: MultivaluedMap[String, AnyRef], entityStream: OutputStream) {
    entityStream.write(gson.toJson(t).toString.getBytes)
  }

/*
Original Java:
package com.guavus.rubix.report.servlet;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.guavus.rubix.search.SearchCriterion;
import com.sun.jersey.spi.resource.Singleton;
 
@Provider
@Singleton
public class MessageBodyWriterJSON implements MessageBodyWriter<Object> {

	private static Gson gson  = new GsonBuilder().registerTypeAdapter(SearchCriterion.class, new SearchCriterion.SearchCriterionJsonAdapter()).create()  ;
	
	public long getSize(final Object t, final Class<?> type, final Type genericType, final Annotation[] annotations, final MediaType mediaType) {
		return -1;
	}

	public boolean isWriteable(final Class<?> type, final Type genericType, final Annotation[] annotations, final MediaType mediaType) {
		return mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE);
	}
 
	@Override
	public void writeTo(final Object t, final Class<?> type, final Type genericType, final Annotation[] annotations, final MediaType mediaType, final MultivaluedMap<String, Object> httpHeaders, final OutputStream entityStream) throws IOException, WebApplicationException {
		entityStream.write(gson.toJson(t).toString().getBytes());
	}
}
*/
}