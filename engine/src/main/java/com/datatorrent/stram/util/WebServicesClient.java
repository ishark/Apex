/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.stram.util;

import java.io.IOException;
import java.security.Principal;
import java.util.concurrent.Future;

import com.sun.jersey.api.client.AsyncWebResource;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.async.ITypeListener;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.apache4.ApacheHttpClient4Handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/**
 * <p>WebServicesClient class.</p>
 *
 * @since 0.3.2
 */
public class WebServicesClient
{

  private static final Logger LOG = LoggerFactory.getLogger(WebServicesClient.class);

  private static final PoolingHttpClientConnectionManager connectionManager;
  private static final CredentialsProvider credentialsProvider;
  private static final int DEFAULT_CONNECT_TIMEOUT = 10000;
  private static final int DEFAULT_READ_TIMEOUT = 10000;

  private final Client client;

  static {
    connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(200);
    connectionManager.setDefaultMaxPerRoute(5);
    credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new Credentials() {

      @Override
      public Principal getUserPrincipal()
      {
        return null;
      }

      @Override
      public String getPassword()
      {
        return null;
      }

    });
  }

  public WebServicesClient()
  {
    this(new DefaultClientConfig());
    client.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true);
    client.getProperties().put(ClientConfig.PROPERTY_CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
    client.getProperties().put(ClientConfig.PROPERTY_READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
  }

  public WebServicesClient(ClientConfig config)
  {
    if (UserGroupInformation.isSecurityEnabled()) {
      HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
      httpClientBuilder.setConnectionManager(connectionManager);
      httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
      Lookup<AuthSchemeProvider> authProviders = RegistryBuilder.<AuthSchemeProvider>create()
              .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true))
              .build();
      httpClientBuilder.setDefaultAuthSchemeRegistry(authProviders);
      ApacheHttpClient4Handler httpClientHandler = new ApacheHttpClient4Handler(httpClientBuilder.build(), new BasicCookieStore(), false);
      client = new Client(httpClientHandler, config);
    } else {
      client = Client.create(config);
    }
  }

  public WebServicesClient(Client client) {
    this.client = client;
  }

  public Client getClient() {
    return client;
  }

  public <T> T process(String url, Class<T> clazz, WebServicesHandler<T> handler) throws IOException {
    WebResource wr = client.resource(url);
    return process(wr.getRequestBuilder(), clazz, handler);
  }
  public <T> Future<T> process(String url, final ITypeListener<T> listener, WebServicesAsyncHandler<T> handler) throws IOException {
    AsyncWebResource wr = client.asyncResource(url);
    return process(wr, listener, handler);
  }

  public <T> T process(final WebResource.Builder wr, final Class<T> clazz, final WebServicesHandler<T> handler) throws IOException {
    return SecureExecutor.execute(new SecureExecutor.WorkLoad<T>(){
      @Override
      public T run()
      {
        return handler.process(wr, clazz);
      }
    });
  }

  public <T> Future<T> process(final AsyncWebResource wr, final ITypeListener<T> listener, final WebServicesAsyncHandler<T> handler) throws IOException {
    return SecureExecutor.execute(new SecureExecutor.WorkLoad<Future<T>>(){
      @Override
      public Future<T> run()
      {
        return handler.process(wr, listener);
      }
    });
  }

  /**
   *
   * @param <T>
   */
  public static abstract class WebServicesHandler<T> {
    public abstract T process(WebResource.Builder webResource, Class<T> clazz);

    @Override
    public String toString()
    {
      return "WebServicesHandler{Abstract class Useful in Future}";
    }
  }

  /**
   *
   * @param <T>
   */
  public static abstract class WebServicesAsyncHandler<T> {
    public abstract Future<T> process(AsyncWebResource webResource, ITypeListener<T> listener);

    @Override
    public String toString()
    {
      return "WebServicesAsyncHandler{Abstract class Useful in Future}";
    }
  }

  public static class GetWebServicesHandler<T> extends WebServicesHandler<T> {

    @Override
    public T process(WebResource.Builder webResource, Class<T> clazz)
    {
      return webResource.get(clazz);
    }

  }
  public static class GetWebServicesAsyncHandler<T> extends WebServicesAsyncHandler<T> {

    @Override
    public Future<T> process(AsyncWebResource webResource, ITypeListener<T> listener)
    {
      return webResource.get(listener);
    }

  }

  public static class DeleteWebServicesHandler<T> extends WebServicesHandler<T>
  {
    @Override
    public T process(WebResource.Builder webResource, Class<T> clazz)
    {
      return webResource.delete(clazz);
    }

  }

  public static class DeleteWebServicesAsyncHandler<T> extends WebServicesAsyncHandler<T>
  {
    @Override
    public Future<T> process(AsyncWebResource webResource, ITypeListener<T> listener)
    {
      return webResource.delete(listener);
    }

  }
}
