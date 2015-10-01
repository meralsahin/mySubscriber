package my_subscriber_group.my_subscriber_artifact;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpIOExceptionHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.logging.Logger;

public class RetryHttpInitializerWrapper
implements HttpRequestInitializer {
    private static final Logger LOG = Logger.getLogger(RetryHttpInitializerWrapper.class.getName());
    private final Credential wrappedCredential;
    private final Sleeper sleeper;

    public RetryHttpInitializerWrapper(Credential wrappedCredential) {
        this(wrappedCredential, Sleeper.DEFAULT);
    }

    RetryHttpInitializerWrapper(Credential wrappedCredential, Sleeper sleeper) {
        this.wrappedCredential = Preconditions.checkNotNull(wrappedCredential);
        this.sleeper = sleeper;
    }

    @Override
    public void initialize(HttpRequest request) {
        request.setReadTimeout(120000);
        final HttpBackOffUnsuccessfulResponseHandler backoffHandler = new HttpBackOffUnsuccessfulResponseHandler(new ExponentialBackOff()).setSleeper(this.sleeper);
        request.setInterceptor(this.wrappedCredential);
        request.setUnsuccessfulResponseHandler(new HttpUnsuccessfulResponseHandler(){

            @Override
            public boolean handleResponse(HttpRequest request, HttpResponse response, boolean supportsRetry) throws IOException {
                if (RetryHttpInitializerWrapper.this.wrappedCredential.handleResponse(request, response, supportsRetry)) {
                    return true;
                }
                if (backoffHandler.handleResponse(request, response, supportsRetry)) {
                    LOG.info("Retrying " + request.getUrl());
                    return true;
                }
                return false;
            }
        });
        request.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(new ExponentialBackOff()).setSleeper(this.sleeper));
    }

}

