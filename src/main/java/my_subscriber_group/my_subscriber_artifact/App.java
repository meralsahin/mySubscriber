package my_subscriber_group.my_subscriber_artifact;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.ListSubscriptionsResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.pubsub.model.Subscription;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class App {
    public static Pubsub createPubsubClient() throws IOException {
        return App.createPubsubClient(Utils.getDefaultTransport(), Utils.getDefaultJsonFactory());
    }

    public static Pubsub createPubsubClient(HttpTransport httpTransport, JsonFactory jsonFactory) throws IOException {
        Preconditions.checkNotNull(httpTransport);
        Preconditions.checkNotNull(jsonFactory);
        GoogleCredential credential = GoogleCredential.getApplicationDefault(httpTransport, jsonFactory);
        if (credential.createScopedRequired()) {
            credential = credential.createScoped(PubsubScopes.all());
        }
        RetryHttpInitializerWrapper initializer = new RetryHttpInitializerWrapper(credential);
        return new Pubsub.Builder(httpTransport, jsonFactory, initializer).setApplicationName("PubSub Sample").build();
    }

    public static void main(String[] args) throws IOException {
        ListSubscriptionsResponse response;
        System.out.println("Hello World!");
        NetHttpTransport transport = new NetHttpTransport();
        JacksonFactory jsonFactory = new JacksonFactory();
        GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
        if (credential.createScopedRequired()) {
            credential = credential.createScoped(BigqueryScopes.all());
        }
        Bigquery bg = new Bigquery.Builder(transport, jsonFactory, credential).setApplicationName("Bigquery Samples").build();
        Pubsub pubsub = App.createPubsubClient();
        Subscription subscription = new Subscription().setTopic("projects/myhelloworldprojectmeral/topics/mytopic").setAckDeadlineSeconds(10);
        String subscriptionName = "mysubscription";
        String projectName = "myhelloworldprojectmeral";
        String fullName = "projects/" + projectName + "/subscriptions/" + subscriptionName;
        boolean subscriptionAlreadyExists = false;
        Pubsub.Projects.Subscriptions.List listMethod = pubsub.projects().subscriptions().list("projects/" + projectName);
        String nextPageToken = null;
        do {
            List<Subscription> subscriptions;
            if (nextPageToken != null) {
                listMethod.setPageToken(nextPageToken);
            }
            if ((subscriptions = (response = (ListSubscriptionsResponse)listMethod.execute()).getSubscriptions()) == null) continue;
            for (Subscription subscr : subscriptions) {
                System.out.println("Found subscription: " + subscr.getName());
                if (!subscr.getName().equals(fullName)) continue;
                subscriptionAlreadyExists = true;
            }
        } while ((nextPageToken = response.getNextPageToken()) != null);
        if (!subscriptionAlreadyExists) {
            Subscription newSubscription = (Subscription)pubsub.projects().subscriptions().create(fullName, subscription).execute();
            System.out.println("Created: " + newSubscription.getName());
        }
        int batchSize = 10;
        PullRequest pullRequest = new PullRequest().setReturnImmediately(false).setMaxMessages(batchSize);
        do {
            PullResponse pullResponse = (PullResponse)pubsub.projects().subscriptions().pull(fullName, pullRequest).execute();
            ArrayList<String> ackIds = new ArrayList<String>(batchSize);
            List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessages();
            if (receivedMessages == null || receivedMessages.isEmpty()) {
                System.out.println("There were no messages.");
                continue;
            }
            for (ReceivedMessage receivedMessage : receivedMessages) {
                PubsubMessage pubsubMessage = receivedMessage.getMessage();
                if (pubsubMessage != null) {
                    System.out.print("Message: ");
                    String messageAsString = new String(pubsubMessage.decodeData(), "UTF-8");
                    System.out.println(messageAsString);
                    Map rowData = null;
                    rowData = (Map)new ObjectMapper().readValue(messageAsString, HashMap.class);
                    bg.tabledata().insertAll("myhelloworldprojectmeral", "mydataset", "mytable", new TableDataInsertAllRequest().setRows(Collections.singletonList(new TableDataInsertAllRequest.Rows().setJson(rowData)))).execute();
                }
                ackIds.add(receivedMessage.getAckId());
            }
            AcknowledgeRequest ackRequest = new AcknowledgeRequest().setAckIds(ackIds);
            pubsub.projects().subscriptions().acknowledge(fullName, ackRequest).execute();
        } while (true);
    }
}

