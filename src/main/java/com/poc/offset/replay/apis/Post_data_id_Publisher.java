package com.poc.offset.replay.apis;

import com.poc.offset.replay.utility.AppUtility;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

public class Post_data_id_Publisher {
    private static final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(100))
            .build();

    public void publishDataToHermes(String dataFileIdGroup,String env) throws IOException {
        Map<String, String> properties = AppUtility.loadProperties();
        try {
            String[] arrayDataFile = dataFileIdGroup.split(",");
            String url = null;
            if (env.equalsIgnoreCase("QA")) {
                url = properties.get("hermesQA");
            } else if (env.equalsIgnoreCase("STAGE")) {
                url = properties.get("hermesStage");
                ;
            } else if (env.equalsIgnoreCase("PROD")) {
                url = properties.get("hermesProd");
                ;
            }
            for (int i = 0; i < arrayDataFile.length; i++) {
                try {
                    System.out.println("Running for: " + env + ",Method as POST with " + arrayDataFile[i] + ".URL is:" + url + arrayDataFile[i]);
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(new URI(url + arrayDataFile[i]))
                            .version(HttpClient.Version.HTTP_2)
                            .timeout(Duration.ofSeconds(100))
                            .POST(HttpRequest.BodyPublishers.noBody())
                            .build();
                    HttpResponse<Void> response = httpClient.send(request,HttpResponse.BodyHandlers.discarding());
                    AppUtility.writeToFile(response.statusCode() + ",Done for: " + env + " with " + arrayDataFile[i]);
                    System.out.println(response.statusCode() + ". Published for: " + env + " with data_file_id => " + arrayDataFile[i]);
                } catch (Exception e) {
                    AppUtility.writeToFile("Exception for:" + env + " with " + arrayDataFile[i]);
                    e.printStackTrace();
                }
                System.out.println("Waiting for completing transfers to hermes......");
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Completed");
        }
    }

    public void bulkPublishHermes(String env) {
        String path = null;
        if (env.equalsIgnoreCase("QA")) {
            path = "src\\main\\resources\\QA_data_file_id";
        } else if (env.equalsIgnoreCase("STAGE")) {
            path = "src\\main\\resources\\Stage_data_file_id";
        } else if (env.equalsIgnoreCase("PROD")) {
            path = "src\\main\\resources\\Prod_data_file_id";
        }
        try {
            BufferedReader br = new BufferedReader(new FileReader(path));
            String line;
            StringBuilder dataFileIdGroup = new StringBuilder();
            while ((line = br.readLine()) != null) {
                dataFileIdGroup.append(line);
            }
            br.close();
            publishDataToHermes(dataFileIdGroup.toString(),env);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

}
