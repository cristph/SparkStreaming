package com.DaDaoYunJiSuan.SparkStreaming.spark;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Created by Cristph on 2017/10/18.
 */
public class JavaCustomReceiver extends Receiver<String> {

    String host = null;
    int port = -1;

    public JavaCustomReceiver(String host_, int port_) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        host = host_;
        port = port_;
    }

    @Override
    public void onStart() {
        new Thread(this::receive).start();
    }

    @Override
    public void onStop() {}

    private void receive() {
        Socket socket = null;
        String userInput = null;

        try {
            // connect to the server
            socket = new Socket(host, port);

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            // Until stopped or connection broken continue reading
            while (!isStopped() && (userInput = reader.readLine()) != null) {
                System.out.println("Received data '" + userInput + "'");
                store(userInput);
            }
            reader.close();
            socket.close();

            // Restart in an attempt to connect again when server is active again
            restart("Trying to connect again");
        } catch (ConnectException ce) {
            // restart if could not connect to server
            restart("Could not connect", ce);
        } catch (Throwable t) {
            // restart if there is any other error
            restart("Error receiving data", t);
        }
    }
}
