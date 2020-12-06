package com.amane.main;

import com.amane.zmq.ZmqServer;
import org.apache.log4j.BasicConfigurator;

public class Main {

    public static void main(String[] args) {
        BasicConfigurator.configure();
        ZmqServer zmqServer = new ZmqServer();
        zmqServer.start();
    }
}
