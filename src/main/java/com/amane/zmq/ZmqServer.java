package com.amane.zmq;

import com.amane.handler.HBaseHandler;
import org.zeromq.ZMQ;

public class ZmqServer {

    private static final String SERVER_ADDRESS = "tcp://10.128.201.144:5555";

    public void start() {
        new Thread(() -> {
            ZMQ.Context context = ZMQ.context(1);
            ZMQ.Socket server = context.socket(ZMQ.REP);
            server.bind(SERVER_ADDRESS);
            // open TCPKeepAlive
            server.setTCPKeepAlive(1);
            // detect the connection in 2 minutes
            server.setTCPKeepAliveIdle(120L);
            // resend the package if no response in 10s
            server.setTCPKeepAliveInterval(1L);
            // three detection indicates to useless
            server.setTCPKeepAliveCount(3);
            while (!Thread.currentThread().isInterrupted()) {
                byte[] request = server.recv(0);
                byte[] reply = HBaseHandler.handle(request);
                server.send(reply, ZMQ.NOBLOCK);
            }
        }).start();
    }
}
