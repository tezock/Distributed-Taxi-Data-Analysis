package edu.utexas.cs.cs378;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.Input;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.PriorityQueue;

public class ReducerServer {

    static public int portNumber = 33333;
    private static PriorityQueue<Driver> topKDrivers;
    private static int k = 10;
    private static String hostName;
    private static int hostPort;

    public static void main(String[] args) {

        // handle incorrect args length.
        if (args.length != 3) {
            System.out.println("Not enough arguments.");
            System.out.println("Correct usage: <port number> <host name> <host port>");
        }

        // parse arguments.
        try {
            System.err.println("Usage: ReducerServer <port number> <host name> <host port>");
            portNumber = Integer.parseInt(args[0]);
            hostName = args[1];
            hostPort = Integer.parseInt(args[2]);
        }
        catch (Exception e) {
            System.out.println("Difficulty parsing arguments: " + e.getMessage());
            System.out.println("Correct usage: <port number> <host name> <host port>");
            return;
        }

		try {

            // set up the server
			ServerSocket serverSocket = new ServerSocket(portNumber);

			System.out.println("Server is running on port number " + portNumber);
			System.out.println("Waiting for client connection ...");

            // accept clients
			Socket clientSocket1 = serverSocket.accept();
            Thread thread1 = allocateThreadForSocket(clientSocket1);
            System.out.println("Accepted client connection (1/2)");

            Socket clientSocket2 = serverSocket.accept();
            Thread thread2 = allocateThreadForSocket(clientSocket2);
            System.out.println("Accepted client connection (2/2)");


            topKDrivers = new PriorityQueue<>();

            // start reading from child machines in cluster and wait for them to finish.
            thread1.start();
            thread2.start();
            thread1.join();
            thread2.join();

            System.out.println("All data received!");
            System.out.println("Sending data to merger...");

            /// flush our top K to the 
            Socket mergeSocket = new Socket(hostName, hostPort);
            flushDataToMerger(mergeSocket);

            // close the server.
            serverSocket.close();

		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
    }

    /*
     * Flushes all of the data we've accumulated from child machines to the
     * merger server.
     */
    private static void flushDataToMerger(Socket socket) 
        throws IOException {
        
        // create new kryo instance
        Kryo kryo = new Kryo();
        
        // register relevant & derived types
        kryo.register(Driver.class);
        kryo.register(HashSet.class);

        // initialize in/out streams
        Input input = new Input(socket.getInputStream());
        Output output = new Output(socket.getOutputStream());

        // send each of the drivers through the socket.
        while (topKDrivers.size() > 0) {
            kryo.writeObject(output, topKDrivers.poll());
        }

        // flush the buffer.
        output.flush();

        // close resources
        input.close();
        output.close();
        socket.close();
    }

    /*
     * Allocates a thread for a child machine to handle the client.
     */
    private static Thread allocateThreadForSocket(Socket socket) {

        return new Thread(() -> {
            try {
                handleClient(socket);
            } catch (Exception e) {
                System.out.println("Exception.");
                e.printStackTrace();
                return;
            }
        });
    }

    /*
     * Handles a connection with a client.
     */
    private static void handleClient(Socket socket) 
        throws InterruptedException, IOException {

        Kryo kryo = new Kryo();

        kryo.register(Driver.class);
        kryo.register(HashSet.class);

        Input inputStream = new Input(socket.getInputStream());
        Output outputStream = new Output(socket.getOutputStream());

        // do work until there's a problem
        while (true) {

            try {

                // read a driver from the socket
                Driver driver = kryo.readObject(inputStream, Driver.class);

                // safely add the current driver to the heap.
                synchronized (topKDrivers) {

                    topKDrivers.add(driver);
                    
                    // if we have more than 10 drivers in the heap, remove the
                    // driver with the least money per minute.
                    if (topKDrivers.size() > k) {
                       topKDrivers.poll();
                    }
                }
            }

            // when there's a problem reading, exit.
            catch (Exception e) {
                break;
            }
        }

        System.out.println("Finished reading from a client");
        inputStream.close();
        outputStream.close();
        socket.close();
    }
}
