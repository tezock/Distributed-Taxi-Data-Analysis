package edu.utexas.cs.cs378;

import java.net.ServerSocket;
import java.net.Socket;

import java.util.PriorityQueue;
import java.io.IOException;

import java.io.BufferedWriter;
import java.io.FileWriter;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.Input;

import java.util.HashSet;


public class MergeServer {

    private static int portNumber;
    private static String outputFileName;
    private static PriorityQueue<Driver> topKDrivers;
    private static int k = 10;

    public static void main(String[] args) 
        throws IOException, InterruptedException {

        if (args.length != 2) {

            System.out.println("Incorrect Argument Count.");
            System.out.println("Correct usage: <port number> <output file name>");
            return;
        }

        try {
            System.err.println("Usage: MergeServer <port number> <output file name>");
            portNumber = Integer.parseInt(args[0]);
            outputFileName = args[1] + ".txt";
        }
        catch (Exception e) {
            System.out.println("Incorrect Arguments.");
            System.out.println("Correct usage: <port number> <output file name>");
        }
        
        // set up server
        ServerSocket serverSocket = new ServerSocket(portNumber);
        System.out.println("Server is listening on port " + portNumber);

        // accept children
        Socket clientSocket1 = serverSocket.accept();
        System.out.println("Accepted child (1/2)");
        Socket clientSocket2 = serverSocket.accept();
        System.out.println("Accepted child (2/2)");

        // initialize topK queue
        topKDrivers = new PriorityQueue<Driver>();

        // allocate and process data from the clients
        Thread thread1 = allocateThreadForSocket(clientSocket1);
        Thread thread2 = allocateThreadForSocket(clientSocket2);
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        System.out.println("Received all data from reducers. Writing final result to output file.");

        // flush the results to an output file.
        flushResultsToDisk();

        // close the server socket.
        serverSocket.close();

        System.out.println("Successfully written to file: " + outputFileName);
        System.out.println("Done with all jobs!");
    }

    /*
     * Helper method to allocate a thread for the client.
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
     * Handles a client connection by merging the top k from the child machines.
     */
    private static void handleClient(Socket socket) 
        throws IOException {

        // set up a new kryo
        Kryo kryo = new Kryo();

        // register relevant classes & derived types
        kryo.register(Driver.class);
        kryo.register(HashSet.class);

        // set up input & output to the socket
        Input input = new Input(socket.getInputStream());
        Output output = new Output(socket.getOutputStream());

        // read while there's data
        while (true) {

            try {

                Driver driver = kryo.readObject(input, Driver.class);

                // safely update the topKDrivers heap.
                synchronized (topKDrivers) {

                    topKDrivers.add(driver);

                    // if we hvae more than k drivers, pop the one with the least $/min.
                    if (topKDrivers.size() > k)
                        topKDrivers.poll();
                }
            }

            // on an exception, we're done reading.
            catch (Exception e) {
                break;
            }
        }

        System.out.println("Finished reading from a client.");
        input.close();
        output.close();
        socket.close();
    }

    private static void flushResultsToDisk() {

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName, false))) {

            while (topKDrivers.size() > 0) {
                Driver driver = topKDrivers.poll();
                writer.write(driver.toString());
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
    }
    
}
