package edu.utexas.cs.cs378;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.Input;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

public class MapperClient {
    
    private static String hostName1;
    private static int hostPort1;
    private static String hostName2;
    private static int hostPort2;
    private static BlockingQueue<String> messageQueue;
    private static ConcurrentHashMap<String, Driver> driverMappings;
    private final static int NUM_WRITERS = Math.max(Runtime.getRuntime().availableProcessors() - 1, 1);
    private static String datasetName = "taxi-data-sorted-small.csv.bz2";
    private static ArrayList<Driver> drivers;

    /*
     * 
     */
    public static void main(String[] args) 
        throws InterruptedException, FileNotFoundException, CompressorException, IOException {

        if (args.length != 5) {
            System.out.println("Incorrect Argument Count.");
            System.out.println("Correct usage: <host name 1> <host port 1> <host name 2> <host port 2> <file name>");
            return;
        }
        try {
			System.err.println("Usage: MapperClient <host name 1> <host port 1> <host name 2> <host port 2> <file name>");
			hostName1 = args[0];
			hostPort1 = Integer.parseInt(args[1]);
            hostName2 = args[2];
			hostPort2 = Integer.parseInt(args[3]);
            datasetName = args[4];
		}
        catch (Exception e) {
            System.out.println("Difficulty parsing arguments: " + e.getMessage());
            System.out.println("Correct usage: <host name 1> <host port 1> <host name 2> <host port 2> <file name>");
            return;
        }

        try {

            /* stage 1: reading the data */

            messageQueue = new LinkedBlockingQueue<String>();
            driverMappings = new ConcurrentHashMap<String, Driver>();

            System.out.println("Creating " + NUM_WRITERS + " worker threads...");
            ArrayList<Thread> workers = createWorkers(NUM_WRITERS);
            System.out.println("Reading lines from " + datasetName + "...");
            readLines(datasetName);
            waitForWorkers(workers);

            drivers = new ArrayList<>(driverMappings.values());

            System.out.println("Done processing data. Found " + drivers.size() + " unique drivers.");

            /* stage 2: send the data to the first layer of reducers */

            System.out.println("Connecting to servers to transmit data ... ");

            Socket reducerSocket1 = new Socket(hostName1, hostPort1);
            Socket reducerSocket2 = new Socket(hostName2, hostPort2);

            System.out.println("Connections to 2/2 servers Established, sending data ...");
            
            Thread thread1 = allocateThreadForSocket(reducerSocket1, 0);
            Thread thread2 = allocateThreadForSocket(reducerSocket2, 1);
            thread1.start();
            thread2.start();
            thread1.join();
            thread2.join();
            
            System.out.println("Done sending driver data to reducer server!");
        }
        catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
			e.printStackTrace();
		}
    }

    private static void sendDriverData(Socket socket, int start) 
        throws IOException {

        // initialize new kryo object
        Kryo kryo = new Kryo();

        // register all classes & derived datatypes involved
        kryo.register(Driver.class);
        kryo.register(HashSet.class);
        kryo.register(String.class);

        // get the input and output streams
        Input inputStream = new Input(socket.getInputStream());
        Output outputStream = new Output(socket.getOutputStream());

        // send every other driver!
        for (int i = 0; i < drivers.size(); i += 2) {
            kryo.writeObject(outputStream, drivers.get(i));
        }

        // flush remaining data in the buffer.
        outputStream.flush();

        // close open resources
        inputStream.close();
        outputStream.close();
        socket.close();
    }

    /*
     * Concurrently updates the mappings for each driver.
     */
    private static void updateDriverMappings() 
        throws InterruptedException {

        while (true) {

            String line = messageQueue.take();

            // if we've read an EOF, we're done reading
            if ("EOF".equals(line))
                break;

            

            // if the line is valid, report the trip. otherwise, report an invalid line.
            try {

                Trip trip = new Trip(line);

                driverMappings.compute(trip.getDriver(), (id, driver) -> {

                    if (driver == null) {
                        driver = new Driver(id);
                    }

                    driver.reportTrip(trip);

                    return driver;
                });
            }
            
            catch (IllegalArgumentException e) {
                // if we're here, we found an invalid line.
            }
        }
        return;
    }

    private static void waitForWorkers(ArrayList<Thread> workers) {

        for (Thread worker : workers) {
            try {
                worker.join();
            }
            catch (InterruptedException e) {
                System.out.println("Error with waiting on workers...");
                return;
            }
        }
    }

    private static ArrayList<Thread> createWorkers(int numWorkers) {

        ArrayList<Thread> workers = new ArrayList<>();
        for (int i = 0; i < numWorkers; i++) {
            Thread thread = new Thread(() -> {
                try {
                    updateDriverMappings();
                } catch (InterruptedException e) {
                    System.out.println("Thread interrupted. Exiting...");
                    return;
                }
            });
            thread.start();
            workers.add(thread);
        }

        return workers;
    }

    /*
	 * Reads lines from the input file and adds them into a message queue to be
     * consumed by worker threads.
	 */
	public static void readLines(String dataset) 
		throws FileNotFoundException, CompressorException, IOException, InterruptedException {

        // read the file specified from the user
		FileInputStream fin = new FileInputStream(dataset);
		BufferedInputStream bis = new BufferedInputStream(fin);
		CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
		BufferedReader br = new BufferedReader(new InputStreamReader(input));

		String line;

		// add each line into the message queue.
		while ((line = br.readLine()) != null) {
			messageQueue.put(line);
		}

        // tell each of the writers to stop reading.
        for (int i = 0; i < NUM_WRITERS; i++)
            messageQueue.put("EOF");
	}

    /*
     * Helper method to allocate a thread for the client for mappers.
     * Defines the offset for the current socket.
     */
    private static Thread allocateThreadForSocket(Socket socket, int start) {

        return new Thread(() -> {
            try {
                sendDriverData(socket, start);
            } catch (Exception e) {
                System.out.println("Exception.");
                e.printStackTrace();
                return;
            }
        });
    }
}
