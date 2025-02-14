package edu.utexas.cs.cs378;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

public class Trip {

    // the indices of each field
    private static final int MEDALLION = 0;
    private static final int HACK_LICENSE = 1;
    private static final int FARE_AMOUNT = 11;
    private static final int SURCHARGE = 12;
    private static final int MTA_TAX = 13;
    private static final int TIP_AMOUNT = 14;
    private static final int TOLLS_AMOUNT = 15;
    private static final int TOTAL_AMOUNT = 16;
    private static final int TRIP_TIME_IN_SECS = 4;
    private static final int PICKUP_LONGITUDE = 6;
    private static final int PICKUP_LATITUDE = 7;
    private static final int DROPOFF_LONGITUDE = 8;
    private static final int DROPOFF_LATITUDE = 9;
    private static final int PAYMENT_TYPE = 10;
    private static final int PICKUP_DATETIME = 2;
    private static final int DROPOFF_DATETIME = 3;

    // irrelevant fields for the current use case

    // private static final int TRIP_DISTANCE = 5;
    
    // the fields representing a trip
    private String medallion;
    private String hackLicense;
    private float totalAmount;
    private long tripTimeInSecs;


    // irrelevant fields for the current use-case
    // private String pickupDatetime;
    // private String dropoffDatetime;
    // private String tripDistance;
    // private String pickupLongitude;
    // private String pickupLatitude;
    // private String dropoffLongitude;
    // private String dropoffLatitude;
    // private String paymentType;
    // private String fareAmount;
    // private String surcharge;
    // private String mtaTax;
    // private String tipAmount;
    // private String tollsAmount;

    // regex patterns for validating dates
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Pattern DATE_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");

    public Trip() {};

    
    /*
     * Initializes a 'trip' object.
     * 
     * Each trip instance represents a valid trip.
     */
    public Trip(String line) {

        // split the line by delimeter
        String[] parsedLine = line.split(",");
        
        // handle invalid lines
        if (!lineIsValid(parsedLine))
            throw new IllegalArgumentException("invalid line");

        // initialize relevant fields
        medallion = parsedLine[MEDALLION];
        hackLicense = parsedLine[HACK_LICENSE];
        totalAmount = Float.parseFloat(parsedLine[TOTAL_AMOUNT]);
        tripTimeInSecs = Long.parseLong(parsedLine[TRIP_TIME_IN_SECS]);
    }
    
    /*
     * Gets the driver for the current trip
     */
    public String getDriver() {
        return hackLicense;
    }

    /*
     * Returns the taxi used for the current trip
     */
    public String getTaxi() {
        return medallion;
    }

    /*
     * Returns the total amount/cost of the current trip
     */
    public float getAmount() {
        return totalAmount;
    }

    /*
     * Returns the duration of the trip in seconds.
     */
    public long getTripDuration() {
        return tripTimeInSecs;
    }

    /*
     * Helper method to determine whether a given string is a float or not.
     * If an exception is caught, we simply return false.
     */
    private static boolean isFloat(String str) {
        try {
            Float.parseFloat(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /*
     * Helper method to determine whether a given string is a long or not.
     * If an exception is caught, we simply return false.
     */
    private static boolean isLong(String str) {
        try {
            Long.parseLong(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /*
     * Helper method to ensure that the line used to construct this 'Trip'
     * is valid.
     */
    private boolean lineIsValid(String[] parsedLine) {

        // ensure line length
        if (parsedLine.length != 17) 
            return false;

        // ensure money values are floats
        if (!isFloat(parsedLine[FARE_AMOUNT]))
            return false;
        if (!isFloat(parsedLine[SURCHARGE]))
            return false;
        if (!isFloat(parsedLine[MTA_TAX]))
            return false;
        if (!isFloat(parsedLine[TIP_AMOUNT]))
            return false;
        if (!isFloat(parsedLine[TOLLS_AMOUNT]))
            return false;
        if (!isFloat(parsedLine[TOTAL_AMOUNT]))
            return false;

        // ensure the total charge matches (w/ flexibility for floating point math)
        float fareAmount = Float.parseFloat(parsedLine[FARE_AMOUNT]);
        float surcharge = Float.parseFloat(parsedLine[SURCHARGE]);
        float mtaTax = Float.parseFloat(parsedLine[MTA_TAX]);
        float tipAmount = Float.parseFloat(parsedLine[TIP_AMOUNT]);
        float tollsAmount = Float.parseFloat(parsedLine[TOLLS_AMOUNT]);

        float testAmount = fareAmount + surcharge + mtaTax + tipAmount + tollsAmount;
        float totalAmount = Float.parseFloat(parsedLine[TOTAL_AMOUNT]);

        if (Math.abs(testAmount - totalAmount) >= 0.05)
            return false;
        
        // ignore rides with total amount >= 500.
        if (totalAmount >= 500)
            return false;

        // do checks for longitude & latitude to ensure they're reasonable
        if (!isFloat(parsedLine[PICKUP_LATITUDE]))
            return false;
        if (!isFloat(parsedLine[PICKUP_LONGITUDE]))
            return false;
        if (!isFloat(parsedLine[DROPOFF_LATITUDE]))
            return false;
        if (!isFloat(parsedLine[DROPOFF_LONGITUDE]))
            return false;

        float pickup_latitude = Float.parseFloat(parsedLine[PICKUP_LATITUDE]);
        float pickup_longitude = Float.parseFloat(parsedLine[PICKUP_LONGITUDE]);
        float dropoff_latitude = Float.parseFloat(parsedLine[DROPOFF_LATITUDE]);
        float dropoff_longitude = Float.parseFloat(parsedLine[DROPOFF_LONGITUDE]);

        if (Math.abs(pickup_latitude) > 90 || Math.abs(dropoff_latitude) > 90)
            return false;
        if (Math.abs(pickup_longitude) > 180 || Math.abs(dropoff_longitude) > 180)
            return false;

        // validate the trip length
        if (!isLong(parsedLine[TRIP_TIME_IN_SECS]))
            return false;

        LocalDateTime start = validateAndParse(parsedLine[PICKUP_DATETIME]);
        LocalDateTime end = validateAndParse(parsedLine[DROPOFF_DATETIME]);

        if (start == null || end == null)
            return false;
        
        long actualDuration = java.time.Duration.between(start, end).getSeconds();
        long statedDuration = Long.parseLong(parsedLine[TRIP_TIME_IN_SECS]);

        if (actualDuration != statedDuration || actualDuration < 30)
            return false;

        // ensure that we know the payment type
        if ("UNK".equals(parsedLine[PAYMENT_TYPE]))
            return false;

        // if we meet data cleaning criteria, return true!
        return true;
    }

    public static LocalDateTime validateAndParse(String dateStr) {

        // check if format matches expected pattern
        if (!DATE_PATTERN.matcher(dateStr).matches()) {
            return null;
        }

        // attempt to parse the date
        try {
            return LocalDateTime.parse(dateStr, FORMATTER);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

}
