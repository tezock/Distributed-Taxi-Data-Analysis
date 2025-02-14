package edu.utexas.cs.cs378;

import java.util.HashSet;

/**
 * This class represents a Driver.
 * 
 * @author kiat
 *
 */
public class Driver implements Comparable<Driver> {

	private String id;
	private int taxiCount;
	private float totalAmount;
    private HashSet<String> taxiIds;
    private long timeSpentDriving;

    public Driver() {};

	public Driver(String id) {
		this.id = id;
		this.taxiCount = 0;
        this.taxiIds = new HashSet<String>();
        this.totalAmount = 0;
        this.timeSpentDriving = 0;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getNumTaxis() {
		return taxiCount;
	}

	public float getTotalAmount() {
		return totalAmount;
	}

	public void setTotalAmount(float totalAmount) {
		this.totalAmount = totalAmount;
	}

    public float getMoneyPerMinute() {

        if (timeSpentDriving == 0)
            return 0;
        
        float minutesSpentDriving = timeSpentDriving / 60.0f;
		return totalAmount / minutesSpentDriving;
	}

    public float getTimeSpentDriving() {
		return timeSpentDriving;
	}

    /*
     * Reports a trip for the current driver.
     * 
     * Must be synchronized, as multiple threads can be active in the code
     * for a given driver, so as to prevent race conditions.
     */
    public synchronized void reportTrip(Trip trip) {

        String taxiId = trip.getTaxi();
        float tripAmount = trip.getAmount();
        long tripDuration = trip.getTripDuration();
        this.timeSpentDriving += tripDuration;
        this.totalAmount += tripAmount;

        if (this.taxiIds.add(taxiId))
            this.taxiCount = this.taxiIds.size();
    }

    @Override
    public int compareTo(Driver other) {
        return Float.compare(this.getMoneyPerMinute(), other.getMoneyPerMinute());
    }

    /*
     * Stores the 
     */
	@Override
	public String toString() {
		return "Driver [id=" + id + ", taxis=" + taxiCount + ", totalAmount=" + totalAmount + ", moneyPerMinute=" + getMoneyPerMinute() + "]";
	}

}
