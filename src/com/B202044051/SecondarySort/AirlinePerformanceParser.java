package com.B202044051.SecondarySort;

import org.apache.hadoop.io.Text;

public class AirlinePerformanceParser {
    private int year;
    private int month;

    private int departureDelayTime = 0;
    private int arrivalDelayTime = 0;
    private int distance = 0;


    private boolean departureDelayAvailable = true;
    private boolean arrivalDelayAvailable = true;
    private boolean distanceAvailable = true;

    private String uniqueCarrier;

    public AirlinePerformanceParser(Text text) {
        try {
            String[] colums = text.toString().split(",");

            this.year = Integer.parseInt(colums[0]);     // 한 줄의 년도
            this.month =  Integer.parseInt(colums[1]);   // 한 줄의 월

            this.uniqueCarrier = colums[8];


            if(!colums[14].equals("NA")) {
                this.arrivalDelayTime = Integer.parseInt(colums[14]);
            } else {
                this.arrivalDelayAvailable = false;
            }

            if(!colums[15].equals("NA")) {
                this.departureDelayTime = Integer.parseInt(colums[15]);
            } else {
                this.departureDelayAvailable = false;
            }

            if(!colums[18].equals("NA")) {
                this.distance = Integer.parseInt(colums[18]);
            } else {
                this.distanceAvailable = false;
            }

        } catch (Exception e) {
            System.out.printf("Error parsing a recode: %s\n", e.getMessage());
        }
    }

    public int getYear() { return this.year; }

    public int getMonth() { return this.month; }

    public int getDistance() { return this.distance; }

    public int getDepartureDelayTime() { return this.departureDelayTime; }

    public int getArrivalDelayTime() { return this.arrivalDelayTime; }

    public String getUniqueCarrier() { return this.uniqueCarrier; }

    public boolean isDepartureDelayAvailable() { return this.departureDelayAvailable; }

    public boolean isArrivalDelayAvailable() { return this.arrivalDelayAvailable; }

    public boolean isDistanceAvailable() { return this.distanceAvailable; }
}
