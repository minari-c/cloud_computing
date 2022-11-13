package com.B202044051.DepartureDelayCount;

import org.apache.hadoop.io.Text;

public class AirlinePerformanceParser {
    private int year;
    private int month;

    private int departureDelayTime = 0;
    private int arrivalDelayTime = 0;

    private boolean departureDelayAvailable = true;
    private boolean arrivalDelayAvailable = true;

    public AirlinePerformanceParser(Text text) {
        try {
            String[] colums = text.toString().split(",");

            year = Integer.parseInt(colums[0]);     // 한 줄의 년도
            month =  Integer.parseInt(colums[1]);   // 한 줄의 월

            if(!colums[15].equals("NA")) {
                departureDelayTime = Integer.parseInt(colums[15]);
            } else {
                departureDelayAvailable = false;
            }

            if(!colums[14].equals("NA")) {
                arrivalDelayTime = Integer.parseInt(colums[14]);
            } else {
                arrivalDelayAvailable = false;
            }

        } catch (Exception e) {
            System.out.printf("Error parsing a recode: %s\n", e.getMessage());
        }
    }

    public int getYear() { return year; }

    public int getMonth() { return month; }

    public int getDepartureDelayTime() { return departureDelayTime; }

    public int getArrivalDelayTime() { return arrivalDelayTime; }

    public boolean isDepartureDelayAvailable() { return departureDelayAvailable; }

    public boolean isArrivalDelayAvailable() { return arrivalDelayAvailable; }
}
