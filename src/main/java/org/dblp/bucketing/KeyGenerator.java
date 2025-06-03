package org.dblp.bucketing;

public class KeyGenerator {

    public static String generateKeyByBucket(int number, int bucketSize) {
        if (number == 0) {
            return "0";
        } else if (number % bucketSize > 0) {
            int mod = number % bucketSize;
            return (number - mod + 1) + "-" + (number + (bucketSize - mod));
        } else {
            return (number - bucketSize + 1) + "-" + number;
        }
    }
}
