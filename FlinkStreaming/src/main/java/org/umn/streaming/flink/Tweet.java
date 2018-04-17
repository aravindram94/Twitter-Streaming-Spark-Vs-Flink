package org.umn.streaming.flink;

class Tweet {
    String id;
    String text;
    String place;
    String sentiment = null;
    Tweet(String id, String text, String place) {
        this.id = id;
        this.text = text;
        this.place = place;
    }
}
