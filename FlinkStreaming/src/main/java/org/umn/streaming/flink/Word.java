package org.umn.streaming.flink;

class Word {
    String text;
    int count;

    Word(String text, int count) {
        this.text = text;
        this.count = count;
    }

    @Override
    public int hashCode() {
        return this.text.hashCode();
    }
}
