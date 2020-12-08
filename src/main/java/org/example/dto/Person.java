package org.example.dto;

public class Person {
    private int id;
    private String enrichedString;

    public Person(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getEnrichedString() {
        return enrichedString;
    }

    public void setEnrichedString(String enrichedString) {
        this.enrichedString = enrichedString;
    }
}
