package info.makingsense.trevas.batch.model;

public class Input {

    private String name;

    private String format;

    private String location;

    public Input(String name, String format, String location) {
        this.name = name;
        this.format = format;
        this.location = location;
    }

    public Input() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }
}
