package info.makingsense.trevas.batch.model;

import java.util.List;

public class BatchConfiguration {

    private List<Input> inputs;

    private List<Output> outputs;

    private String script;

    public BatchConfiguration(List<Input> inputs, List<Output> outputs, String script) {
        this.inputs = inputs;
        this.outputs = outputs;
        this.script = script;
    }

    public BatchConfiguration() {
    }

    public List<Input> getInputs() {
        return inputs;
    }

    public void setInputs(List<Input> inputs) {
        this.inputs = inputs;
    }

    public List<Output> getOutputs() {
        return outputs;
    }

    public void setOutputs(List<Output> outputs) {
        this.outputs = outputs;
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }
}
