package com.amane.enumurate;

public enum MessageEnum {

    SUCCESS("success", "successfully executed and the result is in line with expectations"),
    WRONG("wrong", "successfully executed but the result isn't in line with expectations"),
    FAIL("fail", "execution failed");

    private String message;

    private String instructions;

    MessageEnum(String message, String instructions) {
        this.message = message;
        this.instructions = instructions;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getInstructions() {
        return instructions;
    }

    public void setInstructions(String instructions) {
        this.instructions = instructions;
    }

}
