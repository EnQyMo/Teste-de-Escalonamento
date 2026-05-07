package utils;

/**
 * Custom exception for JSON parsing errors
 */
public class JsonParseException extends Exception {

    /**
     * Constructor with message and cause
     * @param message Error message
     * @param cause Root cause exception
     */
    public JsonParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
