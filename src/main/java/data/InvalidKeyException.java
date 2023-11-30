package data;

public class InvalidKeyException extends DataException {

    public InvalidKeyException() {
    }

    public InvalidKeyException(final String message) {
        super(message);
    }
}
