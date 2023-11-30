package data;

public class InvalidTupleException extends DataException {

    public InvalidTupleException(Throwable cause) {
        super(cause);
    }

    public InvalidTupleException() {
    }

    public InvalidTupleException(final String message) {
        super(message);
    }
}
