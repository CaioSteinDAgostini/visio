package data;

public class InvalidFieldException extends DataException {

    public InvalidFieldException() {
        super();
    }

    public InvalidFieldException(final String message) {
        super(message);
    }
}
