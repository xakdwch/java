package io.connector.mysql;

public class traceMessage extends RuntimeException {
    public traceMessage(String s) {
        super(s);
    }

    public traceMessage(Throwable throwable) {
        super(throwable);
    }

    public traceMessage(String s, Throwable throwable) {
        super(s, throwable);
    }

    public static int getLineNumber() {
        return Thread.currentThread().getStackTrace()[2].getLineNumber();
    }

    public static String getMethodName() {
        return Thread.currentThread().getStackTrace()[2].getMethodName();
    }

    public static String getFileName() {
        return Thread.currentThread().getStackTrace()[2].getFileName();
    }
}
