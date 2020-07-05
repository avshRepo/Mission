package models;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * This class extends BufferedOutputStream by adding write line method
 */
public class BufferedOutputStreamExtension extends BufferedOutputStream {
    public BufferedOutputStreamExtension(OutputStream out) {
        super(out);
    }

    public BufferedOutputStreamExtension(OutputStream out, int size) {
        super(out, size);
    }

    /**
     * This method write the value plus new line
     *
     * @param value - the value we want to write
     *
     * @throws IOException
     */
    public void writeLine(String value) throws IOException {
        this.write(value.getBytes());
        this.write("\n".getBytes());
    }
}
