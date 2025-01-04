package one.terenin.datagenerator.generator.fw;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

public class OzoneOutputFile implements OutputFile {

    private final OutputStream outputStream;

    public OzoneOutputFile(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
        return new PositionOutputStream() {
            private long position = 0;

            @Override
            public void write(int b) throws IOException {
                outputStream.write(b);
                position++;
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                outputStream.write(b, off, len);
                position += len;
            }

            @Override
            public long getPos() {
                return position;
            }

            @Override
            public void close() throws IOException {
                outputStream.close();
            }
        };
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
        return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

    @Override
    public String getPath() {
        return "ozone_output_file";
    }
}