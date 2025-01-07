package one.terenin.datagenerator.generator.fw;

import lombok.RequiredArgsConstructor;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

@RequiredArgsConstructor
public class BufferedWriter  implements OutputFile {

    private final ByteArrayOutputStream bos;

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        return createPositionOutputstream();
    }

    private PositionOutputStream createPositionOutputstream() {
        return new PositionOutputStream() {

            int pos = 0;

            @Override
            public long getPos() throws IOException {
                return pos;
            }

            @Override
            public void flush() throws IOException {
                bos.flush();
            };

            @Override
            public void close() throws IOException {
                bos.close();
            };

            @Override
            public void write(int b) throws IOException {
                bos.write(b);
                pos++;
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                bos.write(b, off, len);
                pos += len;
            }
        };
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return createPositionOutputstream();
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }
}
