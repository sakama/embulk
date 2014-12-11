package org.quickload.spi;

import java.io.InputStream;
import java.io.OutputStream;
import org.quickload.buffer.Buffer;
import org.quickload.buffer.BufferAllocator;
import org.quickload.channel.BufferOutput;
import org.quickload.channel.BufferInput;
import org.quickload.channel.ChannelAsynchronousCloseException;
import org.quickload.channel.ChannelInterruptedException;

public abstract class BufferPlugins
{
    // NB: BufferOutput.channel.maxQueuedSize must be equal or larger than 1024
    public static long transferInputStream(BufferAllocator bufferAllocator,
            InputStream input, BufferOutput output) throws PartialTransferException
    {
        long transferredSize = 0;
        try {
            while (true) {
                Buffer buffer = bufferAllocator.allocateBuffer(1024);
                int len = input.read(buffer.get());
                if (len < 0) {
                    break;
                } else if (len > 0) {
                    buffer.limit(len);
                    output.add(buffer);
                    transferredSize += len;
                }
            }
        } catch (ChannelInterruptedException ex) {
            throw ex;  // deterministic exception
        } catch (ChannelAsynchronousCloseException ex) {
            throw ex;  // deterministic exception
        } catch (Exception ex) {
            throw new PartialTransferException(ex, transferredSize);
        }
        return transferredSize;
    }

    public static long transferBufferInput(BufferAllocator bufferAllocator,
            BufferInput input, OutputStream output) throws PartialTransferException
    {
        long transferredSize = 0;
        try {
            for (Buffer buffer : input) {
                if (buffer.get() != null) {
                    output.write(buffer.get(), 0, buffer.limit());
                    transferredSize += buffer.limit();
                }
                buffer.release();
            }
        } catch (ChannelInterruptedException ex) {
            throw ex;  // deterministic exception
        } catch (ChannelAsynchronousCloseException ex) {
            throw ex;  // deterministic exception
        } catch (Exception ex) {
            throw new PartialTransferException(ex, transferredSize);
        }
        return transferredSize;
    }
}
