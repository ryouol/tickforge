package dev.tickforge.persistence;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** PostgreSQL simple-protocol proxy: drop COMMIT's acknowledgement after the server commits. */
final class CommitAckProxy implements AutoCloseable {
  private final ServerSocket listener;
  private final ExecutorService executor = Executors.newFixedThreadPool(2);
  private volatile Socket client, server;
  final AtomicBoolean dropped = new AtomicBoolean();

  CommitAckProxy(String host, int port, int commitToDrop) throws IOException {
    listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
    executor.submit(
        () -> {
          try {
            client = listener.accept();
            server = new Socket(host, port);
            executor.submit(
                () -> {
                  try {
                    client.getInputStream().transferTo(server.getOutputStream());
                  } catch (IOException ignored) {
                  }
                });
            DataInputStream in = new DataInputStream(server.getInputStream());
            OutputStream out = client.getOutputStream();
            int commits = 0;
            while (true) {
              int type = in.readUnsignedByte();
              int length = in.readInt();
              if (length < 4 || length > 16777216) throw new IOException("invalid protocol frame");
              byte[] body = in.readNBytes(length - 4);
              if (type == 'C'
                  && new String(body, StandardCharsets.UTF_8).equals("COMMIT\0")
                  && ++commits == commitToDrop) {
                dropped.set(true);
                client.close();
                server.close();
                return;
              }
              DataOutputStream frame = new DataOutputStream(out);
              frame.writeByte(type);
              frame.writeInt(length);
              frame.write(body);
              frame.flush();
            }
          } catch (IOException ignored) {
          }
        });
  }

  int port() {
    return listener.getLocalPort();
  }

  @Override
  public void close() throws IOException {
    listener.close();
    if (client != null) client.close();
    if (server != null) server.close();
    executor.shutdownNow();
  }
}
