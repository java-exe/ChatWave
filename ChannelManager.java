import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ChannelManager {

    private static final Logger LOG = Logger.getLogger(ChannelManager.class.getName());

    // log format: ts \t from \t type \t payload
    // type=text   -> payload is the message text
    // type=attach -> payload is kind;;;url;;;name;;;mime;;;size

    public static class Channel {
        public final String name;
        public final boolean persistHistory;
        public final Predicate<ChatWaveServer.Client> accessCheck;

        public Channel(String name, boolean persistHistory,
                       Predicate<ChatWaveServer.Client> accessCheck) {
            this.name           = name;
            this.persistHistory = persistHistory;
            this.accessCheck    = accessCheck;
        }
    }

    private final Map<String, Channel> channels = new HashMap<>();
    private final Path historyDir;

    public ChannelManager(Path historyDir) {
        this.historyDir = historyDir;
        try {
            Files.createDirectories(historyDir);
        } catch (IOException e) {
            LOG.log(Level.WARNING, "could not create history dir: " + historyDir, e);
        }
    }

    public void register(Channel c)              { channels.put(c.name, c); }
    public boolean exists(String channel)        { return channels.containsKey(channel); }
    public Channel get(String channel)           { return channels.get(channel); }

    public List<String> readHistory(String channel, int limit) {
        Path f = historyFile(channel);
        if (!Files.exists(f)) return Collections.emptyList();
        try {
            List<String> all = Files.readAllLines(f, StandardCharsets.UTF_8);
            if (limit <= 0 || all.size() <= limit) return all;
            return all.subList(all.size() - limit, all.size());
        } catch (IOException e) {
            LOG.log(Level.WARNING, "readHistory failed for channel: " + channel, e);
            return Collections.emptyList();
        }
    }

    public void appendText(String channel, String from, String text, String timestamp) {
        appendLine(channel, String.join("\t", timestamp, from, "text", safe(text)));
    }

    public void appendAttach(String channel, String from, String kind, String url,
                             String name, String mime, long size, String timestamp) {
        String payload = String.join(";;;", kind, safe(url), safe(name), safe(mime), String.valueOf(size));
        appendLine(channel, String.join("\t", timestamp, from, "attach", payload));
    }

    private void appendLine(String channel, String line) {
        Path f = historyFile(channel);
        try {
            Files.createDirectories(f.getParent());
            Files.write(f, (line + "\n").getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            LOG.log(Level.WARNING, "appendLine failed for channel: " + channel, e);
        }
    }

    private Path historyFile(String channel) {
        return historyDir.resolve("chan_" + channel + ".log");
    }

    private static String safe(String s) {
        return s == null ? "" : s.replace("\n", " ").replace("\r", " ");
    }

    public static String nowTs() {
        return DateTimeFormatter.ofPattern("HH:mm dd.MM.yyyy").format(LocalDateTime.now());
    }
}
