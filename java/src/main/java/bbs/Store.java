package bbs;

import bbs.ChatProto.LogEntry;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

final class Store {
    static final class Entry {
        String id = "";
        String origin = "";
        String kind = "";
        double ts = 0;
        long clock = 0;
        String user = "";
        String channel = "";
        String text = "";

        LogEntry toProto() {
            return LogEntry.newBuilder()
                    .setId(id)
                    .setOrigin(origin)
                    .setKind(kind)
                    .setTimestamp(ts)
                    .setClock(clock)
                    .setUser(user)
                    .setChannel(channel)
                    .setText(text)
                    .build();
        }

        static Entry fromProto(LogEntry le) {
            Entry e = new Entry();
            e.id = le.getId();
            e.origin = le.getOrigin();
            e.kind = le.getKind();
            e.ts = le.getTimestamp();
            e.clock = le.getClock();
            e.user = le.getUser();
            e.channel = le.getChannel();
            e.text = le.getText();
            return e;
        }
    }

    private final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    private final String serverName;
    final Path path;
    final List<Entry> entries = new ArrayList<>();
    private final Set<String> ids = new HashSet<>();
    private final Set<String> channels = new TreeSet<>();
    private final FileOutputStream file;

    Store(String dataDir, String serverName) {
        this.serverName = serverName;
        try {
            Files.createDirectories(Path.of(dataDir));
            this.path = Path.of(dataDir, serverName + ".jsonl");
            if (Files.exists(path)) {
                for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                    if (!line.isBlank()) {
                        apply(gson.fromJson(line, Entry.class));
                    }
                }
            }
            this.file = new FileOutputStream(path.toFile(), true);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void apply(Entry e) {
        entries.add(e);
        ids.add(e.id);
        if (e.kind.equals("channel")) {
            channels.add(e.channel);
        }
    }

    Entry newEntry(String kind, double ts, long clock, String user, String channel, String text) {
        Entry e = new Entry();
        e.id = UUID.randomUUID().toString().replace("-", "");
        e.origin = serverName;
        e.kind = kind;
        e.ts = ts;
        e.clock = clock;
        e.user = user;
        e.channel = channel;
        e.text = text;
        return e;
    }

    boolean add(Entry e) {
        if (ids.contains(e.id)) {
            return false;
        }
        apply(e);
        try {
            file.write((gson.toJson(e) + "\n").getBytes(StandardCharsets.UTF_8));
            file.flush();
            file.getFD().sync();
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return true;
    }

    List<Entry> messages(String channel) {
        List<Entry> msgs = new ArrayList<>();
        for (Entry e : entries) {
            if (e.kind.equals("message") && e.channel.equals(channel)) {
                msgs.add(e);
            }
        }
        msgs.sort(Comparator.<Entry>comparingLong(x -> x.clock)
                .thenComparing(x -> x.origin)
                .thenComparing(x -> x.id));
        return msgs;
    }

    boolean hasChannel(String name) {
        return channels.contains(name);
    }

    List<String> channelNames() {
        return new ArrayList<>(channels);
    }
}
