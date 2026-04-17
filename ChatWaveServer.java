import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.net.URLEncoder;
import java.net.URLDecoder;

public class ChatWaveServer {

    private static final Logger LOG = Logger.getLogger(ChatWaveServer.class.getName());

    private static final int    PORT = 12345;
    private static final String HOST = "0.0.0.0";

    private static final Path DATA_DIR      = Paths.get(".").toAbsolutePath().normalize();
    private static final Path USERS_FILE    = DATA_DIR.resolve("users.db");
    private static final Path UPLOAD_DIR    = DATA_DIR.resolve("uploads");
    private static final Path DM_DIR        = DATA_DIR.resolve("dm");
    private static final Path TMP_DIR       = DATA_DIR.resolve("tmp");
    private static final Path INVENTORY_FILE = DATA_DIR.resolve("inventory.db");
    private static final Path PROFILES_FILE = DATA_DIR.resolve("profiles.db");
    private static final Path CHANNEL_DIR   = DATA_DIR.resolve("channels");

    private static final int MAX_FILE_SIZE = 10 * 1024 * 1024;

    private static final Map<String, UserRec>       USERS       = new ConcurrentHashMap<>();
    private static final Map<String, Client>        ONLINE      = new ConcurrentHashMap<>();
    private static final Map<String, Long>          MUTED_UNTIL = new ConcurrentHashMap<>();
    private static final Set<String>                BANNED      = ConcurrentHashMap.newKeySet();
    private static final Map<String, Call>          CALLS       = new ConcurrentHashMap<>();
    private static final Map<String, Set<String>>   DM_INDEX    = new ConcurrentHashMap<>();

    // pending DMs for users who were offline at send time; delivered on next login
    private static final Map<String, List<Map<String, String>>> PENDING_DMS = new ConcurrentHashMap<>();

    private static final Map<String, Set<String>> INV_TITLES = new ConcurrentHashMap<>();
    private static final Map<String, Set<String>> INV_COLORS = new ConcurrentHashMap<>();
    private static final Map<String, String>      PROF_TITLE = new ConcurrentHashMap<>();
    private static final Map<String, String>      PROF_COLOR = new ConcurrentHashMap<>();

    private static final List<String> ROLE_ORDER = Arrays.asList("user", "mod", "admin", "dev");

    private static final SecureRandom    RNG  = new SecureRandom();
    private static final ExecutorService POOL = Executors.newCachedThreadPool();

    // --------------------------------------------------------------------------------------------
    // Data model
    // --------------------------------------------------------------------------------------------

    static final class Client {
        final Socket       socket;
        final InputStream  in;
        final OutputStream out;
        volatile String  name;
        volatile String  avatarUrl = "";
        volatile String  role      = "user";
        volatile boolean closed    = false;

        Client(Socket s) throws IOException {
            this.socket = s;
            this.in     = s.getInputStream();
            this.out    = s.getOutputStream();
        }

        boolean isLoggedIn() { return name != null; }

        void sendText(String json) {
            if (closed) return;
            try {
                writeWsFrame(out, (byte) 0x1, json.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                closed = true;
            }
        }

        void close() {
            closed = true;
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private static final class UserRec {
        final String username;
        final String salt;
        final String hashHex;
        String avatar;
        String role;

        UserRec(String u, String s, String h, String a, String r) {
            username = u;
            salt     = s;
            hashHex  = h;
            avatar   = a == null ? "" : a;
            role     = (r == null || r.isBlank()) ? "user" : r;
        }
    }

    private static final class Call {
        final String   id;
        final String   a;
        final String   b;
        volatile String mode;

        Call(String id, String a, String b, String mode) {
            this.id   = id;
            this.a    = a;
            this.b    = b;
            this.mode = mode;
        }

        String other(String who) { return who.equals(a) ? b : a; }
    }

    // --------------------------------------------------------------------------------------------
    // Startup
    // --------------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Files.createDirectories(UPLOAD_DIR);
        Files.createDirectories(DM_DIR);
        Files.createDirectories(TMP_DIR);
        loadUsers();
        loadInventoryAndProfiles();
        rebuildDmIndex();

        try (ServerSocket server = new ServerSocket()) {
            server.bind(new InetSocketAddress(HOST, PORT));
            LOG.info("ChatWaveServer listening on " + HOST + ":" + PORT);
            while (true) {
                Socket s = server.accept();
                POOL.submit(() -> handleConnection(s));
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Connection / HTTP / WS handshake
    // --------------------------------------------------------------------------------------------

    private static void handleConnection(Socket s) {
        try {
            s.setTcpNoDelay(true);
            InputStream  in  = s.getInputStream();
            OutputStream out = s.getOutputStream();

            BufferedReader br  = new BufferedReader(new InputStreamReader(in, StandardCharsets.US_ASCII));
            String         req = br.readLine();
            if (req == null) { s.close(); return; }

            Map<String, String> headers = new LinkedHashMap<>();
            String line;
            while ((line = br.readLine()) != null && !line.isEmpty()) {
                int i = line.indexOf(':');
                if (i > 0)
                    headers.put(line.substring(0, i).trim().toLowerCase(Locale.ROOT),
                                line.substring(i + 1).trim());
            }

            String[] parts  = req.split("\\s+");
            String   method = parts.length > 0 ? parts[0] : "";
            String   path   = parts.length > 1 ? parts[1] : "/";

            if ("websocket".equalsIgnoreCase(headers.getOrDefault("upgrade", ""))
                    && "GET".equals(method) && path.startsWith("/chat")) {
                doWebSocketHandshake(headers, out);
                wsLoop(new Client(s));
                return;
            }

            if ("GET".equals(method)) {
                handleHttpGet(path, out);
            } else {
                writeHttp(out, "405 Method Not Allowed", "text/plain; charset=utf-8",
                          "Method Not Allowed".getBytes(StandardCharsets.UTF_8));
            }

            try { s.close(); } catch (IOException ignored) {}

        } catch (Exception e) {
            LOG.log(Level.WARNING, "handleConnection error", e);
        }
    }

    private static void handleHttpGet(String path, OutputStream out) throws IOException {
        if (path.startsWith("/uploads/")) {
            Path f = safeJoin(UPLOAD_DIR, path.substring("/uploads/".length()));
            if (f == null || !Files.exists(f) || Files.isDirectory(f)) {
                writeHttp(out, "404 Not Found", "text/plain; charset=utf-8",
                          "Not Found".getBytes(StandardCharsets.UTF_8));
                return;
            }
            writeHttp(out, "200 OK", guessMime(f.getFileName().toString()), Files.readAllBytes(f));
            return;
        }
        writeHttp(out, "200 OK", "text/plain; charset=utf-8",
                  ("ChatWave running. " + new Date()).getBytes(StandardCharsets.UTF_8));
    }

    private static void doWebSocketHandshake(Map<String, String> headers, OutputStream out) throws Exception {
        String key = headers.get("sec-websocket-key");
        if (key == null) throw new IOException("missing Sec-WebSocket-Key");
        String accept = base64(sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
                                    .getBytes(StandardCharsets.US_ASCII)));
        String resp = "HTTP/1.1 101 Switching Protocols\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Accept: " + accept + "\r\n\r\n";
        out.write(resp.getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    // --------------------------------------------------------------------------------------------
    // WebSocket message loop
    // --------------------------------------------------------------------------------------------

    private static void wsLoop(Client c) {
        try {
            c.sendText(json(Map.of("type", "hello", "timestamp", nowTime())));
            while (!c.closed) {
                WsFrame f = readWsFrame(c.in);
                if (f == null) break;
                if (f.opcode == 0x8) break;
                if (f.opcode == 0x9) { writeWsFrame(c.out, (byte) 0xA, f.payload); continue; }
                if (f.opcode != 0x1) continue;

                Map<String, String> kv   = parseJson(new String(f.payload, StandardCharsets.UTF_8));
                String              type = kv.get("type");
                if (type == null) continue;

                switch (type) {
                    case "register"        -> doRegister(c, kv);
                    case "login"           -> doLogin(c, kv);
                    case "chat"            -> handleGlobalChat(c, kv);
                    case "send_file"       -> handleGlobalFile(c, kv);
                    case "dm_history"      -> handleDmHistory(c, kv);
                    case "dm_send"         -> handleDmSend(c, kv);
                    case "dm_send_file"    -> handleDmSendFile(c, kv);
                    case "chan_join"        -> handleChanJoin(c, kv);
                    case "chan_history"     -> handleChanHistory(c, kv);
                    case "chan_send"        -> handleChanSend(c, kv);
                    case "chan_send_file"   -> handleChanSendFile(c, kv);
                    case "set_avatar"      -> handleAvatarUrl(c, kv);
                    case "set_avatar_upload" -> handleAvatarUpload(c, kv);
                    case "inventory_get"   -> handleInventoryGet(c);
                    case "inventory_admin" -> handleInventoryAdmin(c, kv);
                    case "set_title"       -> handleSetTitle(c, kv);
                    case "set_name_color"  -> handleSetNameColor(c, kv);
                    case "rtc_call"        -> handleRtcCall(c, kv);
                    case "rtc_accept"      -> handleRtcAccept(c, kv);
                    case "rtc_decline"     -> handleRtcDecline(c, kv);
                    case "rtc_offer", "rtc_answer", "rtc_ice" -> relayCall(c, kv);
                    case "rtc_hangup"      -> handleRtcHangup(c, kv);
                }
            }
        } catch (Exception e) {
            LOG.log(Level.FINE, "wsLoop ended for " + c.name, e);
        } finally {
            if (c.isLoggedIn()) {
                ONLINE.remove(c.name);
                broadcast(json(Map.of("type", "user_left", "user", c.name, "timestamp", nowTime())), null);
            }
            c.close();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Auth
    // --------------------------------------------------------------------------------------------

    private static void doRegister(Client c, Map<String, String> kv) {
        String username = safeUser(kv.get("username"));
        String password = kv.get("password");
        if (username == null || password == null || password.isEmpty()) {
            err(c, "bad_request", "Missing username/password"); return;
        }
        synchronized (USERS) {
            if (USERS.containsKey(username.toLowerCase())) {
                err(c, "username_taken", "Username already taken."); return;
            }
            String salt        = genSalt();
            String hash        = hashPw(password, salt);
            String defaultRole = USERS.isEmpty() ? "dev" : "user";
            USERS.put(username.toLowerCase(), new UserRec(username, salt, hash, "", defaultRole));
            saveUsers();
        }
        completeLogin(c, username);
        c.sendText(json(Map.of("type", "register_ok", "user", username,
                               "role", roleOfUser(username), "timestamp", nowTime())));
    }

    private static void doLogin(Client c, Map<String, String> kv) {
        String username = safeUser(kv.get("username"));
        String password = kv.get("password");
        if (username == null || password == null) {
            err(c, "bad_request", "Missing username/password"); return;
        }

        UserRec rec = USERS.get(username.toLowerCase());
        // same error for unknown user and wrong password to avoid username enumeration
        if (rec == null || !hashPw(password, rec.salt).equals(rec.hashHex)) {
            err(c, "invalid_login", "Invalid credentials"); return;
        }
        if (BANNED.contains(username.toLowerCase())) {
            err(c, "banned", "You are banned."); return;
        }

        completeLogin(c, rec.username);
        broadcastFullProfile(rec.username);

        Map<String, String> resp = new LinkedHashMap<>();
        resp.put("type",      "login_ok");
        resp.put("user",      rec.username);
        resp.put("avatar",    rec.avatar == null ? "" : rec.avatar);
        resp.put("role",      roleOfUser(rec.username));
        resp.put("timestamp", nowTime());
        c.sendText(json(resp));

        deliverPendingDms(c);
    }

    private static void completeLogin(Client c, String username) {
        UserRec rec = USERS.getOrDefault(username.toLowerCase(),
                                         new UserRec(username, "", "", "", "user"));
        c.name      = username;
        c.avatarUrl = rec.avatar == null ? "" : rec.avatar;
        c.role      = roleOf(rec.role);
        ONLINE.put(username, c);

        sendUserList(c);
        sendDmList(c);

        broadcast(json(Map.of(
                "type",      "user_joined",
                "user",      username,
                "role",      c.role,
                "avatar",    c.avatarUrl,
                "timestamp", nowTime()
        )), c);
    }

    private static void deliverPendingDms(Client c) {
        List<Map<String, String>> pending = PENDING_DMS.remove(c.name.toLowerCase());
        if (pending == null) return;
        for (Map<String, String> msg : pending) {
            c.sendText(json(msg));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Global chat
    // --------------------------------------------------------------------------------------------

    private static void handleGlobalChat(Client c, Map<String, String> kv) {
        if (!c.isLoggedIn()) { err(c, "not_authenticated", "Not logged in."); return; }
        String msg = kv.getOrDefault("text", "").trim();
        if (msg.isEmpty()) return;

        if (msg.startsWith("/")) { handleCommand(c, msg); return; }
        if (isMuted(c.name)) { err(c, "muted", "You are muted."); return; }

        broadcast(json(Map.of("type", "message", "from", c.name,
                              "text", msg, "timestamp", nowTime())), null);
    }

    private static void handleGlobalFile(Client c, Map<String, String> kv) {
        if (!c.isLoggedIn()) { err(c, "not_authenticated", "Not logged in."); return; }
        if (isMuted(c.name)) { err(c, "muted", "You are muted."); return; }

        String name   = safeFilename(kv.get("name"));
        String mime   = safeMime(kv.get("mime"));
        String data64 = kv.get("data");
        if (name == null || data64 == null) return;

        try {
            byte[] raw  = Base64.getDecoder().decode(data64);
            Path   dest = uniqueUploadPath(name);
            Files.write(dest, raw);
            String url = "/uploads/" + UPLOAD_DIR.relativize(dest).toString().replace('\\', '/');

            Map<String, String> m = new LinkedHashMap<>();
            m.put("type",      "file");
            m.put("from",      c.name);
            m.put("url",       url);
            m.put("name",      name);
            m.put("mime",      mime);
            m.put("size",      String.valueOf(raw.length));
            m.put("timestamp", nowTime());
            broadcast(json(m), null);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "file upload failed", e);
            err(c, "upload_fail", "Upload failed.");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Channels
    // --------------------------------------------------------------------------------------------

    private static void handleChanJoin(Client c, Map<String, String> kv) {
        String ch = kv.getOrDefault("channel", "");
        if ("beta".equals(ch) && !hasBetaAccess(c.name, c.role))
            c.sendText(json(Map.of("type", "chan_error", "channel", ch,
                                   "message", "No access", "timestamp", nowTime())));
    }

    private static void handleChanHistory(Client c, Map<String, String> kv) {
        String ch = kv.getOrDefault("channel", "");
        if ("beta".equals(ch) && !hasBetaAccess(c.name, c.role)) {
            c.sendText(json(Map.of("type", "chan_error", "channel", ch,
                                   "message", "No access", "timestamp", nowTime())));
            return;
        }
        int limit = 200;
        try { limit = Integer.parseInt(kv.getOrDefault("limit", "200")); } catch (NumberFormatException ignored) {}

        Map<String, String> resp = new LinkedHashMap<>();
        resp.put("type",      "chan_history");
        resp.put("channel",   ch);
        resp.put("lines",     String.join("\n", readChannelLines(ch, limit)));
        resp.put("timestamp", nowTime());
        c.sendText(json(resp));
    }

    private static void handleChanSend(Client c, Map<String, String> kv) {
        if (!c.isLoggedIn()) { err(c, "not_authenticated", "Not logged in."); return; }
        if (isMuted(c.name)) { err(c, "muted", "You are muted."); return; }

        String ch   = kv.getOrDefault("channel", "");
        String text = kv.getOrDefault("text", "").trim();
        if (text.isEmpty()) return;

        if ("beta".equals(ch) && !hasBetaAccess(c.name, c.role)) {
            c.sendText(json(Map.of("type", "chan_error", "channel", ch,
                                   "message", "No access", "timestamp", nowTime())));
            return;
        }

        String ts = nowTime();
        appendChannelLine(ch, ts, c.name, "text", text);
        broadcast(json(Map.of("type", "chan_message", "channel", ch,
                              "from", c.name, "text", text, "timestamp", ts)), null);
    }

    private static void handleChanSendFile(Client c, Map<String, String> kv) {
        if (!c.isLoggedIn()) { err(c, "not_authenticated", "Not logged in."); return; }
        if (isMuted(c.name)) { err(c, "muted", "You are muted."); return; }

        String ch     = kv.getOrDefault("channel", "");
        String name   = safeFilename(kv.get("name"));
        String mime   = safeMime(kv.get("mime"));
        String data64 = kv.get("data");
        if (name == null || data64 == null) return;

        if ("beta".equals(ch) && !hasBetaAccess(c.name, c.role)) {
            c.sendText(json(Map.of("type", "chan_error", "channel", ch,
                                   "message", "No access", "timestamp", nowTime())));
            return;
        }

        try {
            byte[] raw  = Base64.getDecoder().decode(data64);
            Path   dest = uniqueUploadPath(name);
            Files.write(dest, raw);
            String url  = "/uploads/" + UPLOAD_DIR.relativize(dest).toString().replace('\\', '/');
            String ts   = nowTime();
            String kind = (mime != null && mime.startsWith("image/")) ? "image" : "file";
            appendChannelAttach(ch, ts, c.name, kind, url, name, mime, raw.length);

            Map<String, String> m = new LinkedHashMap<>();
            m.put("type",      "chan_file");
            m.put("channel",   ch);
            m.put("from",      c.name);
            m.put("url",       url);
            m.put("name",      name);
            m.put("mime",      mime);
            m.put("size",      String.valueOf(raw.length));
            m.put("timestamp", ts);
            broadcast(json(m), null);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "chan file upload failed", e);
            err(c, "upload_fail", "Upload failed.");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Direct messages
    // --------------------------------------------------------------------------------------------

    private static void handleDmSend(Client c, Map<String, String> kv) {
        if (!c.isLoggedIn()) { err(c, "not_authenticated", "Not logged in."); return; }

        String to   = safeUser(kv.get("to"));
        String text = kv.getOrDefault("text", "").trim();
        if (to == null)      { err(c, "invalid_recipient", "Invalid recipient."); return; }
        if (text.isEmpty())  { err(c, "empty_message", "Message is empty."); return; }

        String              ts      = nowTime();
        Map<String, String> payload = Map.of("type", "dm", "from", c.name,
                                             "to", to, "text", text, "timestamp", ts);
        appendDmLine(c.name, to, ts, "text", text);

        Client target = ONLINE.get(to);
        if (target != null) {
            target.sendText(json(payload));
        } else {
            // queue for delivery when the recipient comes online
            PENDING_DMS.computeIfAbsent(to.toLowerCase(), k -> new ArrayList<>()).add(payload);
        }
        c.sendText(json(payload));
    }

    private static void handleDmSendFile(Client c, Map<String, String> kv) {
        if (!c.isLoggedIn()) { err(c, "not_authenticated", "Not logged in."); return; }

        String to     = safeUser(kv.get("to"));
        String name   = safeFilename(kv.get("name"));
        String mime   = safeMime(kv.get("mime"));
        String data64 = kv.get("data");
        if (to == null || name == null || data64 == null) {
            err(c, "invalid_input", "Missing required fields."); return;
        }

        byte[] raw;
        try {
            raw = Base64.getDecoder().decode(data64);
        } catch (IllegalArgumentException e) {
            err(c, "invalid_file", "Invalid base64 data."); return;
        }
        if (raw.length > MAX_FILE_SIZE) {
            err(c, "file_too_large", "Max file size is 10 MB."); return;
        }

        final String toFinal = to;
        Thread.startVirtualThread(() -> {
            try {
                Path   dest = uniqueUploadPath(name);
                Files.write(dest, raw);
                String url  = "/uploads/" + UPLOAD_DIR.relativize(dest).toString().replace('\\', '/');
                String ts   = nowTime();
                String kind = (mime != null && mime.startsWith("image/")) ? "image" : "file";
                appendDmAttach(c.name, toFinal, ts, kind, url, name, mime, raw.length);

                Map<String, String> m = new LinkedHashMap<>();
                m.put("type",      "dm_file");
                m.put("from",      c.name);
                m.put("to",        toFinal);
                m.put("url",       url);
                m.put("name",      name);
                m.put("mime",      mime);
                m.put("size",      String.valueOf(raw.length));
                m.put("timestamp", ts);

                Client target = ONLINE.get(toFinal);
                if (target != null) {
                    target.sendText(json(m));
                } else {
                    PENDING_DMS.computeIfAbsent(toFinal.toLowerCase(), k -> new ArrayList<>()).add(m);
                }
                c.sendText(json(m));
            } catch (IOException e) {
                LOG.log(Level.WARNING, "dm file upload failed", e);
                err(c, "upload_fail", "Upload failed.");
            }
        });
    }

    private static void handleDmHistory(Client c, Map<String, String> kv) {
        if (!c.isLoggedIn()) { err(c, "not_authenticated", "Not logged in."); return; }
        String with = safeUser(kv.get("with"));
        if (with == null) return;
        int limit = 200;
        try { limit = Integer.parseInt(kv.getOrDefault("limit", "200")); } catch (NumberFormatException ignored) {}

        Map<String, String> resp = new LinkedHashMap<>();
        resp.put("type",      "dm_history");
        resp.put("with",      with);
        resp.put("lines",     String.join("\n", readDmLines(c.name, with, limit)));
        resp.put("timestamp", nowTime());
        c.sendText(json(resp));
    }

    private static void sendDmList(Client c) {
        Set<String>   partners = DM_INDEX.getOrDefault(c.name.toLowerCase(), Collections.emptySet());
        StringBuilder avatars  = new StringBuilder();
        boolean       first    = true;
        for (String p : partners) {
            String av = Optional.ofNullable(USERS.get(p.toLowerCase())).map(u -> u.avatar).orElse("");
            if (!first) avatars.append('|');
            first = false;
            avatars.append(p).append('=').append((av == null ? "" : av).replace("|", ""));
        }
        Map<String, String> resp = new LinkedHashMap<>();
        resp.put("type",      "dm_list");
        resp.put("users",     String.join(",", partners));
        resp.put("avatars",   avatars.toString());
        resp.put("timestamp", nowTime());
        c.sendText(json(resp));
    }

    // --------------------------------------------------------------------------------------------
    // Avatars
    // --------------------------------------------------------------------------------------------

    private static void handleAvatarUrl(Client c, Map<String, String> kv) {
        if (!c.isLoggedIn()) { err(c, "not_authenticated", "Not logged in."); return; }
        String  url = kv.getOrDefault("avatar", "").trim();
        UserRec r   = USERS.get(c.name.toLowerCase());
        if (r == null) return;
        r.avatar    = url;
        c.avatarUrl = url;
        saveUsers();
        c.sendText(json(Map.of("type", "avatar_ok", "avatar", url, "timestamp", nowTime())));
        broadcast(json(Map.of("type", "profile_update", "user", c.name,
                              "avatar", url, "timestamp", nowTime())), c);
    }

    private static void handleAvatarUpload(Client c, Map<String, String> kv) {
        if (!c.isLoggedIn()) { err(c, "not_authenticated", "Not logged in."); return; }
        String name   = safeFilename(kv.get("name"));
        String data64 = kv.get("data");
        if (name == null || data64 == null) return;

        try {
            byte[] raw  = Base64.getDecoder().decode(data64);
            Path   dest = uniqueUploadPath(name);
            Files.write(dest, raw);
            String url = "/uploads/" + UPLOAD_DIR.relativize(dest).toString().replace('\\', '/');

            UserRec r = USERS.get(c.name.toLowerCase());
            if (r != null) { r.avatar = url; c.avatarUrl = url; saveUsers(); }

            c.sendText(json(Map.of("type", "avatar_ok", "avatar", url, "timestamp", nowTime())));
            broadcast(json(Map.of("type", "profile_update", "user", c.name,
                                  "avatar", url, "timestamp", nowTime())), c);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "avatar upload failed", e);
            err(c, "upload_fail", "Upload failed.");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Inventory / profiles
    // --------------------------------------------------------------------------------------------

    private static void handleInventoryGet(Client c) {
        if (!c.isLoggedIn()) { err(c, "not_authenticated", "Not logged in."); return; }
        String key = c.name.toLowerCase();
        c.sendText(jsonInventoryPayload(
                new ArrayList<>(INV_TITLES.getOrDefault(key, Collections.emptySet())),
                new ArrayList<>(INV_COLORS.getOrDefault(key, Collections.emptySet()))
        ));
    }

    private static void handleInventoryAdmin(Client c, Map<String, String> kv) {
        if (!atLeast(c.role, "dev")) { err(c, "forbidden", "Need dev."); return; }

        String action = kv.getOrDefault("action", "");
        String target = safeUser(kv.getOrDefault("user", ""));
        String kind   = kv.getOrDefault("kind", "").toLowerCase();
        String value  = kv.getOrDefault("value", "");

        if (target == null || (!"title".equals(kind) && !"color".equals(kind)) || value.isBlank()) {
            err(c, "bad_request", "Invalid payload."); return;
        }
        String key = target.toLowerCase();

        switch (action) {
            case "give" -> {
                if ("title".equals(kind))
                    INV_TITLES.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(value);
                else
                    INV_COLORS.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(value);
                saveInventory();
                Client tcl = ONLINE.get(target);
                if (tcl != null) handleInventoryGet(tcl);
            }
            case "revoke" -> {
                if ("title".equals(kind)) {
                    INV_TITLES.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).remove(value);
                    if (value.equals(PROF_TITLE.getOrDefault(key, ""))) {
                        PROF_TITLE.put(key, "");
                        saveProfiles();
                        broadcast(json(Map.of("type", "profile_update", "user", target,
                                             "title", "", "nameColor", PROF_COLOR.getOrDefault(key, ""),
                                             "timestamp", nowTime())), null);
                    }
                } else {
                    INV_COLORS.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).remove(value);
                    if (value.equals(PROF_COLOR.getOrDefault(key, ""))) {
                        PROF_COLOR.put(key, "");
                        saveProfiles();
                        broadcast(json(Map.of("type", "profile_update", "user", target,
                                             "title", PROF_TITLE.getOrDefault(key, ""), "nameColor", "",
                                             "timestamp", nowTime())), null);
                    }
                }
                saveInventory();
                Client tcl = ONLINE.get(target);
                if (tcl != null) handleInventoryGet(tcl);
            }
            default -> { err(c, "bad_request", "action must be give|revoke."); return; }
        }
        sendInventoryOf(c, target);
    }

    private static void handleSetTitle(Client c, Map<String, String> kv) {
        if (!c.isLoggedIn()) { err(c, "not_authenticated", "Not logged in."); return; }
        String v = Optional.ofNullable(kv.get("value")).orElse("");
        String u = c.name.toLowerCase();
        if (!v.isEmpty() && !INV_TITLES.getOrDefault(u, Collections.emptySet()).contains(v)) {
            err(c, "forbidden", "You don't own this title."); return;
        }
        PROF_TITLE.put(u, v);
        saveProfiles();
        broadcast(json(Map.of("type", "profile_update", "user", c.name,
                              "title", v, "nameColor", PROF_COLOR.getOrDefault(u, ""),
                              "timestamp", nowTime())), null);
    }

    private static void handleSetNameColor(Client c, Map<String, String> kv) {
        if (!c.isLoggedIn()) { err(c, "not_authenticated", "Not logged in."); return; }
        String v = Optional.ofNullable(kv.get("value")).orElse("");
        String u = c.name.toLowerCase();
        if (!v.isEmpty() && !INV_COLORS.getOrDefault(u, Collections.emptySet()).contains(v)) {
            err(c, "forbidden", "You don't own this color."); return;
        }
        PROF_COLOR.put(u, v);
        saveProfiles();
        broadcast(json(Map.of("type", "profile_update", "user", c.name,
                              "title", PROF_TITLE.getOrDefault(u, ""), "nameColor", v,
                              "timestamp", nowTime())), null);
    }

    private static void sendInventoryOf(Client to, String user) {
        String key   = user.toLowerCase();
        List<String> t  = new ArrayList<>(INV_TITLES.getOrDefault(key, Collections.emptySet()));
        List<String> cs = new ArrayList<>(INV_COLORS.getOrDefault(key, Collections.emptySet()));
        String payload =
                "{\"type\":\"inventory_of\",\"user\":\"" + esc(user) + "\"," +
                "\"items\":{\"titles\":" + toJsonArray(t) + ",\"colors\":" + toJsonArray(cs) + "}," +
                "\"profile\":{\"title\":\"" + esc(PROF_TITLE.getOrDefault(key, "")) + "\"," +
                "\"nameColor\":\"" + esc(PROF_COLOR.getOrDefault(key, "")) + "\"}," +
                "\"timestamp\":\"" + esc(nowTime()) + "\"}";
        to.sendText(payload);
    }

    private static void broadcastFullProfile(String username) {
        String u = username.toLowerCase();
        broadcast(json(Map.of(
                "type",      "profile_update",
                "user",      username,
                "title",     PROF_TITLE.getOrDefault(u, ""),
                "nameColor", PROF_COLOR.getOrDefault(u, ""),
                "timestamp", nowTime()
        )), null);
    }

    // --------------------------------------------------------------------------------------------
    // WebRTC signaling
    // --------------------------------------------------------------------------------------------

    private static void handleRtcCall(Client c, Map<String, String> kv) {
        if (!c.isLoggedIn()) { err(c, "not_authenticated", "Not logged in."); return; }
        String to     = safeUser(kv.get("to"));
        String mode   = kv.getOrDefault("mode", "video");
        Client target = ONLINE.get(to);
        if (target == null) { err(c, "rtc_no_target", "User not online."); return; }

        String callId = genId();
        CALLS.put(callId, new Call(callId, c.name, to, mode));
        c.sendText(json(Map.of("type", "rtc_outgoing", "to", to,
                               "mode", mode, "callId", callId, "timestamp", nowTime())));
        target.sendText(json(Map.of("type", "rtc_incoming", "from", c.name,
                                    "mode", mode, "callId", callId, "timestamp", nowTime())));
    }

    private static void handleRtcAccept(Client c, Map<String, String> kv) {
        String callId = kv.get("callId");
        String mode   = kv.getOrDefault("mode", "video");
        Call   call   = CALLS.get(callId);
        if (call == null) return;
        call.mode = mode;
        Client target = ONLINE.get(call.other(c.name));
        if (target != null)
            target.sendText(json(Map.of("type", "rtc_accept", "from", c.name,
                                        "mode", mode, "callId", callId, "timestamp", nowTime())));
    }

    private static void handleRtcDecline(Client c, Map<String, String> kv) {
        String callId = kv.get("callId");
        Call   call   = CALLS.remove(callId);
        if (call == null) return;
        Client target = ONLINE.get(call.other(c.name));
        if (target != null)
            target.sendText(json(Map.of("type", "rtc_decline", "from", c.name,
                                        "callId", callId, "timestamp", nowTime())));
    }

    private static void relayCall(Client c, Map<String, String> kv) {
        Call call = CALLS.get(kv.get("callId"));
        if (call == null) return;
        Client target = ONLINE.get(call.other(c.name));
        if (target == null) return;
        Map<String, String> forwarded = new LinkedHashMap<>(kv);
        forwarded.put("from",      c.name);
        forwarded.put("timestamp", nowTime());
        target.sendText(json(forwarded));
    }

    private static void handleRtcHangup(Client c, Map<String, String> kv) {
        String callId = kv.get("callId");
        Call   call   = CALLS.remove(callId);
        if (call == null) return;
        Client target = ONLINE.get(call.other(c.name));
        if (target != null)
            target.sendText(json(Map.of("type", "rtc_hangup", "from", c.name,
                                        "callId", callId, "timestamp", nowTime())));
    }

    // --------------------------------------------------------------------------------------------
    // Chat commands
    // --------------------------------------------------------------------------------------------

    private static void handleCommand(Client c, String raw) {
        String[] parts  = raw.trim().split("\\s+");
        String   cmd    = parts[0].toLowerCase(Locale.ROOT);
        String   myRole = roleOfUser(c.name);

        switch (cmd) {
            case "/help" -> sysTo(c, String.join("\n",
                    "/help  /list",
                    "/mute <user> [mins]   (mod+)",
                    "/unmute <user>        (mod+)",
                    "/kick <user>          (admin+)",
                    "/ban <user>           (admin+)",
                    "/unban <user>         (admin+)",
                    "/announce <text>      (admin+)",
                    "/setrole <user> <role> (dev)",
                    "/giveitem <user> <title|color> <value>  (dev)",
                    "/revokeitem <user> <title|color> <value> (dev)",
                    "/viewinv <user>       (dev)"));

            case "/list" -> sysTo(c, "Online: " + String.join(", ", ONLINE.keySet()));

            case "/mute" -> {
                if (!atLeast(myRole, "mod")) { err(c, "forbidden", "Need mod+."); return; }
                if (parts.length < 2) { sysTo(c, "Usage: /mute <user> [minutes]"); return; }
                String u    = safeUser(parts[1]);
                int    mins = 10;
                if (parts.length >= 3) try { mins = Math.max(1, Integer.parseInt(parts[2])); }
                                        catch (NumberFormatException ignored) {}
                MUTED_UNTIL.put(u.toLowerCase(), System.currentTimeMillis() + mins * 60_000L);
                sysAll(u + " muted for " + mins + " min by " + c.name);
            }

            case "/unmute" -> {
                if (!atLeast(myRole, "mod")) { err(c, "forbidden", "Need mod+."); return; }
                if (parts.length < 2) { sysTo(c, "Usage: /unmute <user>"); return; }
                MUTED_UNTIL.remove(safeUser(parts[1]).toLowerCase());
                sysAll(parts[1] + " unmuted by " + c.name);
            }

            case "/kick" -> {
                if (!atLeast(myRole, "admin")) { err(c, "forbidden", "Need admin+."); return; }
                if (parts.length < 2) { sysTo(c, "Usage: /kick <user>"); return; }
                Client target = ONLINE.get(safeUser(parts[1]));
                if (target == null) { sysTo(c, "User not online."); return; }
                sysAll(parts[1] + " kicked by " + c.name);
                target.close();
            }

            case "/ban" -> {
                if (!atLeast(myRole, "admin")) { err(c, "forbidden", "Need admin+."); return; }
                if (parts.length < 2) { sysTo(c, "Usage: /ban <user>"); return; }
                String u = safeUser(parts[1]);
                BANNED.add(u.toLowerCase());
                sysAll(u + " banned by " + c.name);
                Client target = ONLINE.get(u);
                if (target != null) target.close();
            }

            case "/unban" -> {
                if (!atLeast(myRole, "admin")) { err(c, "forbidden", "Need admin+."); return; }
                if (parts.length < 2) { sysTo(c, "Usage: /unban <user>"); return; }
                BANNED.remove(safeUser(parts[1]).toLowerCase());
                sysAll(parts[1] + " unbanned by " + c.name);
            }

            case "/announce" -> {
                if (!atLeast(myRole, "admin")) { err(c, "forbidden", "Need admin+."); return; }
                String text = raw.substring("/announce".length()).trim();
                if (text.isEmpty()) { sysTo(c, "Usage: /announce <text>"); return; }
                sysAll("[ANNOUNCEMENT] " + text);
            }

            case "/inventory"    -> handleInventoryGet(c);
            case "/settitle"     -> {
                String v = parts.length >= 2 && !"none".equalsIgnoreCase(parts[1])
                           ? raw.substring(cmd.length()).trim() : "";
                handleSetTitle(c, Map.of("value", v));
            }
            case "/setnamecolor" -> {
                String v = parts.length >= 2 && !"none".equalsIgnoreCase(parts[1])
                           ? raw.substring(cmd.length()).trim() : "";
                handleSetNameColor(c, Map.of("value", v));
            }

            case "/giveitem" -> {
                if (!atLeast(myRole, "dev")) { err(c, "forbidden", "Need dev."); return; }
                if (parts.length < 4) { sysTo(c, "Usage: /giveitem <user> <title|color> <value>"); return; }
                String u    = safeUser(parts[1]);
                String kind = parts[2].toLowerCase(Locale.ROOT);
                String val  = raw.substring(raw.indexOf(parts[2]) + parts[2].length()).trim();
                if (u == null || val.isEmpty()) { sysTo(c, "Usage: /giveitem <user> <title|color> <value>"); return; }
                if ("title".equals(kind))
                    INV_TITLES.computeIfAbsent(u.toLowerCase(), k -> ConcurrentHashMap.newKeySet()).add(val);
                else if ("color".equals(kind))
                    INV_COLORS.computeIfAbsent(u.toLowerCase(), k -> ConcurrentHashMap.newKeySet()).add(val);
                else { sysTo(c, "kind must be title or color"); return; }
                saveInventory();
                Client target = ONLINE.get(u);
                if (target != null) handleInventoryGet(target);
                sysAll(c.name + " gave " + kind + " to " + u + ": " + val);
            }

            case "/revokeitem" -> {
                if (!atLeast(myRole, "dev")) { err(c, "forbidden", "Need dev."); return; }
                if (parts.length < 4) { sysTo(c, "Usage: /revokeitem <user> <title|color> <value>"); return; }
                String u    = safeUser(parts[1]);
                String kind = parts[2].toLowerCase(Locale.ROOT);
                String val  = raw.substring(raw.indexOf(parts[2]) + parts[2].length()).trim();
                if (u == null || val.isEmpty()) { sysTo(c, "Usage: /revokeitem <user> <title|color> <value>"); return; }
                String key = u.toLowerCase();
                if ("title".equals(kind)) {
                    INV_TITLES.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).remove(val);
                    if (val.equals(PROF_TITLE.getOrDefault(key, ""))) {
                        PROF_TITLE.put(key, ""); saveProfiles();
                        broadcast(json(Map.of("type", "profile_update", "user", u,
                                             "title", "", "timestamp", nowTime())), null);
                    }
                } else if ("color".equals(kind)) {
                    INV_COLORS.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).remove(val);
                    if (val.equals(PROF_COLOR.getOrDefault(key, ""))) {
                        PROF_COLOR.put(key, ""); saveProfiles();
                        broadcast(json(Map.of("type", "profile_update", "user", u,
                                             "nameColor", "", "timestamp", nowTime())), null);
                    }
                } else { sysTo(c, "kind must be title or color"); return; }
                saveInventory();
                Client target = ONLINE.get(u);
                if (target != null) handleInventoryGet(target);
                sysAll(c.name + " revoked " + kind + " from " + u + ": " + val);
            }

            case "/viewinv" -> {
                if (!atLeast(myRole, "dev")) { err(c, "forbidden", "Need dev."); return; }
                if (parts.length < 2) { sysTo(c, "Usage: /viewinv <user>"); return; }
                String u = safeUser(parts[1]);
                if (u == null) { sysTo(c, "User not found."); return; }
                sendInventoryOf(c, u);
            }

            case "/setrole" -> {
                if (!atLeast(myRole, "dev")) { err(c, "forbidden", "Need dev."); return; }
                if (parts.length < 3) { sysTo(c, "Usage: /setrole <user> <user|mod|admin|dev>"); return; }
                String  u       = safeUser(parts[1]);
                String  newRole = parts[2].toLowerCase(Locale.ROOT);
                if (!ROLE_ORDER.contains(newRole)) { sysTo(c, "Invalid role."); return; }
                UserRec r = USERS.get(u.toLowerCase());
                if (r == null) { sysTo(c, "No such user."); return; }
                r.role = newRole;
                saveUsers();
                Client target = ONLINE.get(r.username);
                if (target != null) target.role = newRole;
                broadcast(json(Map.of("type", "role_update", "user", r.username,
                                     "role", newRole, "timestamp", nowTime())), null);
                sysAll(r.username + " is now " + newRole + " (by " + c.name + ")");
            }

            default -> sysTo(c, "Unknown command. Try /help");
        }
    }

    // --------------------------------------------------------------------------------------------
    // User list helper
    // --------------------------------------------------------------------------------------------

    private static void sendUserList(Client to) {
        StringBuilder avatars    = new StringBuilder();
        StringBuilder roles      = new StringBuilder();
        StringBuilder profiles   = new StringBuilder();
        boolean       first      = true;

        for (Map.Entry<String, Client> e : ONLINE.entrySet()) {
            String name = e.getKey();
            String av   = e.getValue().avatarUrl == null ? "" : e.getValue().avatarUrl;
            String role = roleOfUser(name);
            String pt   = PROF_TITLE.getOrDefault(name.toLowerCase(), "");
            String pc   = PROF_COLOR.getOrDefault(name.toLowerCase(), "");
            String pj   = "{\"title\":\"" + esc(pt) + "\",\"nameColor\":\"" + esc(pc) + "\"}";

            if (!first) { avatars.append('|'); roles.append('|'); profiles.append('|'); }
            first = false;

            avatars.append(name).append('=').append(av.replace("|", "").replace("\n", ""));
            roles.append(name).append('=').append(role);
            profiles.append(name).append('=')
                    .append(URLEncoder.encode(pj, StandardCharsets.UTF_8));
        }

        Map<String, String> m = new LinkedHashMap<>();
        m.put("type",      "user_list");
        m.put("users",     String.join(",", ONLINE.keySet()));
        m.put("avatars",   avatars.toString());
        m.put("roles",     roles.toString());
        m.put("profiles",  profiles.toString());
        m.put("timestamp", nowTime());
        to.sendText(json(m));
    }

    // --------------------------------------------------------------------------------------------
    // Persistence
    // --------------------------------------------------------------------------------------------

    private static synchronized void loadUsers() {
        USERS.clear();
        if (!Files.exists(USERS_FILE)) return;
        try (BufferedReader br = Files.newBufferedReader(USERS_FILE, StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] p = line.split("\t", -1);
                if (p.length < 3) continue;
                USERS.put(p[0].toLowerCase(),
                          new UserRec(p[0], p[1], p[2],
                                      p.length >= 4 ? p[3] : "",
                                      p.length >= 5 ? p[4] : "user"));
            }
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "failed to load users", e);
        }
    }

    private static synchronized void saveUsers() {
        try (BufferedWriter bw = Files.newBufferedWriter(USERS_FILE, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (UserRec r : USERS.values()) {
                bw.write(String.join("\t", r.username, r.salt, r.hashHex,
                                    r.avatar == null ? "" : r.avatar,
                                    r.role   == null ? "user" : r.role));
                bw.newLine();
            }
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "failed to save users", e);
        }
    }

    private static synchronized void loadInventoryAndProfiles() {
        INV_TITLES.clear(); INV_COLORS.clear();
        PROF_TITLE.clear(); PROF_COLOR.clear();
        try {
            if (Files.exists(INVENTORY_FILE)) {
                for (String line : Files.readAllLines(INVENTORY_FILE, StandardCharsets.UTF_8)) {
                    String[] p = line.split("\t", -1);
                    if (p.length < 3) continue;
                    String      u = p[0].toLowerCase();
                    Set<String> t = ConcurrentHashMap.newKeySet();
                    Set<String> c = ConcurrentHashMap.newKeySet();
                    if (!p[1].isEmpty()) for (String s : p[1].split("\\|")) t.add(URLDecoder.decode(s, "UTF-8"));
                    if (!p[2].isEmpty()) for (String s : p[2].split("\\|")) c.add(URLDecoder.decode(s, "UTF-8"));
                    INV_TITLES.put(u, t);
                    INV_COLORS.put(u, c);
                }
            }
            if (Files.exists(PROFILES_FILE)) {
                for (String line : Files.readAllLines(PROFILES_FILE, StandardCharsets.UTF_8)) {
                    String[] p = line.split("\t", -1);
                    if (p.length < 3) continue;
                    String u = p[0].toLowerCase();
                    PROF_TITLE.put(u, URLDecoder.decode(p[1], "UTF-8"));
                    PROF_COLOR.put(u, URLDecoder.decode(p[2], "UTF-8"));
                }
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "failed to load inventory/profiles", e);
        }
    }

    private static synchronized void saveInventory() {
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(INVENTORY_FILE, StandardCharsets.UTF_8))) {
            for (String u : USERS.keySet()) {
                String ts = String.join("|", INV_TITLES.getOrDefault(u, Collections.emptySet())
                        .stream().map(s -> URLEncoder.encode(s, StandardCharsets.UTF_8)).toList());
                String cs = String.join("|", INV_COLORS.getOrDefault(u, Collections.emptySet())
                        .stream().map(s -> URLEncoder.encode(s, StandardCharsets.UTF_8)).toList());
                pw.println(u + "\t" + ts + "\t" + cs);
            }
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "failed to save inventory", e);
        }
    }

    private static synchronized void saveProfiles() {
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(PROFILES_FILE, StandardCharsets.UTF_8))) {
            for (String u : USERS.keySet()) {
                pw.println(u + "\t"
                        + URLEncoder.encode(PROF_TITLE.getOrDefault(u, ""), StandardCharsets.UTF_8) + "\t"
                        + URLEncoder.encode(PROF_COLOR.getOrDefault(u, ""), StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "failed to save profiles", e);
        }
    }

    // rebuild DM_INDEX from existing log files on startup so contacts survive restarts
    private static void rebuildDmIndex() {
        if (!Files.exists(DM_DIR)) return;
        try (var stream = Files.list(DM_DIR)) {
            stream.filter(p -> p.getFileName().toString().endsWith(".log")).forEach(p -> {
                String fname = p.getFileName().toString().replace(".log", "");
                String[] pair = fname.split("__", 2);
                if (pair.length == 2) indexPartner(pair[0], pair[1]);
            });
        } catch (IOException e) {
            LOG.log(Level.WARNING, "could not rebuild DM index", e);
        }
    }

    // --------------------------------------------------------------------------------------------
    // DM / channel file I/O
    // --------------------------------------------------------------------------------------------

    private static Path dmFile(String a, String b) {
        String name = a.compareToIgnoreCase(b) <= 0 ? a + "__" + b : b + "__" + a;
        return DM_DIR.resolve(name + ".log");
    }

    private static synchronized void appendDmLine(String from, String to, String ts,
                                                   String type, String payload) {
        try {
            Path f = dmFile(from, to);
            Files.createDirectories(f.getParent());
            Files.writeString(f, String.join("\t", ts, from, type, sanitize(payload)) + "\n",
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            indexPartner(from, to);
        } catch (IOException e) {
            LOG.log(Level.WARNING, "appendDmLine failed", e);
        }
    }

    private static synchronized void appendDmAttach(String from, String to, String ts,
                                                     String kind, String url, String name,
                                                     String mime, long size) {
        appendDmLine(from, to, ts, "attach",
                     String.join(";;;", kind, url, name == null ? "" : name,
                                 mime == null ? "" : mime, String.valueOf(size)));
    }

    private static List<String> readDmLines(String a, String b, int limit) {
        Path f = dmFile(a, b);
        if (!Files.exists(f)) return Collections.emptyList();
        try {
            List<String> all = Files.readAllLines(f, StandardCharsets.UTF_8);
            return limit > 0 && all.size() > limit ? all.subList(all.size() - limit, all.size()) : all;
        } catch (IOException e) {
            LOG.log(Level.WARNING, "readDmLines failed", e);
            return Collections.emptyList();
        }
    }

    private static void indexPartner(String a, String b) {
        DM_INDEX.computeIfAbsent(a.toLowerCase(), k -> ConcurrentHashMap.newKeySet()).add(b);
        DM_INDEX.computeIfAbsent(b.toLowerCase(), k -> ConcurrentHashMap.newKeySet()).add(a);
    }

    private static Path channelFile(String channel) {
        String safe = channel == null ? "_" : channel.replaceAll("[^a-zA-Z0-9_-]", "_");
        return CHANNEL_DIR.resolve(safe + ".log");
    }

    private static synchronized void appendChannelLine(String channel, String ts, String from,
                                                        String type, String payload) {
        try {
            Path f = channelFile(channel);
            Files.createDirectories(f.getParent());
            Files.writeString(f, String.join("\t", ts, from, type, sanitize(payload)) + "\n",
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            LOG.log(Level.WARNING, "appendChannelLine failed", e);
        }
    }

    private static synchronized void appendChannelAttach(String channel, String ts, String from,
                                                          String kind, String url, String name,
                                                          String mime, long size) {
        appendChannelLine(channel, ts, from, "attach",
                String.join(";;;", kind,
                            url  == null ? "" : url,
                            name == null ? "" : name,
                            mime == null ? "" : mime,
                            Long.toString(Math.max(0, size))));
    }

    private static List<String> readChannelLines(String channel, int limit) {
        Path f = channelFile(channel);
        if (!Files.exists(f)) return Collections.emptyList();
        try {
            List<String> all = Files.readAllLines(f, StandardCharsets.UTF_8);
            return limit > 0 && all.size() > limit ? all.subList(all.size() - limit, all.size()) : all;
        } catch (IOException e) {
            LOG.log(Level.WARNING, "readChannelLines failed", e);
            return Collections.emptyList();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Roles / access
    // --------------------------------------------------------------------------------------------

    private static boolean atLeast(String role, String min) {
        return ROLE_ORDER.indexOf(roleOf(role)) >= ROLE_ORDER.indexOf(min);
    }

    private static String roleOf(String r)             { return r == null ? "user" : r; }
    private static String roleOfUser(String username)  {
        UserRec r = USERS.get(username.toLowerCase());
        return r == null ? "user" : roleOf(r.role);
    }

    private static boolean hasBetaAccess(String username, String role) {
        if (atLeast(role, "dev")) return true;
        String key = username == null ? "" : username.toLowerCase();
        if ("beta tester".equalsIgnoreCase(PROF_TITLE.getOrDefault(key, ""))) return true;
        for (String t : INV_TITLES.getOrDefault(key, Collections.emptySet()))
            if ("beta tester".equalsIgnoreCase(t)) return true;
        return false;
    }

    private static boolean isMuted(String username) {
        return MUTED_UNTIL.getOrDefault(username.toLowerCase(), 0L) > System.currentTimeMillis();
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private static void broadcast(String json, Client except) {
        for (Client x : ONLINE.values()) {
            if (x != except) x.sendText(json);
        }
    }

    private static void err(Client c, String code, String message) {
        c.sendText(json(Map.of("type", "error", "code", code,
                               "message", message, "timestamp", nowTime())));
    }

    private static void sysTo(Client c, String text) {
        c.sendText(json(Map.of("type", "message", "from", "System",
                               "text", text, "timestamp", nowTime())));
    }

    private static void sysAll(String text) {
        broadcast(json(Map.of("type", "message", "from", "System",
                              "text", text, "timestamp", nowTime())), null);
    }

    private static String genSalt() {
        byte[] b = new byte[16];
        RNG.nextBytes(b);
        return base64(b);
    }

    // SHA-256 + salt; not BCrypt but avoids adding a dependency for now
    private static String hashPw(String pw, String saltB64) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(Base64.getDecoder().decode(saltB64));
            md.update(pw.getBytes(StandardCharsets.UTF_8));
            return hex(md.digest());
        } catch (Exception e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    private static String nowTime() {
        return DateTimeFormatter.ofPattern("HH:mm dd.MM.yyyy").format(LocalDateTime.now());
    }

    private static String genId() { return UUID.randomUUID().toString().replace("-", ""); }

    private static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) sb.append(String.format("%02x", x));
        return sb.toString();
    }

    private static byte[] sha1(byte[] in) throws Exception {
        return MessageDigest.getInstance("SHA-1").digest(in);
    }

    private static String base64(byte[] b) { return Base64.getEncoder().encodeToString(b); }

    private static String guessMime(String name) {
        String n = name.toLowerCase(Locale.ROOT);
        if (n.endsWith(".png"))               return "image/png";
        if (n.endsWith(".jpg") || n.endsWith(".jpeg")) return "image/jpeg";
        if (n.endsWith(".gif"))               return "image/gif";
        if (n.endsWith(".webp"))              return "image/webp";
        if (n.endsWith(".mp4"))               return "video/mp4";
        if (n.endsWith(".mp3"))               return "audio/mpeg";
        if (n.endsWith(".pdf"))               return "application/pdf";
        return "application/octet-stream";
    }

    private static String safeUser(String u) {
        if (u == null) return null;
        u = u.trim();
        return u.isEmpty() || !u.matches("[A-Za-z0-9_.\\-]{1,32}") ? null : u;
    }

    private static String safeFilename(String n) {
        if (n == null) return null;
        n = n.replaceAll("[\\\\/]+", "_").replaceAll("\\s+", "_");
        return n.isBlank() ? "file_" + Instant.now().toEpochMilli() : n;
    }

    private static String safeMime(String m) {
        if (m == null || m.isBlank()) return "application/octet-stream";
        m = m.trim();
        return m.length() > 100 ? "application/octet-stream" : m;
    }

    private static String sanitize(String s) {
        if (s == null) return "";
        return s.replace("\n", " ").replace("\r", " ").replace("\t", " ");
    }

    private static Path safeJoin(Path base, String rel) {
        try {
            Path p = base.resolve(rel).normalize();
            return p.startsWith(base) ? p : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static Path uniqueUploadPath(String name) throws IOException {
        int    dot  = name.lastIndexOf('.');
        String base = dot > 0 ? name.substring(0, dot) : name;
        String ext  = dot > 0 && dot < name.length() - 1 ? name.substring(dot) : "";
        for (int k = 0; k < 10_000; k++) {
            Path p = UPLOAD_DIR.resolve(base + (k == 0 ? "" : "_" + k) + ext);
            if (!Files.exists(p)) return p;
        }
        return Files.createTempFile(UPLOAD_DIR, "up_", ext);
    }

    // --------------------------------------------------------------------------------------------
    // Minimal flat-object JSON (no nested objects / arrays in values)
    // --------------------------------------------------------------------------------------------

    private static final Pattern P_PAIR =
            Pattern.compile("\"([^\"]+)\"\\s*:\\s*(\"([^\"]*)\"|[^,}\\s]+)");

    private static Map<String, String> parseJson(String s) {
        Map<String, String> m = new HashMap<>();
        if (s == null) return m;
        Matcher mm = P_PAIR.matcher(s);
        while (mm.find()) {
            String v = mm.group(3);
            if (v == null) v = mm.group(2);
            if (v != null) m.put(mm.group(1), unescapeJson(v));
        }
        return m;
    }

    private static String unescapeJson(String v) {
        return v.replace("\\n", "\n").replace("\\t", "\t")
                .replace("\\\"", "\"").replace("\\\\", "\\");
    }

    private static String json(Map<String, String> m) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> e : m.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(esc(e.getKey())).append("\":");
            String val = e.getValue();
            if (val == null) {
                sb.append("null");
            } else if (val.matches("^-?\\d+(\\.\\d+)?$")) {
                sb.append(val);
            } else {
                sb.append('"').append(esc(val)).append('"');
            }
        }
        return sb.append('}').toString();
    }

    private static String jsonInventoryPayload(List<String> titles, List<String> colors) {
        return "{\"type\":\"inventory\",\"items\":{\"titles\":" + toJsonArray(titles)
             + ",\"colors\":" + toJsonArray(colors)
             + "},\"timestamp\":\"" + esc(nowTime()) + "\"}";
    }

    private static String toJsonArray(List<String> list) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append(esc(list.get(i))).append('"');
        }
        return sb.append(']').toString();
    }

    private static String esc(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\t", "\\t").replace("\r", "");
    }

    // --------------------------------------------------------------------------------------------
    // WebSocket framing (RFC 6455)
    // --------------------------------------------------------------------------------------------

    private static final class WsFrame {
        byte   opcode;
        byte[] payload;
    }

    private static WsFrame readWsFrame(InputStream in) throws IOException {
        int b1 = in.read(), b2 = in.read();
        if (b1 == -1 || b2 == -1) return null;

        int  opcode = b1 & 0x0F;
        boolean masked = (b2 & 0x80) != 0;
        long len    = b2 & 0x7F;

        if (len == 126) {
            len = ByteBuffer.wrap(in.readNBytes(2)).getShort() & 0xFFFFL;
        } else if (len == 127) {
            len = ByteBuffer.wrap(in.readNBytes(8)).getLong();
        }

        byte[] mask    = masked ? in.readNBytes(4) : null;
        byte[] payload = in.readNBytes((int) len);
        if (masked) for (int i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];

        WsFrame f = new WsFrame();
        f.opcode  = (byte) opcode;
        f.payload = payload;
        return f;
    }

    private static void writeWsFrame(OutputStream out, byte opcode, byte[] payload) throws IOException {
        out.write(0x80 | (opcode & 0x0F));
        int len = payload.length;
        if (len <= 125) {
            out.write(len);
        } else if (len <= 65535) {
            out.write(126);
            out.write((len >>> 8) & 0xFF);
            out.write(len & 0xFF);
        } else {
            out.write(127);
            out.write(ByteBuffer.allocate(8).putLong(len).array());
        }
        out.write(payload);
        out.flush();
    }

    private static void writeHttp(OutputStream out, String status, String contentType,
                                   byte[] body) throws IOException {
        String hdr = "HTTP/1.1 " + status + "\r\n"
                   + "Content-Type: " + contentType + "\r\n"
                   + "Content-Length: " + body.length + "\r\n"
                   + "Access-Control-Allow-Origin: *\r\n"
                   + "Connection: close\r\n\r\n";
        out.write(hdr.getBytes(StandardCharsets.US_ASCII));
        out.write(body);
        out.flush();
    }
}
