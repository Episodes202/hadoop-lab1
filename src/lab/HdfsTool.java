package lab;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Scanner;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/** Ten basic HDFS operations. Paths refer to the configured default filesystem. */
public final class HdfsTool implements AutoCloseable {
    private final Configuration conf;
    private final FileSystem fs;
    public HdfsTool(Configuration conf) throws IOException {
        this.conf = conf;
        this.fs = FileSystem.get(conf);
    }
    private void parents(Path path) throws IOException {
        if (path.getParent() != null && !fs.mkdirs(path.getParent()))
            throw new IOException("Cannot create parents: " + path);
    }
    private FileStatus file(Path path) throws IOException {
        FileStatus s = fs.getFileStatus(path);
        if (!s.isFile()) throw new IOException("Not a regular file: " + path);
        return s;
    }
    public void upload(java.nio.file.Path local, Path dst, String mode) throws IOException {
        if (!mode.equals("append") && !mode.equals("overwrite"))
            throw new IllegalArgumentException("Mode must be append or overwrite");
        if (!Files.isRegularFile(local)) throw new IOException("Not a local file: " + local);
        parents(dst);
        boolean exists = fs.exists(dst);
        if (exists) file(dst);
        try (InputStream in = Files.newInputStream(local);
             OutputStream out = exists && mode.equals("append") ? fs.append(dst) : fs.create(dst, true)) {
            in.transferTo(out);
        }
    }
    public java.nio.file.Path download(Path src, java.nio.file.Path dir) throws IOException {
        file(src); Files.createDirectories(dir);
        String name = src.getName();
        int dot = name.lastIndexOf('.');
        String stem = dot > 0 ? name.substring(0, dot) : name;
        String ext = dot > 0 ? name.substring(dot) : "";
        for (int n = 0; ; n++) {
            java.nio.file.Path dst = dir.resolve(n == 0 ? name : stem + "_" + n + ext);
            OutputStream local;
            try { local = Files.newOutputStream(dst, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE); }
            catch (java.nio.file.FileAlreadyExistsException e) { continue; }
            try (OutputStream out = local; InputStream in = fs.open(src)) { in.transferTo(out); }
            catch (IOException e) { Files.deleteIfExists(dst); throw e; }
            return dst;
        }
    }
    public void cat(Path src, OutputStream out) throws IOException {
        file(src); try (InputStream in = fs.open(src)) { in.transferTo(out); }
    }
    public void stat(Path src, PrintStream out) throws IOException {
        print(fs.getFileStatus(src), out);
    }
    private void print(FileStatus s, PrintStream out) {
        out.printf("%s\t%s:%s\t%d\tmtime=%s\tcreation=UNAVAILABLE\t%s%n",
            s.getPermission(), s.getOwner(), s.getGroup(), s.getLen(),
            Instant.ofEpochMilli(s.getModificationTime()), s.getPath());
    }
    public void list(Path dir, PrintStream out) throws IOException {
        if (!fs.getFileStatus(dir).isDirectory()) throw new IOException("Not a directory: " + dir);
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(dir, true);
        while (it.hasNext()) print(it.next(), out);
    }
    public void create(Path dst) throws IOException {
        parents(dst); try (OutputStream out = fs.create(dst, false)) { /* create without overwrite */ }
    }
    public void mkdir(Path dir) throws IOException {
        if (!fs.mkdirs(dir)) throw new IOException("Cannot mkdir: " + dir);
    }
    public void rmdir(Path dir) throws IOException {
        if (!fs.getFileStatus(dir).isDirectory()) throw new IOException("Not a directory: " + dir);
        if (!fs.delete(dir, false)) throw new IOException("Cannot remove directory: " + dir);
    }
    public void append(Path dst, java.nio.file.Path content, String position) throws IOException {
        FileStatus s = file(dst);
        if (position.equals("tail")) {
            try (InputStream in = Files.newInputStream(content); OutputStream out = fs.append(dst)) {
                in.transferTo(out);
            }
        } else if (position.equals("head")) {
            Path temp = new Path(dst.getParent(), ".prepend-" + UUID.randomUUID());
            try {
                try (OutputStream out = fs.create(temp, s.getPermission(), false, 65536,
                        s.getReplication(), s.getBlockSize(), null);
                     InputStream prefix = Files.newInputStream(content); InputStream original = fs.open(dst)) {
                    prefix.transferTo(out); original.transferTo(out);
                }
                fs.setPermission(temp, s.getPermission());
                FileContext.getFileContext(fs.getUri(), conf).rename(temp, dst, Options.Rename.OVERWRITE);
            } finally { if (fs.exists(temp)) fs.delete(temp, false); }
        } else throw new IllegalArgumentException("Position must be head or tail");
    }
    public void remove(Path path) throws IOException {
        file(path); if (!fs.delete(path, false)) throw new IOException("Cannot delete: " + path);
    }
    public void move(Path src, Path dst) throws IOException {
        file(src);
        if (fs.exists(dst)) throw new IOException("Destination exists: " + dst);
        parents(dst);
        if (!fs.rename(src, dst)) throw new IOException("Cannot rename: " + src);
    }
    public void close() throws IOException { fs.close(); }
    public static void main(String[] a) throws Exception {
        if (a.length < 2) throw new IllegalArgumentException(
            "upload LOCAL HDFS [append|overwrite]; download HDFS LOCAL_DIR; cat/stat/list/touch/rm/mkdir/rmdir PATH; append HDFS LOCAL head|tail; mv SRC DST");
        try (HdfsTool h = new HdfsTool(new Configuration())) {
            Path p = new Path(a[1]);
            switch (a[0]) {
            case "upload":
                String mode;
                if (a.length > 3) mode = a[3];
                else {
                    System.err.print("Choose append or overwrite: ");
                    mode = new Scanner(System.in).nextLine().trim();
                }
                h.upload(java.nio.file.Path.of(a[1]), new Path(a[2]), mode); break;
            case "download": System.out.println(h.download(p, java.nio.file.Path.of(a[2]))); break;
            case "cat": h.cat(p, System.out); break;
            case "stat": h.stat(p, System.out); break;
            case "list": h.list(p, System.out); break;
            case "touch": h.create(p); break;
            case "rm": h.remove(p); break;
            case "mkdir": h.mkdir(p); break;
            case "rmdir": h.rmdir(p); break;
            case "append": h.append(p, java.nio.file.Path.of(a[2]), a[3]); break;
            case "mv": h.move(p, new Path(a[2])); break;
            default: throw new IllegalArgumentException("Unknown operation: " + a[0]);
            }
        }
    }
}
