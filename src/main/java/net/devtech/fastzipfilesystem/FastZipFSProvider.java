package net.devtech.fastzipfilesystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.zip.ZipException;

class FastZipFSProvider extends FileSystemProvider {
	// todo allow flushing a specific entry
	// todo allow deleting the uncompressed cache of an entry
	// todo in amalg add a zip-io-like system to avoid iteration and
	
	final Map<Path, FastZipFS> filesystems = new ConcurrentHashMap<>();
	
	@Override
	public String getScheme() {
		return "jar";
	}
	
	Path uriToPath(URI uri) {
		String scheme = uri.getScheme();
		if((scheme == null) || !scheme.equalsIgnoreCase(getScheme())) {
			throw new IllegalArgumentException("URI scheme is not '" + getScheme() + "'");
		}
		try {
			// only support legacy JAR URL syntax  jar:{uri}!/{entry} for now
			String spec = uri.getRawSchemeSpecificPart();
			int sep = spec.indexOf("!/");
			if(sep != -1) {
				spec = spec.substring(0, sep);
			}
			return Paths.get(new URI(spec)).toAbsolutePath();
		} catch(URISyntaxException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}
	
	boolean ensureFile(Path path) {
		try {
			BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
			if(!attrs.isRegularFile()) {
				throw new UnsupportedOperationException();
			}
			return true;
		} catch(IOException ioe) {
			return false;
		}
	}
	
	@Override
	public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
		Path path = uriToPath(uri);
		if(ensureFile(path)) {
			path = path.toRealPath();
		}
		FastZipFS fs = filesystems.computeIfAbsent(path, abs -> {
			try {
				return new FastZipFS(this, abs, env);
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		});
		
		if(fs.zipfsPath != path) {
			throw new FileSystemAlreadyExistsException(path + "");
		}
		return fs;
	}
	
	@Override
	public FileSystem newFileSystem(Path path, Map<String, ?> env) throws IOException {
		ensureFile(path);
		return getZipFileSystem(path, env);
	}
	
	@Override
	public FileSystem getFileSystem(URI uri) {
		FastZipFS zipfs = null;
		try {
			zipfs = filesystems.get(uriToPath(uri).toRealPath());
		} catch(IOException x) {
			// ignore the ioe from toRealPath(), return FSNFE
		}
		if(zipfs == null) {
			throw new FileSystemNotFoundException(uri + "");
		}
		return zipfs;
	}
	
	FastZipFS getZipFileSystem(Path path, Map<String, ?> env) throws IOException {
		try {
			return new FastZipFS(this, path, env);
		} catch(ZipException ze) {
			String pname = path.toString();
			if(pname.endsWith(".zip") || pname.endsWith(".jar")) {
				throw ze;
			}
			throw new UnsupportedOperationException();
		}
	}
	
	@Override
	public Path getPath(URI uri) {
		String spec = uri.getSchemeSpecificPart();
		int sep = spec.indexOf("!/");
		if(sep == -1) {
			throw new IllegalArgumentException("URI: " + uri + " does not contain path info ex. jar:file:/c:/foo.zip!/BAR");
		}
		return getFileSystem(uri).getPath(spec.substring(sep + 1));
	}
	
	@Override
	public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
		FastZipPath path1 = (FastZipPath) path;
		FastZipEntry entry;
		if(options.contains(StandardOpenOption.CREATE_NEW)) {
			if(path1.getEntry(false) != null) {
				throw new FileAlreadyExistsException(path + "");
			} else {
				entry = path1.getOrCreateEntry(false);
			}
		} else if(options.contains(StandardOpenOption.CREATE)) {
			entry = path1.getOrCreateEntry(false);
		} else {
			entry = path1.getEntry(false);
			if(entry == null) {
				throw new FileNotFoundException(path + "");
			}
		}
		
		if(options.contains(StandardOpenOption.APPEND)) {
			path1.fs.dirty = true;
			return new WriteEntryByteChannel(entry, true);
		} else if(options.contains(StandardOpenOption.WRITE)) {
			path1.fs.dirty = true;
			return new WriteEntryByteChannel(entry, false);
		} else {
			return new ReadEntryByteChannel(entry);
		}
	}
	
	@Override
	public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
		FastZipPath parent = (FastZipPath) dir.getParent();
		if(parent.getEntry(true) == null) {
			throw new FileNotFoundException(parent + "");
		}
		FastZipPath fdir = (FastZipPath) dir;
		if(fdir.getEntry(true) != null) {
			throw new FileAlreadyExistsException(fdir + "");
		}
		fdir.getOrCreateEntry(true);
		fdir.fs.dirty = true;
	}
	
	@Override
	public void delete(Path path) throws IOException {
		FastZipPath del = (FastZipPath) path;
		FastZipEntry entry = del.getAnyEntry();
		if(entry == null) {
			throw new FileNotFoundException(path + "");
		} else if(!entry.children.isEmpty()) {
			throw new DirectoryNotEmptyException(path + "");
		} else if(!del.fs.removePath(del)) {
			throw new FileNotFoundException(path + "");
		}
		
		del.fs.dirty = true;
	}
	
	@Override
	public void copy(Path source, Path target, CopyOption... options) throws IOException {
		FastZipPath parent = (FastZipPath) target.getParent();
		Objects.requireNonNull(parent.getEntry(true), "Directory Parent Does Not Exist For " + target);
		FastZipPath from = (FastZipPath) source, to = (FastZipPath) target;
		FastZipEntry entry = from.getEntry(false);
		boolean maybeDir = entry == null;
		if(maybeDir) {
			entry = from.getEntry(true);
		}
		
		if(entry == null) {
			throw new FileNotFoundException(from + "");
		}
		
		if(!entry.children.isEmpty()) {
			throw new DirectoryNotEmptyException(entry + "");
		}
		
		// todo only copy attributes if specified
		FastZipEntry entry1 = to.getOrCreateEntry(maybeDir);
		FastZipPath mod = to;
		long time = System.currentTimeMillis();
		do {
			entry1.lastMod = time;
		} while((mod = mod.getParent()) != null);
		
		if(!maybeDir) {
			entry1.contents = entry.contents;
		}
		to.fs.dirty = true;
	}
	
	@Override
	public void move(Path source, Path target, CopyOption... options) throws IOException {
		this.copy(source, target, options);
		this.delete(source);
	}
	
	@Override
	public boolean isSameFile(Path path, Path path2) throws IOException {
		if(path.getFileSystem() != path2.getFileSystem()) {
			return false;
		}
		FastZipPath a = (FastZipPath) path, b = (FastZipPath) path2;
		return a.name.equals(b.name);
	}
	
	@Override
	public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
		FastZipPath paths = (FastZipPath) dir;
		FastZipEntry entry = paths.getEntry(true);
		if(entry == null) {
			throw new FileNotFoundException(dir + "");
		}
		return new DirectoryStream<>() {
			@Override
			public void close() {}
			
			@Override
			public Iterator<Path> iterator() {
				return new MappedIterator<>(entry.children.iterator(), p -> new FastZipPath(true, p, paths.fs));
			}
		};
	}
	
	@Override
	public boolean isHidden(Path path) throws IOException {
		return false;
	}
	
	@Override
	public FileStore getFileStore(Path path) throws IOException {
		return null; // todo
	}
	
	@Override
	public void checkAccess(Path path, AccessMode... modes) throws IOException {
		if(((FastZipPath) path).getAnyEntry() == null) {
			throw new FileNotFoundException();
		}
	}
	
	@Override
	public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
		if(BasicFileAttributeView.class == type) {
			return (V) new ZipEntryFileAttributeView((FastZipPath) path);
		}
		return null;
	}
	
	@Override
	public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
		if(type == BasicFileAttributes.class) {
			return (A) new ZipEntryFileAttributeView((FastZipPath) path).readAttributes();
		}
		return null;
	}
	
	@Override
	public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public AsynchronousFileChannel newAsynchronousFileChannel(
			Path path, Set<? extends OpenOption> options, ExecutorService executor, FileAttribute<?>... attrs) throws IOException {
		throw new UnsupportedOperationException();
	}
}
