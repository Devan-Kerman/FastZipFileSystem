package net.devtech.fastzipfilesystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.function.Consumer;

class FastZipPath implements Path {
	final boolean isAbsolute;
	final ByteBuffer name;
	final int trailingLen;
	final FastZipFS fs;
	String cachedToString;
	private FastZipEntry entry, dirEntry;
	
	FastZipPath(boolean absolute, ByteBuffer name, FastZipFS fs) {
		this.isAbsolute = absolute;
		this.name = name;
		this.fs = fs;
		this.trailingLen = hasTrailing(name) ? name.limit()-1 : name.limit();
	}
	
	FastZipPath(FastZipFS fs, String string) {
		this.fs = fs;
		boolean abs = !string.isEmpty() && string.charAt(0) == '/';
		String substring = abs ? string.substring(1) : string;
		ByteBuffer name = this.name = StandardCharsets.UTF_8.encode(substring);
		this.cachedToString = substring;
		this.isAbsolute = abs;
		this.trailingLen = hasTrailing(name) ? name.limit()-1 : name.limit();
	}
	
	public void remove(FastZipEntry remove) {
		if(this.entry == remove) {
			this.entry.valid = false;
			this.entry = null;
		}
		
		if(this.dirEntry == remove) {
			this.dirEntry.valid = false;
			this.dirEntry = null;
		}
	}
	
	static boolean hasTrailing(ByteBuffer buffer) {
		int limit = buffer.limit();
		return limit > 1 && buffer.get(limit-1) == '/';
	}
	
	FastZipEntry getOrCreateEntry(boolean dir) throws FileNotFoundException {
		FastZipEntry entry;
		if(dir) {
			entry = this.dirEntry;
			if(entry == null || !entry.valid) {
				entry = this.dirEntry = this.fs.getOrCreatePath(this.name, true);
			}
		} else {
			entry = this.entry;
			if(entry == null || !entry.valid) {
				entry = this.entry = this.fs.getOrCreatePath(this.name, false);
			}
		}
		return entry;
	}
	
	// todo getOrCreateDirectoryEntry
	
	void mutEntries(Consumer<FastZipEntry> consumer) {
		FastZipEntry en = getEntry(false);
		if(en != null) {
			consumer.accept(en);
		}
		en = getEntry(true);
		if(en != null) {
			consumer.accept(en);
		}
	}
	
	FastZipEntry getEntry(boolean dir) {
		FastZipEntry entry;
		if(dir) {
			entry = this.dirEntry;
			if(entry == null || !entry.valid) {
				entry = this.dirEntry = this.fs.paths.get(FastZipFS.absoluteName(name, true));
			}
		} else {
			entry = this.entry;
			if(entry == null || !entry.valid) {
				entry = this.entry = this.fs.paths.get(this.name);
			}
		}
		return entry;
	}
	
	FastZipEntry getAnyEntry() {
		FastZipEntry entry1 = getEntry(false);
		return entry1 == null ? getEntry(true) : entry1;
	}
	
	@Override
	public FileSystem getFileSystem() {
		return this.fs;
	}
	
	@Override
	public boolean isAbsolute() {
		return this.isAbsolute;
	}
	
	@Override
	public FastZipPath getRoot() {
		if(this.isAbsolute) {
			return this.fs.root;
		} else {
			return null;
		}
	}
	
	@Override
	public Path getFileName() {
		int index = this.lastIndexOfSeperator() + 1;
		if(index == 0) {
			return this;
		}
		return new FastZipPath(false, this.name.slice(index, name.limit() - index), this.fs);
	}
	
	@Override
	public FastZipPath getParent() {
		int index = this.lastIndexOfSeperator();
		if(index == -1 && this.trailingLen != 0) {
			return this.getRoot();
		}
		if(this == this.getRoot()) {
			return null;
		}
		return new FastZipPath(false, this.name.slice(0, index), this.fs);
	}
	
	@Override
	public int getNameCount() {
		ByteBuffer name = this.name;
		int count = 0;
		for(int i = 0; i < name.limit() - 1; i++) {
			if(name.get(i) == '/') {
				count++;
			}
		}
		return count + 1;
	}
	
	int lastIndexOfSeperator() {
		ByteBuffer name = this.name;
		for(int i = name.limit() - 2; i >= 0; i--) { // -1 to avoid directory ending
			if(name.get(i) == '/') {
				return i;
			}
		}
		return -1;
	}
	
	static int nthIndexOfSeperator(ByteBuffer name, int from, int n) {
		if(n == -1) {
			return -1;
		}
		
		int i = from;
		for(; i < name.limit() - 1; i++) {
			if(name.get(i) == '/' && n-- <= 0) {
				return i;
			}
		}
		
		if(n == 0) {
			return i;
		}
		return -1;
	}
	
	@Override
	public Path getName(int index) {
		int start = nthIndexOfSeperator(this.name, 0, index - 1) + 1;
		int end = nthIndexOfSeperator(this.name, start, 0);
		if(end == -1) {
			throw new IllegalArgumentException();
		}
		return new FastZipPath(false, this.name.slice(start, end - start), this.fs);
	}
	
	@Override
	public Path subpath(int beginIndex, int endIndex) {
		int start = nthIndexOfSeperator(this.name, 0, beginIndex - 1) + 1;
		int end = nthIndexOfSeperator(this.name, start, endIndex - beginIndex - 1) + 1;
		if(end == 0) {
			throw new IllegalArgumentException();
		}
		return new FastZipPath(false, this.name.slice(start, end - start), this.fs);
	}
	
	@Override
	public boolean startsWith(Path path) {
		FastZipPath other = (FastZipPath) path;
		ByteBuffer otherName = other.name, name = this.name;
		int limit = otherName.limit();
		if(other.trailingLen > this.trailingLen) {
			return false;
		}
		for(int i = 0; i < limit; i++) {
			byte o = otherName.get(i), c = name.get(i);
			if(o != c && !(i == limit - 1 && o == '/')) {
				return false;
			}
		}
		if(name.limit() > limit) {
			return name.get(limit) == '/';
		} else {
			return true;
		}
	}
	
	@Override
	public boolean endsWith(Path path) {
		FastZipPath other = (FastZipPath) path;
		ByteBuffer otherName = other.name, name = this.name;
		int otherLim = other.trailingLen;
		int index = this.trailingLen - otherLim;
		return name.slice(index, otherLim).equals(otherName.slice(0, otherLim)) && (index == 0 || name.get(index - 1) == '/');
	}
	
	@Override
	public Path normalize() {
		return this;
	}
	
	@Override
	public Path resolve(Path other) {
		FastZipPath path = (FastZipPath) other;
		int limit = this.name.limit();
		ByteBuffer combined = ByteBuffer.allocate(limit + 1 + path.name.limit());
		int off = 0;
		if(limit > 0) {
			combined.put(0, this.name, 0, limit);
			if(this.name.get(limit - 1) != '/') {
				combined.put((byte) '/');
				off += 1;
			}
			off += limit;
		}
		
		int limit1 = path.name.limit();
		combined.put(off, path.name, 0, limit1);
		off += limit1;
		combined.limit(off);
		
		return new FastZipPath(this.isAbsolute, combined, this.fs);
	}
	
	static final ByteBuffer EMPTY = ByteBuffer.allocate(0);
	
	@Override
	public Path relativize(Path other) {
		FastZipPath path = (FastZipPath) other;
		int thisLen = this.name.limit();
		int otherLen = path.name.limit();
		if(otherLen < thisLen) {
			throw new UnsupportedOperationException("/../../ paths not supported!");
		}
		
		if(path.name.slice(0, thisLen).equals(this.name)) {
			if(otherLen == thisLen) {
				return new FastZipPath(false, EMPTY, this.fs);
			}
			ByteBuffer buffer = ByteBuffer.allocate(otherLen - thisLen - 1);
			buffer.put(0, ((FastZipPath) other).name, thisLen + 1, otherLen - thisLen - 1);
			return new FastZipPath(false, buffer, this.fs);
		} else {
			throw new UnsupportedOperationException("/../../ paths not supported!");
		}
	}
	
	@Override
	public URI toUri() {
		try {
			return new URI("jar", FastZipUtil.decodeUri(this.fs.zipfsPath.toUri().toString()) + "!" + this.toAbsolutePath(), null);
		} catch(Exception ex) {
			throw new AssertionError(ex);
		}
	}
	
	// todo implement
	@Override
	public Path toRealPath(LinkOption... options) throws IOException {
		if(this.isAbsolute && this.trailingLen == this.name.limit()) {
			return this;
		}
		
		return new FastZipPath(true, this.name.slice(0, this.trailingLen), this.fs);
	}
	
	@Override
	public Path toAbsolutePath() {
		if(this.isAbsolute) {
			return this;
		}
		return new FastZipPath(true, this.name, this.fs);
	}
	
	@Override
	public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public int compareTo(Path other) {
		return this.name.compareTo(((FastZipPath) other).name);
	}
	
	@Override
	public String toString() {
		String string = this.cachedToString;
		if(string == null) {
			string = FastZipUtil.toStr(this.name).toString();
			if(this.isAbsolute) {
				string = "/" + string;
			}
			this.cachedToString = string;
		}
		return string;
	}
	
	@Override
	public boolean equals(Object o) {
		if(this == o) {
			return true;
		}
		if(!(o instanceof FastZipPath paths)) {
			return false;
		}
		
		if(isAbsolute != paths.isAbsolute) {
			return false;
		}
		if(!name.equals(paths.name)) {
			return false;
		}
		return fs.equals(paths.fs);
	}
	
	@Override
	public int hashCode() {
		int result = (isAbsolute ? 1 : 0);
		result = 31 * result + name.hashCode();
		result = 31 * result + fs.hashCode();
		return result;
	}
	
	public boolean isDirectory() {
		return (this.name.limit() == 0 || this.name.get(this.name.limit() - 1) == '/') || this.getEntry(true) != null;
	}
}
