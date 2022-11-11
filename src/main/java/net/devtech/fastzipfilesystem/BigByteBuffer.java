package net.devtech.fastzipfilesystem;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import java.util.zip.ZipException;

class BigByteBuffer {
	static final ConcurrentLinkedDeque<Inflater> INFLATERS = new ConcurrentLinkedDeque<>();
	static final int MAX_SIZE = Integer.MAX_VALUE;
	ByteBuffer[] buffers;
	long size;
	
	BigByteBuffer(BigByteBuffer buffer, long offset, long len) {
		long size = len;
		ByteBuffer[] contents = new ByteBuffer[FastZipUtil.ceilDiv(size, MAX_SIZE)];
		for(int i = 0; i < contents.length; i++) {
			long min = Math.min(size, MAX_SIZE);
			contents[i] = ByteBuffer.allocate((int) min);
			size -= min;
		}
		long off = offset;
		for(ByteBuffer content : contents) {
			int capacity = content.capacity();
			buffer.read(off, content, 0, capacity);
			off += capacity;
		}
		
		this.buffers = contents;
		this.size = len;
	}
	
	BigByteBuffer(ByteBuffer buffer) {
		this.buffers = new ByteBuffer[] {buffer};
		this.size = buffer.limit();
	}
	
	BigByteBuffer(long uncompressedSize, BigByteBuffer compressed, long offset, long size) throws IOException {
		Inflater pop = INFLATERS.pollFirst();
		if(pop == null) {
			pop = new Inflater(true);
		}
		
		try {
			ByteBuffer[] outputs = new ByteBuffer[FastZipUtil.ceilDiv(uncompressedSize, BigByteBuffer.MAX_SIZE)];
			long outputSize = uncompressedSize;
			for(int i = 0; i < outputs.length; i++) {
				outputs[i] = ByteBuffer.allocate((int) Math.min(BigByteBuffer.MAX_SIZE, outputSize));
				outputSize -= BigByteBuffer.MAX_SIZE;
			}
			
			int[] idx = {0};
			Inflater inflater = pop;
			compressed.segmentedInsert(buffer -> {
				try {
					inflater.setInput(buffer);
					while(!inflater.needsInput() && !inflater.finished()) {
						ByteBuffer output = outputs[idx[0]];
						inflater.inflate(output);
						if(!output.hasRemaining()) {
							idx[0]++;
						}
					}
				} catch(DataFormatException e) {
					throw new RuntimeException(e);
				}
			}, offset, size);
			
			for(ByteBuffer output : outputs) {
				output.position(0);
			}
			if(!inflater.finished()) {
				throw new ZipException("Reported Inflated size > Actual Inflated Size");
			}
			this.buffers = outputs;
			this.size = uncompressedSize;
		} finally {
			pop.reset();
			INFLATERS.push(pop);
		}
	}
	
	public BigByteBuffer() {
		this.buffers = new ByteBuffer[] {FastZipPath.EMPTY};
		this.size = 0;
	}
	
	record PathBuffer(Closeable closeable, BigByteBuffer buffer) {}
	
	public static PathBuffer buffer(Path path) throws IOException {
		FileSystem system = path.getFileSystem();
		if(system == FileSystems.getDefault() && Files.exists(path)) {
			FileChannel channel = null;
			boolean success = false;
			try {
				channel = system.provider().newFileChannel(path, Set.of());
				ByteBuffer[] contents = new ByteBuffer[FastZipUtil.ceilDiv(channel.size(), MAX_SIZE)];
				long offset = 0;
				for(int i = 0; i < contents.length; i++) {
					long min = Math.min(channel.size() - offset, MAX_SIZE);
					contents[i] = channel.map(FileChannel.MapMode.READ_ONLY, offset, min);
					offset += min;
				}
				BigByteBuffer buffer = new BigByteBuffer(contents, channel.size());
				success = true;
				return new PathBuffer(channel, buffer);
			} catch(IOException ignored) {
			} finally {
				if(!success && channel != null) {
					channel.close();
				}
			}
		}
		
		return new PathBuffer(null, new BigByteBuffer(path));
	}
	
	private BigByteBuffer(ByteBuffer[] contents, long len) {
		this.buffers = contents;
		this.size = len;
	}
	
	BigByteBuffer(Path path) throws IOException {
		this(Files.newByteChannel(path));
	}
	
	BigByteBuffer(SeekableByteChannel channel) throws IOException {
		try(channel) {
			long size = channel.size();
			ByteBuffer[] contents = new ByteBuffer[FastZipUtil.ceilDiv(size, MAX_SIZE)];
			for(int i = 0; i < contents.length; i++) {
				long min = Math.min(size, MAX_SIZE);
				contents[i] = ByteBuffer.allocate((int) min);
				size -= min;
			}
			
			long read = 0;
			if(channel instanceof ScatteringByteChannel s) {
				while(read < channel.size()) {
					read += s.read(contents);
				}
			} else {
				for(ByteBuffer content : contents) {
					read += channel.read(content);
				}
			}
			this.buffers = contents;
			this.size = channel.size();
		}
	}
	
	public void append(ByteBuffer buffer, long offset) {
		int remaining = buffer.remaining();
		if(remaining + offset > size) {
			int length = this.buffers.length;
			ByteBuffer end = this.buffers[length - 1]; // attempt to resize end buffer
			int toExpand = MAX_SIZE - end.limit();
			if(toExpand > 0) {
				long min = Math.min(MAX_SIZE, (remaining + offset) - this.size);
				ByteBuffer copy = ByteBuffer.allocate((int) min);
				copy.put(end);
				copy.clear();
				this.buffers[length-1] = copy;
				this.size += min;
			}
			while(remaining + offset > this.size) {
				int init = this.buffers.length;
				ByteBuffer[] buf = this.buffers = Arrays.copyOf(this.buffers, init + 1);
				long min = Math.min(MAX_SIZE, (remaining + offset) - this.size);
				buf[init] = ByteBuffer.allocate((int) min);
				this.size += min;
			}
		}
		this.segmentedInsert(buffer2 -> {
			buffer2.put(0, buffer, buffer.position(), buffer2.limit());
			buffer.position(buffer.position() + buffer2.limit());
		}, offset, remaining);
		this.size = Math.max(remaining + offset, this.size);
	}
	
	static ByteBuffer copyBuffer(ByteBuffer buffer, int newSize) {
		ByteBuffer copy = ByteBuffer.allocate(newSize);
		copy.put(buffer);
		return copy;
	}
	
	ByteBuffer slice(long off, int len) {
		int index = (int) (off / MAX_SIZE);
		Objects.checkIndex(index, this.buffers.length);
		int offset = (int) (off % MAX_SIZE);
		int next = nextOverread(off, len);
		ByteBuffer buffer = this.buffers[index];
		Objects.checkFromIndexSize(offset, next, buffer.limit());
		if(next == len) {
			return buffer.slice(offset, len).order(ByteOrder.LITTLE_ENDIAN);
		} else {
			ByteBuffer copy = ByteBuffer.allocate(len).order(ByteOrder.LITTLE_ENDIAN);
			this.segmentedInsert(copy::put, off, len);
			copy.position(0);
			return copy;
		}
	}
	
	long segmentedInsert(Consumer<ByteBuffer> buf, long off, long llen) {
		return this.segmentedInsert0(buffer -> {
			buf.accept(buffer);
			return buffer.limit();
		}, off, llen);
	}
	
	long segmentedInsert0(ToIntFunction<ByteBuffer> buf, long off, long llen) {
		long read = 0;
		while(read < llen) {
			long position = read + off;
			int index = (int) (position / MAX_SIZE);
			if(index >= this.buffers.length) {
				break;
			}
			ByteBuffer buffer = this.buffers[index];
			int offset = (int) (position % MAX_SIZE);
			int toRead = Math.min(nextOverread(position, llen - read), buffer.limit() - offset);
			int test = buf.applyAsInt(buffer.slice(offset, toRead).order(ByteOrder.LITTLE_ENDIAN));
			read += test;
			if(test != toRead) {
				break;
			}
			if(read == this.size) {
				break;
			}
		}
		return read;
	}
	
	static int nextOverread(long position, long len) {
		int remaining = (int) (MAX_SIZE - (position % MAX_SIZE));
		return (int) Math.min(remaining, len);
	}
	
	static void main(String[] args) {
		long i = nextOverread(BigByteBuffer.MAX_SIZE, BigByteBuffer.MAX_SIZE);
		System.out.println(i);
	}
	
	int read(long position, ByteBuffer dst, int start, int len) {
		dst.clear();
		dst.position(start);
		long inserted = segmentedInsert(dst::put, position, len);
		dst.limit((int) (start + inserted));
		return (int) inserted;
	}
	
	long size() throws IOException {
		return this.size;
	}
}
