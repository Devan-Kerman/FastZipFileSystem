package net.devtech.fastzipfilesystem;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class WriteEntryByteChannel extends ReadEntryByteChannel implements WritableByteChannel {
	final FastZipEntry entry;
	WriteEntryByteChannel(FastZipEntry entry, boolean append) throws IOException {
		super(entry);
		FastZipEntry.ZipContents contents = new FastZipEntry.ZipContents();
		if(append) {
			BigByteBuffer decompress = entry.contents.decompress();
			contents.uncompressedData = new BigByteBuffer(decompress, 0, decompress.size);
			this.pos = entry.contents.uncompressedSize;
		} else {
			contents.uncompressedData = new BigByteBuffer();
		}
		this.building = contents;
		this.entry = entry;
	}
	
	@Override
	protected void building(FastZipEntry entry) {
	}
	
	@Override
	public int write(ByteBuffer src) throws IOException {
		int toWrite = src.remaining();
		this.building.decompress().append(src, this.pos);
		this.pos += toWrite;
		this.size = Math.max(this.size, this.pos);
		return toWrite;
	}
	
	@Override
	public boolean isOpen() {
		return true;
	}
	
	@Override
	public void close() throws IOException {
		this.building.uncompressedOffset = 0;
		this.building.uncompressedSize = this.size;
		this.building.compress();
		this.entry.contents = this.building;
	}
}
